from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaDocument,
    MessageMediaPhoto,
    MessageMediaWebPage,
)

from src.config import AppConfig
from src.queue_client import RedisQueue, RedisState

logger = logging.getLogger(__name__)

# Redis key that stores the highest source-channel message_id we have pushed.
LAST_PROCESSED_KEY = "sorter:last_processed_id"
# Never look further back than this on startup catch-up.
CATCHUP_WINDOW = timedelta(days=1)


def _media_type(media: object) -> str | None:
    """Return a human-readable media type label."""
    if media is None:
        return None
    if isinstance(media, MessageMediaPhoto):
        return "photo"
    if isinstance(media, MessageMediaDocument):
        return "document"
    if isinstance(media, MessageMediaWebPage):
        return "webpage"
    return type(media).__name__


class ListenerService:
    """
    Connects to Telegram as a **user account** via Telethon.

    Start-up sequence
    -----------------
    1. Determine the last processed message ID (Redis → category channels → none).
    2. Fetch any messages from the source channel posted in the last 24 h that
       are newer than the last processed ID, and push them in chronological order.
    3. Register a real-time handler and block until disconnected.

    Deduplication
    -------------
    ``_pushed_ids`` is an in-memory set of message IDs pushed this session.
    It prevents a message that arrives during the catch-up window from being
    enqueued a second time by the real-time handler.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        session = StringSession(config.telegram.session_string or None)
        self.client = TelegramClient(
            session,
            config.telegram.api_id,
            config.telegram.api_hash,
        )
        self.queue = RedisQueue(
            url=config.redis.url,
            stream=config.redis.messages_stream,
            group=config.redis.consumer_group,
        )
        self.state = RedisState(url=config.redis.url)
        # In-memory dedup set — prevents double-push during catch-up / live overlap
        self._pushed_ids: set[int] = set()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        await self.queue.connect()
        await self.state.connect()

        logger.info("Connecting to Telegram (user account)…")
        await self.client.start()

        me = await self.client.get_me()
        logger.info("Logged in as: %s (id=%s)", me.username or me.first_name, me.id)

        # Resolve once — gives us the numeric chat_id we'll embed in every payload
        source_entity = await self.client.get_entity(self.config.telegram.source_channel)
        logger.info(
            "Source channel resolved: @%s  (id=%d)",
            self.config.telegram.source_channel,
            source_entity.id,
        )

        # Register the real-time handler BEFORE catch-up so no live message is
        # missed during the (potentially slow) historical fetch.
        @self.client.on(events.NewMessage(chats=source_entity))
        async def _on_new_message(event: events.NewMessage.Event) -> None:
            await self._push_msg(event.message, source_entity.id)

        # Catch-up: process any messages from the last 24 h we might have missed
        await self._catchup(source_entity)

        logger.info("Real-time listener active for @%s", self.config.telegram.source_channel)
        await self.client.run_until_disconnected()

    # ------------------------------------------------------------------
    # Catch-up
    # ------------------------------------------------------------------

    async def _catchup(self, source_entity) -> None:
        """
        Push missed messages (up to 1 day back) to the queue in chronological
        order.  Skips any message_id already tracked in Redis or in-memory.
        """
        cutoff = datetime.now(timezone.utc) - CATCHUP_WINDOW

        # Step 1 — try Redis high-watermark
        last_id = await self.state.get_int(LAST_PROCESSED_KEY)
        logger.info("Catch-up: Redis last_processed_id=%s", last_id)

        # Step 2 — if Redis is empty, inspect the category channels themselves
        if last_id is None:
            last_id = await self._last_id_from_channels()
            if last_id is not None:
                logger.info("Catch-up: inferred last_id=%d from category channels", last_id)
            else:
                logger.info("Catch-up: no prior state found — scanning the last 24 h.")

        # Step 3 — iterate source channel newest→oldest, collect missed messages
        missed: list = []
        async for msg in self.client.iter_messages(source_entity):
            if msg.date.replace(tzinfo=timezone.utc) < cutoff:
                logger.info("Catch-up: reached 1-day cutoff at msg id=%d  date=%s", msg.id, msg.date)
                break
            if last_id is not None and msg.id <= last_id:
                logger.info("Catch-up: reached already-processed boundary at msg id=%d", msg.id)
                break
            missed.append(msg)

        if not missed:
            logger.info("Catch-up: nothing to do — no missed messages found.")
            return

        # Step 4 — reverse to chronological order, then push
        missed.reverse()
        logger.info("Catch-up: pushing %d missed message(s) in chronological order.", len(missed))
        for msg in missed:
            await self._push_msg(msg, source_entity.id)
        logger.info("Catch-up complete.")

    async def _last_id_from_channels(self) -> int | None:
        """
        Look at each configured category channel and return the highest
        original source message ID we can detect.

        This only works when the router used ``forwardMessage`` (which preserves
        ``fwd_from.channel_post``).  With ``copyMessage`` the method returns
        ``None`` and the caller falls back to a full 1-day scan — which is safe.
        """
        max_id: int | None = None
        for cat in self.config.categories:
            if not cat.is_configured:
                logger.info("Catch-up: channel for '%s' not yet configured, skipping.", cat.name)
                continue
            try:
                messages = await self.client.get_messages(cat.channel_id, limit=1)
                if not messages:
                    logger.info("Catch-up: channel '%s' is empty.", cat.name)
                    continue
                last_msg = messages[0]
                if (
                    last_msg.fwd_from is not None
                    and last_msg.fwd_from.channel_post is not None
                ):
                    fwd_id: int = last_msg.fwd_from.channel_post
                    logger.info(
                        "Catch-up: channel '%s' last forwarded msg → original id=%d",
                        cat.name, fwd_id,
                    )
                    if max_id is None or fwd_id > max_id:
                        max_id = fwd_id
                else:
                    logger.info(
                        "Catch-up: channel '%s' last message is a copy "
                        "(no fwd_from) — cannot infer original ID.",
                        cat.name,
                    )
            except Exception as exc:
                logger.warning("Catch-up: could not read channel '%s': %s", cat.name, exc)
        return max_id

    # ------------------------------------------------------------------
    # Shared push helper
    # ------------------------------------------------------------------

    async def _push_msg(self, msg, source_chat_id: int) -> None:
        """Serialize a Telethon Message and push it to the Redis Stream."""
        if msg.id in self._pushed_ids:
            logger.debug("Skipping already-pushed msg id=%d", msg.id)
            return

        text: str = msg.raw_text or ""
        payload = {
            "message_id": msg.id,
            "chat_id": source_chat_id,
            "text": text,
            "date": msg.date.isoformat(),
            "has_media": msg.media is not None,
            "media_type": _media_type(msg.media),
        }
        await self.queue.push(payload)
        # Update the persistent high-watermark so restarts know where to resume
        await self.state.set_max(LAST_PROCESSED_KEY, msg.id)
        self._pushed_ids.add(msg.id)
        logger.info(
            "Queued msg id=%d  date=%s  has_media=%s  text_len=%d",
            msg.id, msg.date.date(), payload["has_media"], len(text),
        )

