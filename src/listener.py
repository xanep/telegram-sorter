from __future__ import annotations

import asyncio
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


def _group_by_album(msgs: list) -> list[list]:
    """
    Group a chronologically-ordered list of messages by ``grouped_id``.

    Single messages become a one-element group.  Album parts (same
    ``grouped_id``) are collected into one group while preserving the
    overall chronological position of the album's first part.
    """
    groups: list[list] = []
    seen: dict[int, list] = {}
    for msg in msgs:
        if msg.grouped_id:
            if msg.grouped_id not in seen:
                group: list = []
                seen[msg.grouped_id] = group
                groups.append(group)
            seen[msg.grouped_id].append(msg)
        else:
            groups.append([msg])
    return groups


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

    # How long to wait for more parts of an album before flushing (seconds).
    _ALBUM_FLUSH_DELAY: float = 1.0

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
        # Album grouping for real-time events: grouped_id → buffered messages
        self._album_buffer: dict[int, list] = {}
        # Pending flush tasks: grouped_id → asyncio.Task
        self._album_tasks: dict[int, asyncio.Task] = {}

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

        # Catch-up FIRST so historical messages are pushed in chronological order
        # before any live events enter the queue.  Registering the handler
        # afterwards introduces only a millisecond-scale gap; anything that
        # slips through will be caught by the high-watermark on next restart.
        await self._catchup(source_entity)

        # Register the real-time handler only after catch-up is complete.
        @self.client.on(events.NewMessage(chats=source_entity))
        async def _on_new_message(event: events.NewMessage.Event) -> None:
            msg = event.message
            if msg.grouped_id:
                # Buffer album parts and flush after a short delay
                self._album_buffer.setdefault(msg.grouped_id, []).append(msg)
                existing = self._album_tasks.pop(msg.grouped_id, None)
                if existing:
                    existing.cancel()
                self._album_tasks[msg.grouped_id] = asyncio.ensure_future(
                    self._flush_album(msg.grouped_id, source_entity.id)
                )
            else:
                await self._push_group([msg], source_entity.id)

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

        # Step 4 — reverse to chronological order, then group albums and push
        missed.reverse()
        groups = _group_by_album(missed)
        logger.info(
            "Catch-up: pushing %d group(s) from %d missed message(s) in chronological order.",
            len(groups), len(missed),
        )
        for group in groups:
            await self._push_group(group, source_entity.id)
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
    # Push helpers
    # ------------------------------------------------------------------

    async def _flush_album(self, grouped_id: int, source_chat_id: int) -> None:
        """Wait briefly for all album parts to arrive, then push as one group."""
        await asyncio.sleep(self._ALBUM_FLUSH_DELAY)
        msgs = self._album_buffer.pop(grouped_id, [])
        self._album_tasks.pop(grouped_id, None)
        if msgs:
            msgs.sort(key=lambda m: m.id)
            await self._push_group(msgs, source_chat_id)

    async def _push_group(self, msgs: list, source_chat_id: int) -> None:
        """Serialize a group of messages (1 or more) and push as one queue entry."""
        new_msgs = [m for m in msgs if m.id not in self._pushed_ids]
        if not new_msgs:
            logger.debug("Skipping already-pushed group ids=%s", [m.id for m in msgs])
            return

        # Combine text from all parts (caption is usually only on one of them)
        text: str = " ".join(m.raw_text for m in new_msgs if m.raw_text).strip()
        message_ids = [m.id for m in new_msgs]
        first = new_msgs[0]
        payload = {
            "message_ids": message_ids,
            "chat_id": source_chat_id,
            "text": text,
            "date": first.date.isoformat(),
            "has_media": any(m.media is not None for m in new_msgs),
            "media_type": _media_type(first.media),
        }
        await self.queue.push(payload)
        # Update high-watermark to the highest ID in the group
        await self.state.set_max(LAST_PROCESSED_KEY, max(message_ids))
        for m in new_msgs:
            self._pushed_ids.add(m.id)
        logger.info(
            "Queued %d msg(s) ids=%s  date=%s  has_media=%s  text_len=%d",
            len(new_msgs), message_ids, first.date.date(), payload["has_media"], len(text),
        )

