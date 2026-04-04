from __future__ import annotations

import heapq
import logging
import time

from telethon import TelegramClient
from telethon.sessions import StringSession

from src.config import AppConfig, CategoryConfig
from src.queue_client import RedisQueue

logger = logging.getLogger(__name__)


class RouterService:
    """
    Reads classified messages from Redis and forwards them to the correct
    category channel using the **Telethon user client**.

    Using Telethon (user account) instead of the Bot API is necessary because
    the source channel (@stranaua) is not owned by us, so the bot cannot be
    added as a member and therefore cannot access message history via
    ``copyMessage``.  The user account already has read access to the public
    channel and write access to the private destination channels.

    ``forward_messages(..., drop_author=True)`` suppresses the
    "Forwarded from" header so posts appear native in the destination channel.

    Strict ordering
    ---------------
    The listener stamps every queued group with a monotonic ``seq`` number.
    The classifier passes it through unchanged.  Here we buffer arriving
    payloads in a min-heap keyed by ``seq`` and only forward the head when
    its ``seq`` equals ``_next_seq``.  If the next expected seq has not
    arrived within ``_REORDER_WINDOW`` seconds we log a warning and skip
    ahead, preventing a single stalled message from blocking the pipeline.
    """

    # Seconds to wait for a missing seq before skipping ahead.
    _REORDER_WINDOW: float = 30.0
    # Redis read timeout (ms) — short so gap-timeout checks run frequently.
    _READ_TIMEOUT_MS: int = 1_000

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = TelegramClient(
            StringSession(config.telegram.session_string or None),
            config.telegram.api_id,
            config.telegram.api_hash,
        )
        self._queue = RedisQueue(
            url=config.redis.url,
            stream=config.redis.classified_stream,
            group=config.redis.consumer_group,
            consumer_name="router",
        )
        # Strict-ordering state
        self._next_seq: int = 0
        self._heap: list[tuple[int, int, dict]] = []  # (seq, insertion_order, payload)
        self._heap_ctr: int = 0        # tie-breaker so heapq never compares dicts
        self._gap_since: float | None = None  # monotonic time when gap first noticed

    def _resolve_channel(self, category: str) -> CategoryConfig | None:
        return self.config.category_map.get(category)

    async def _forward(self, target_channel_id: str, message_ids: list[int]) -> bool:
        try:
            # Convert numeric string IDs (e.g. "-1003827611915") to int so
            # Telethon can resolve them; @usernames are kept as strings.
            try:
                dest = int(target_channel_id)
            except (ValueError, TypeError):
                dest = target_channel_id

            await self.client.forward_messages(
                entity=dest,
                messages=message_ids,   # list → forwards all album parts at once
                from_peer=self._source_entity,
                drop_author=True,
            )
            return True
        except Exception as exc:
            logger.error("forward_messages failed: %s", exc)
            return False

    async def _route(self, payload: dict) -> None:
        """Forward one classified payload to its destination channel."""
        category: str = payload["category"]
        message_ids: list[int] = payload["message_ids"]
        seq: int = payload.get("seq", -1)

        cat_cfg = self._resolve_channel(category)

        if cat_cfg is not None and cat_cfg.is_configured:
            ok = await self._forward(cat_cfg.channel_id, message_ids)
            if ok:
                logger.info(
                    "Routed seq=%d  %d msg(s) %s  →  %s (%s)",
                    seq, len(message_ids), message_ids, category, cat_cfg.channel_id,
                )
        else:
            unc_id = self.config.classification.uncategorized_channel_id
            if self.config.classification.uncategorized_is_configured:
                ok = await self._forward(unc_id, message_ids)
                if ok:
                    logger.info(
                        "Routed seq=%d  %d msg(s) %s  →  Uncategorized (%s)",
                        seq, len(message_ids), message_ids, unc_id,
                    )
            else:
                logger.warning(
                    "No channel configured for category '%s'; seq=%d msg=%s dropped.",
                    category, seq, message_ids,
                )

    async def _drain(self) -> None:
        """Forward all consecutive payloads from the heap starting at _next_seq."""
        while self._heap and self._heap[0][0] == self._next_seq:
            _, _, payload = heapq.heappop(self._heap)
            await self._route(payload)
            self._next_seq += 1
            self._gap_since = None

    async def run(self) -> None:
        await self._queue.connect()
        await self.client.start()

        # Resolve and cache the source channel entity so Telethon knows its
        # type (Channel vs User) before we try to forward from it.
        self._source_entity = await self.client.get_entity(
            self.config.telegram.source_channel
        )
        logger.info(
            "Router ready (Telethon). Source=%s  Watching stream: %s",
            self.config.telegram.source_channel,
            self.config.redis.classified_stream,
        )

        while True:
            payload = await self._queue.read_one(timeout_ms=self._READ_TIMEOUT_MS)

            if payload is not None:
                # seq defaults to _next_seq for payloads from before this feature
                seq: int = payload.get("seq", self._next_seq)
                heapq.heappush(self._heap, (seq, self._heap_ctr, payload))
                self._heap_ctr += 1

            # Check whether a gap has been waiting too long
            if self._heap and self._heap[0][0] > self._next_seq:
                now = time.monotonic()
                if self._gap_since is None:
                    self._gap_since = now
                    logger.debug(
                        "Ordering gap: waiting for seq=%d, earliest buffered=%d",
                        self._next_seq, self._heap[0][0],
                    )
                elif now - self._gap_since >= self._REORDER_WINDOW:
                    skipped = self._heap[0][0] - self._next_seq
                    logger.warning(
                        "Reorder timeout after %.0fs: skipping %d missing seq(s) (%d → %d)",
                        self._REORDER_WINDOW, skipped, self._next_seq, self._heap[0][0],
                    )
                    self._next_seq = self._heap[0][0]
                    self._gap_since = None
            else:
                self._gap_since = None

            await self._drain()

    async def close(self) -> None:
        await self._queue.close()
        await self.client.disconnect()

