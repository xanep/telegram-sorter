from __future__ import annotations

import json
import logging
import socket
from typing import Any, AsyncIterator

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)


class RedisQueue:
    """
    Thin wrapper around Redis Streams for durable message passing.

    Producer side  → ``push(payload)``
    Consumer side  → ``async for msg_id, payload in queue.consume(): …``

    Messages are acknowledged automatically after the ``async for`` body
    completes, giving at-least-once delivery semantics.
    """

    def __init__(
        self,
        url: str,
        stream: str,
        group: str,
        consumer_name: str | None = None,
    ) -> None:
        self.url = url
        self.stream = stream
        self.group = group
        # Unique consumer name per pod/process (used by Redis for tracking)
        self.consumer_name = consumer_name or socket.gethostname()
        self._client: aioredis.Redis | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        self._client = aioredis.from_url(self.url, decode_responses=True)
        await self._ensure_group()
        logger.info("RedisQueue connected: stream=%s group=%s", self.stream, self.group)

    async def _ensure_group(self) -> None:
        """Create the consumer group if it doesn't already exist."""
        try:
            await self._client.xgroup_create(
                self.stream, self.group, id="0", mkstream=True
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Producer
    # ------------------------------------------------------------------

    async def push(self, payload: dict[str, Any]) -> str:
        """Serialize ``payload`` to JSON and append it to the stream."""
        msg_id: str = await self._client.xadd(
            self.stream, {"data": json.dumps(payload, default=str)}
        )
        logger.debug("Pushed %s → %s", msg_id, self.stream)
        return msg_id

    # ------------------------------------------------------------------
    # Consumer
    # ------------------------------------------------------------------

    async def consume(self, block_ms: int = 5000) -> AsyncIterator[tuple[str, dict]]:
        """
        Yield ``(redis_msg_id, payload_dict)`` for each new message.
        Automatically acknowledges after yielding (at-least-once delivery).
        """
        while True:
            entries = await self._client.xreadgroup(
                groupname=self.group,
                consumername=self.consumer_name,
                streams={self.stream: ">"},
                count=1,
                block=block_ms,
            )
            if not entries:
                continue  # timeout, loop back and wait again

            for _stream_name, messages in entries:
                for redis_msg_id, fields in messages:
                    payload: dict = json.loads(fields["data"])
                    yield redis_msg_id, payload
                    # Acknowledge only after the consumer has processed it
                    await self._client.xack(self.stream, self.group, redis_msg_id)
                    logger.debug("Acked %s from %s", redis_msg_id, self.stream)


class RedisState:
    """
    Simple Redis key-value store for application-level state.

    Used by the listener to persist the high-watermark message ID so that
    a restart can resume from where it left off instead of replaying all
    messages from the last day.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self._client: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(self.url, decode_responses=True)
        logger.info("RedisState connected: %s", self.url)

    async def get_int(self, key: str) -> int | None:
        """Return the integer value stored at ``key``, or ``None`` if absent."""
        value = await self._client.get(key)
        return int(value) if value is not None else None

    async def set_max(self, key: str, value: int) -> None:
        """Atomically set ``key`` to ``max(current, value)``."""
        current = await self.get_int(key)
        if current is None or value > current:
            await self._client.set(key, str(value))
            logger.debug("State %s ← %d", key, value)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

