from __future__ import annotations

import asyncio
import logging

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
    """

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

        async for _redis_id, payload in self._queue.consume(self.config.redis.block_ms):
            category: str = payload["category"]
            message_ids: list[int] = payload["message_ids"]

            cat_cfg = self._resolve_channel(category)

            if cat_cfg is not None and cat_cfg.is_configured:
                ok = await self._forward(cat_cfg.channel_id, message_ids)
                if ok:
                    logger.info(
                        "Routed %d msg(s) %s  →  %s (%s)",
                        len(message_ids), message_ids, category, cat_cfg.channel_id,
                    )
            else:
                unc_id = self.config.classification.uncategorized_channel_id
                if self.config.classification.uncategorized_is_configured:
                    ok = await self._forward(unc_id, message_ids)
                    if ok:
                        logger.info(
                            "Routed %d msg(s) %s  →  Uncategorized (%s)",
                            len(message_ids), message_ids, unc_id,
                        )
                else:
                    logger.warning(
                        "No channel configured for category '%s'; msg=%s dropped.",
                        category, message_ids,
                    )

    async def close(self) -> None:
        await self._queue.close()
        await self.client.disconnect()

