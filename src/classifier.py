from __future__ import annotations

import json
import logging

from openai import AsyncOpenAI
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from src.config import AppConfig
from src.queue_client import RedisQueue

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a news classifier for a Telegram channel.
Classify each news message into exactly one of the provided categories.
The messages are in Russian or Ukrainian — classify based on meaning, not language.
Respond ONLY with valid JSON. Do not add any explanation outside the JSON object.\
"""


def _build_user_prompt(text: str, categories: list[dict]) -> str:
    cats = "\n".join(
        f'  - "{c["name"]}": {c["description"]}' for c in categories
    )
    return f"""\
Classify the following news message into exactly one category.

Available categories:
{cats}
  - "Uncategorized": Use this when the message does not clearly fit any category above.

Message (may be in Russian or Ukrainian):
\"\"\"
{text[:3000]}
\"\"\"

Respond with JSON only, in this exact format:
{{"category": "<exact category name>", "confidence": <0.0-1.0>, "reason": "<brief reason in English>"}}\
"""


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class ClassifierService:
    """
    Reads raw messages from the incoming Redis Stream, calls gpt-4o-mini to
    classify them, then writes results to the outgoing Redis Stream.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._openai = AsyncOpenAI(api_key=config.openai.api_key)
        self._categories = [
            {"name": c.name, "description": c.description}
            for c in config.categories
        ]
        self._valid_names = {c.name for c in config.categories} | {
            config.classification.fallback_category
        }
        self._in = RedisQueue(
            url=config.redis.url,
            stream=config.redis.messages_stream,
            group=config.redis.consumer_group,
            consumer_name="classifier",
        )
        self._out = RedisQueue(
            url=config.redis.url,
            stream=config.redis.classified_stream,
            group=config.redis.consumer_group,
            consumer_name="classifier-out",
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type(Exception),
        reraise=True,
    )
    async def _call_llm(self, text: str) -> dict:
        response = await self._openai.chat.completions.create(
            model=self.config.openai.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": _build_user_prompt(text, self._categories)},
            ],
            response_format={"type": "json_object"},
            temperature=self.config.openai.temperature,
            max_tokens=self.config.openai.max_tokens,
        )
        return json.loads(response.choices[0].message.content)

    async def _classify(self, text: str) -> tuple[str, float, str]:
        """Return (category, confidence, reason), falling back on any error."""
        fallback = self.config.classification.fallback_category
        threshold = self.config.classification.confidence_threshold
        try:
            result = await self._call_llm(text)
            category = result.get("category", fallback)
            confidence = float(result.get("confidence", 0.0))
            reason = result.get("reason", "")
            if category not in self._valid_names or confidence < threshold:
                return fallback, confidence, reason
            return category, confidence, reason
        except Exception as exc:
            logger.error("LLM call failed: %s", exc)
            return fallback, 0.0, str(exc)

    async def run(self) -> None:
        await self._in.connect()
        await self._out.connect()
        logger.info("Classifier ready. Watching stream: %s", self.config.redis.messages_stream)

        async for _redis_id, payload in self._in.consume(self.config.redis.block_ms):
            text = payload.get("text", "").strip()

            if text:
                category, confidence, reason = await self._classify(text)
            else:
                category, confidence, reason = (
                    self.config.classification.fallback_category, 0.0, "No text content"
                )

            classified = {
                "seq": payload.get("seq", 0),
                "message_ids": payload["message_ids"],
                "chat_id": payload["chat_id"],
                "category": category,
                "confidence": confidence,
                "reason": reason,
            }
            await self._out.push(classified)
            logger.info(
                "msg=%s  →  %s  (confidence=%.2f)  reason: %s",
                payload["message_ids"], category, confidence, reason,
            )

