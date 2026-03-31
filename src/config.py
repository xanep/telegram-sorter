from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Env-var substitution
# ---------------------------------------------------------------------------

def _expand_env(value: object) -> object:
    """Recursively replace ``${VAR_NAME:-default}`` patterns with env values."""
    if isinstance(value, str):
        def _replace(m: re.Match) -> str:
            var, _, default = m.group(1).partition(":-")
            return os.environ.get(var.strip(), default)
        return re.sub(r"\$\{([^}]+)\}", _replace, value)
    if isinstance(value, dict):
        return {k: _expand_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    return value


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------

class TelegramConfig(BaseModel):
    api_id: int
    api_hash: str
    bot_token: str
    session_string: str = ""   # Empty → Telethon will prompt interactively
    source_channel: str        # Channel username without @


class OpenAIConfig(BaseModel):
    api_key: str
    model: str = "gpt-4o-mini"
    temperature: float = 0.1
    max_tokens: int = 150


class RedisConfig(BaseModel):
    url: str = "redis://localhost:6379"
    messages_stream: str = "telegram:messages"
    classified_stream: str = "telegram:classified"
    consumer_group: str = "sorter"
    block_ms: int = 5000


class CategoryConfig(BaseModel):
    name: str
    description: str
    channel_id: str

    @property
    def is_configured(self) -> bool:
        """False when the channel_id is still a placeholder."""
        return bool(self.channel_id) and not self.channel_id.startswith("PLACEHOLDER")


class ClassificationConfig(BaseModel):
    confidence_threshold: float = 0.70
    fallback_category: str = "Uncategorized"
    uncategorized_channel_id: str = ""

    @property
    def uncategorized_is_configured(self) -> bool:
        return (
            bool(self.uncategorized_channel_id)
            and not self.uncategorized_channel_id.startswith("PLACEHOLDER")
        )


class AppConfig(BaseModel):
    telegram: TelegramConfig
    openai: OpenAIConfig
    redis: RedisConfig
    categories: list[CategoryConfig]
    classification: ClassificationConfig

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, path: str = "config.yaml") -> "AppConfig":
        # Load .env before expanding variables so ${VAR} references resolve correctly.
        load_dotenv(override=False)
        config_path = Path(path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with config_path.open(encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        raw = _expand_env(raw)
        return cls(**raw)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @property
    def category_map(self) -> dict[str, CategoryConfig]:
        return {cat.name: cat for cat in self.categories}

    @property
    def category_names(self) -> list[str]:
        return [cat.name for cat in self.categories]

