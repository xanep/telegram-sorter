from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class IncomingMessage(BaseModel):
    """A raw message received from the Telegram source channel."""

    message_id: int
    chat_id: int
    # Plain text or caption; empty string for media-only posts.
    text: str
    date: datetime
    has_media: bool
    media_type: Optional[str] = None  # "photo", "document", "webpage", …

    @property
    def is_classifiable(self) -> bool:
        """True when there is enough text for the AI to make a decision."""
        return bool(self.text.strip())


class ClassifiedMessage(BaseModel):
    """Result produced by the classifier and consumed by the router."""

    message_id: int
    chat_id: int
    category: str        # Matches a category name from config, or "Uncategorized"
    confidence: float    # 0.0 – 1.0
    reason: str          # Brief English explanation from the LLM

