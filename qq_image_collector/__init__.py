"""Event-driven QQ image collector."""

from .database import (
    FINAL_CATEGORIES,
    category_for_source,
    connect_database,
)
from .onebot import OneBotClient, OneBotError, OneBotPolicyError

__all__ = [
    "FINAL_CATEGORIES",
    "OneBotClient",
    "OneBotError",
    "OneBotPolicyError",
    "category_for_source",
    "connect_database",
]

__version__ = "1.1.5"
