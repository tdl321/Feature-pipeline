"""Enumerations for the alpha pipeline."""
from __future__ import annotations

from enum import StrEnum


class ExchangeId(StrEnum):
    POLYMARKET = "polymarket"
    OPINION = "opinion"
    LIMITLESS = "limitless"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class OutcomeType(StrEnum):
    YES = "yes"
    NO = "no"


class DataEventType(StrEnum):
    ORDERBOOK_SNAPSHOT = "orderbook_snapshot"
    TRADE = "trade"
