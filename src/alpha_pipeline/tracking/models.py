"""Immutable data models for wallet tracking snapshots."""
from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict


def raw_to_usd(value: str | int | float | None) -> float:
    """Convert a raw USDC integer string (6 decimals) to USD float.

    Handles strings like ``"1500000"`` → ``1.5``, already-float values,
    and returns ``0.0`` for None / empty / unparseable inputs.
    """
    if value is None:
        return 0.0
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return value / 1_000_000
    value = value.strip()
    if not value:
        return 0.0
    try:
        return int(value) / 1_000_000
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return 0.0


class WalletPosition(BaseModel):
    model_config = ConfigDict(frozen=True)

    market_slug: str
    title: str = ""
    status: str = ""
    outcome: str = ""
    shares: float = 0.0
    cost_usd: float = 0.0
    fill_price: float = 0.0
    market_value_usd: float = 0.0
    realized_pnl_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0


class PnlSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    timeframe: str = "7d"
    current_value: float = 0.0
    previous_value: float = 0.0
    percent_change: float = 0.0
    realized_pnl: float = 0.0


class TradedVolume(BaseModel):
    model_config = ConfigDict(frozen=True)

    total_volume_usd: float = 0.0
    raw_fields: dict[str, object] = {}


class WalletSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    address: str
    exchange: str
    polled_at: datetime
    positions: list[WalletPosition] = []
    pnl_summary: PnlSummary | None = None
    traded_volume: TradedVolume | None = None
