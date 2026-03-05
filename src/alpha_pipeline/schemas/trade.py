"""Normalized trade schema for the alpha pipeline."""
from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import orjson
from pydantic import BaseModel, ConfigDict, Field
from typing import Self

from alpha_pipeline.schemas.enums import ExchangeId, OutcomeType, Side


class NormalizedTrade(BaseModel):
    """Exchange-agnostic trade representation for prediction markets.

    All prices are bounded [0, 1].
    """

    model_config = ConfigDict(
        frozen=True,
        json_encoders={datetime: lambda v: v.isoformat()},
    )

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    sequence_num: int = Field(default=0)
    exchange: ExchangeId
    market_id: str
    asset_id: str
    outcome: OutcomeType
    price: float = Field(ge=0.0, le=1.0)
    size: float = Field(ge=0.0)
    side: Side
    usd_notional: float | None = None
    local_timestamp: datetime
    exchange_timestamp: datetime | None = None
    mid_offset_bps: float | None = None
    """Deviation from mid-price at the time of the trade, in basis points."""

    # -- Serialization ------------------------------------------------------

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes via orjson."""
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, data: bytes) -> Self:
        """Deserialize from JSON bytes via orjson."""
        return cls.model_validate(orjson.loads(data))
