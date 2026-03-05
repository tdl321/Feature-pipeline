"""Normalized orderbook schemas for the alpha pipeline."""
from __future__ import annotations

from datetime import datetime, timezone
from functools import cached_property
from typing import Self

import orjson
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_serializer

from alpha_pipeline.schemas.enums import ExchangeId, OutcomeType


def _orjson_dumps(v: object, *, default: object = None) -> str:
    return orjson.dumps(v, default=default).decode()


class OrderbookLevel(BaseModel):
    """A single price/size level in an orderbook."""

    model_config = ConfigDict(frozen=True)

    price: float = Field(ge=0.0, le=1.0)
    size: float = Field(ge=0.0)


class NormalizedOrderbook(BaseModel):
    """Exchange-agnostic orderbook representation for prediction markets.

    Bids are stored descending by price; asks ascending by price.
    All prices are bounded [0, 1].
    """

    model_config = ConfigDict(
        frozen=True,
        json_encoders={datetime: lambda v: v.isoformat()},
    )

    exchange: ExchangeId
    market_id: str
    asset_id: str
    outcome: OutcomeType
    bids: tuple[OrderbookLevel, ...] = ()
    asks: tuple[OrderbookLevel, ...] = ()
    local_timestamp: datetime
    exchange_timestamp: datetime | None = None

    # -- Computed properties ------------------------------------------------

    @cached_property
    def best_bid(self) -> float | None:
        """Price of the highest bid, or None if bids are empty."""
        return self.bids[0].price if self.bids else None

    @cached_property
    def best_ask(self) -> float | None:
        """Price of the lowest ask, or None if asks are empty."""
        return self.asks[0].price if self.asks else None

    @cached_property
    def mid_price(self) -> float | None:
        """Midpoint between best bid and best ask, or None."""
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2.0
        return None

    @cached_property
    def spread(self) -> float | None:
        """Spread between best ask and best bid, or None."""
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    # -- Validators ---------------------------------------------------------

    @field_validator("bids")
    @classmethod
    def _bids_descending(
        cls, v: tuple[OrderbookLevel, ...]
    ) -> tuple[OrderbookLevel, ...]:
        for i in range(1, len(v)):
            if v[i].price > v[i - 1].price:
                msg = "Bids must be sorted descending by price."
                raise ValueError(msg)
        return v

    @field_validator("asks")
    @classmethod
    def _asks_ascending(
        cls, v: tuple[OrderbookLevel, ...]
    ) -> tuple[OrderbookLevel, ...]:
        for i in range(1, len(v)):
            if v[i].price < v[i - 1].price:
                msg = "Asks must be sorted ascending by price."
                raise ValueError(msg)
        return v

    # -- Serialization ------------------------------------------------------

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes via orjson."""
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, data: bytes) -> Self:
        """Deserialize from JSON bytes via orjson."""
        return cls.model_validate(orjson.loads(data))

    # -- Factory ------------------------------------------------------------

    @classmethod
    def from_raw(
        cls,
        exchange: ExchangeId,
        market_id: str,
        asset_id: str,
        outcome: OutcomeType,
        bids_raw: list[tuple[float, float]],
        asks_raw: list[tuple[float, float]],
        exchange_timestamp: datetime | None = None,
    ) -> Self:
        """Build a NormalizedOrderbook from unsorted raw bid/ask tuples.

        Args:
            exchange: Source exchange identifier.
            market_id: Exchange-specific market identifier.
            asset_id: Exchange-specific asset/token identifier.
            outcome: YES or NO outcome side.
            bids_raw: List of (price, size) tuples for bids (any order).
            asks_raw: List of (price, size) tuples for asks (any order).
            exchange_timestamp: Optional timestamp from the exchange.

        Returns:
            A fully validated, frozen NormalizedOrderbook instance.
        """
        sorted_bids = tuple(
            OrderbookLevel(price=p, size=s)
            for p, s in sorted(bids_raw, key=lambda x: x[0], reverse=True)
        )
        sorted_asks = tuple(
            OrderbookLevel(price=p, size=s)
            for p, s in sorted(asks_raw, key=lambda x: x[0])
        )
        return cls(
            exchange=exchange,
            market_id=market_id,
            asset_id=asset_id,
            outcome=outcome,
            bids=sorted_bids,
            asks=sorted_asks,
            local_timestamp=datetime.now(tz=timezone.utc),
            exchange_timestamp=exchange_timestamp,
        )
