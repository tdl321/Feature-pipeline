"""Feature schemas for the alpha pipeline."""
from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import orjson
from pydantic import BaseModel, ConfigDict, Field
from typing import Self

from alpha_pipeline.schemas.enums import ExchangeId


class FeatureSpec(BaseModel):
    """Declarative specification of a single feature computation.

    Describes inputs, dependencies, versioning, and parameters so the
    pipeline can schedule and validate feature computation automatically.
    """

    model_config = ConfigDict(frozen=True)

    name: str
    category: str
    version: str
    requires_orderbook: bool
    requires_trades: bool
    requires_cross_exchange: bool
    min_history_seconds: int
    output_fields: tuple[str, ...]
    parameters: dict[str, float | int | str] = {}
    depends_on: tuple[str, ...] = ()


class FeatureOutput(BaseModel):
    """The result of a single feature computation for one market snapshot."""

    model_config = ConfigDict(frozen=True)

    feature_name: str
    timestamp: datetime
    market_id: str
    values: dict[str, float | int | bool | str | None]
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    trigger_event_ids: tuple[str, ...] = ()

    # -- Serialization ------------------------------------------------------

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes via orjson."""
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, data: bytes) -> Self:
        """Deserialize from JSON bytes via orjson."""
        return cls.model_validate(orjson.loads(data))


class FeatureVector(BaseModel):
    """A collection of feature outputs for a single market at a point in time."""

    model_config = ConfigDict(frozen=True)

    timestamp: datetime
    market_id: str
    exchange: ExchangeId
    features: tuple[FeatureOutput, ...]
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    trigger_event_ids: tuple[str, ...] = ()

    # -- Serialization ------------------------------------------------------

    def to_json_bytes(self) -> bytes:
        """Serialize to JSON bytes via orjson."""
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, data: bytes) -> Self:
        """Deserialize from JSON bytes via orjson."""
        return cls.model_validate(orjson.loads(data))
