from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import polars as pl

from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec


class Feature(ABC):
    """Base class for all features."""

    @abstractmethod
    def spec(self) -> FeatureSpec:
        """Return the feature specification."""
        ...

    @abstractmethod
    def compute(
        self,
        orderbook_df: pl.DataFrame | None,
        trades_df: pl.DataFrame | None,
        market_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> FeatureOutput | None:
        """Compute the feature from data.

        Returns None if insufficient data.
        Pure function -- no side effects.
        """
        ...

    def validate_input(
        self,
        orderbook_df: pl.DataFrame | None,
        trades_df: pl.DataFrame | None,
    ) -> bool:
        """Check if required data is available."""
        s = self.spec()
        if s.requires_orderbook and (orderbook_df is None or orderbook_df.is_empty()):
            return False
        if s.requires_trades and (trades_df is None or trades_df.is_empty()):
            return False
        return True


# ---------------------------------------------------------------------------
# Feature registry decorator
# ---------------------------------------------------------------------------

_FEATURE_REGISTRY: dict[str, type[Feature]] = {}


def register_feature(cls: type[Feature]) -> type[Feature]:
    """Decorator to auto-register a Feature subclass."""
    # Instantiate to get spec name
    instance = cls()
    _FEATURE_REGISTRY[instance.spec().name] = cls
    return cls


def get_registered_features() -> dict[str, type[Feature]]:
    return dict(_FEATURE_REGISTRY)
