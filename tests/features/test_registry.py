"""Tests for FeatureRegistry."""
from __future__ import annotations

from typing import Any

import polars as pl
import pytest

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.features.registry import FeatureRegistry
from alpha_pipeline.schemas.feature import FeatureSpec, FeatureOutput


# ---------------------------------------------------------------------------
# Test features
# ---------------------------------------------------------------------------

class _FeatureA(Feature):
    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="test.feature_a",
            category="test",
            version="1.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=False,
            min_history_seconds=0,
            output_fields=("value",),
        )

    def compute(
        self,
        orderbook_df: pl.DataFrame | None,
        trades_df: pl.DataFrame | None,
        market_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> FeatureOutput | None:
        return None


class _FeatureB(Feature):
    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="test.feature_b",
            category="test",
            version="1.0",
            requires_orderbook=False,
            requires_trades=True,
            requires_cross_exchange=False,
            min_history_seconds=0,
            output_fields=("value",),
            depends_on=("test.feature_a",),
        )

    def compute(
        self,
        orderbook_df: pl.DataFrame | None,
        trades_df: pl.DataFrame | None,
        market_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> FeatureOutput | None:
        return None


class TestFeatureRegistry:
    def test_register_and_get(self) -> None:
        reg = FeatureRegistry()
        feat = _FeatureA()
        reg.register(feat)
        assert "test.feature_a" in reg.all_names
        assert reg.get("test.feature_a") is feat

    def test_enable_disable(self) -> None:
        reg = FeatureRegistry()
        reg.register(_FeatureA())
        assert reg.is_enabled("test.feature_a")

        reg.disable("test.feature_a")
        assert not reg.is_enabled("test.feature_a")
        assert reg.get_enabled() == []

        reg.enable("test.feature_a")
        assert reg.is_enabled("test.feature_a")

    def test_enable_unknown_raises(self) -> None:
        reg = FeatureRegistry()
        with pytest.raises(KeyError, match="unknown"):
            reg.enable("unknown")

    def test_topo_sort(self) -> None:
        reg = FeatureRegistry()
        feat_a = _FeatureA()
        feat_b = _FeatureB()
        # Register B first to verify sorting works
        reg.register(feat_b)
        reg.register(feat_a)

        enabled = reg.get_enabled()
        names = [f.spec().name for f in enabled]
        # A should come before B since B depends on A
        assert names.index("test.feature_a") < names.index("test.feature_b")

    def test_get_nonexistent(self) -> None:
        reg = FeatureRegistry()
        assert reg.get("nonexistent") is None
