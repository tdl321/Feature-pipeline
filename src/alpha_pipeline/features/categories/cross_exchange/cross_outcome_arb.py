"""Cross-outcome arbitrage feature.

Detects when YES_best_ask + NO_best_ask < 1.0 on the same exchange,
indicating a guaranteed profit opportunity (buy YES + buy NO for less
than 1.0, one must pay out).  This is the core signal for same-exchange
cross-outcome arb strategies.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.enums import OutcomeType
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec


@register_feature
class CrossOutcomeArb(Feature):
    """Same-exchange cross-outcome arbitrage detection."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="cross_exchange.cross_outcome_arb",
            category="cross_exchange",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=False,
            min_history_seconds=0,
            output_fields=(
                "cross_outcome_spread",
                "cross_outcome_spread_bps",
                "yes_ask",
                "no_ask",
            ),
        )

    def compute(
        self,
        orderbook_df: pl.DataFrame | None,
        trades_df: pl.DataFrame | None,
        market_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> FeatureOutput | None:
        if not self.validate_input(orderbook_df, trades_df):
            return None

        assert orderbook_df is not None

        # Need both YES and NO outcome data in the buffer
        outcomes = orderbook_df.select("outcome").unique().to_series().to_list()
        has_yes = OutcomeType.YES in outcomes or "yes" in outcomes
        has_no = OutcomeType.NO in outcomes or "no" in outcomes

        if not (has_yes and has_no):
            return None

        # Get the latest YES orderbook row
        yes_df = (
            orderbook_df
            .filter(pl.col("outcome") == OutcomeType.YES)
            .sort("timestamp")
            .tail(1)
        )
        # Get the latest NO orderbook row
        no_df = (
            orderbook_df
            .filter(pl.col("outcome") == OutcomeType.NO)
            .sort("timestamp")
            .tail(1)
        )

        if yes_df.is_empty() or no_df.is_empty():
            return None

        yes_row = yes_df.row(0, named=True)
        no_row = no_df.row(0, named=True)

        yes_ask = yes_row.get("best_ask")
        no_ask = no_row.get("best_ask")

        if yes_ask is None or no_ask is None:
            return None

        yes_ask = float(yes_ask)
        no_ask = float(no_ask)

        # Positive spread = arb exists (buy both for less than 1.0)
        cross_outcome_spread = 1.0 - (yes_ask + no_ask)
        cross_outcome_spread_bps = cross_outcome_spread * 10_000

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values={
                "cross_outcome_spread": round(cross_outcome_spread, 8),
                "cross_outcome_spread_bps": round(cross_outcome_spread_bps, 4),
                "yes_ask": round(yes_ask, 6),
                "no_ask": round(no_ask, 6),
            },
        )
