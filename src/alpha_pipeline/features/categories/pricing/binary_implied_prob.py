"""Binary implied probability feature.

Black-Scholes inspired pricing for prediction markets where prices directly
represent probabilities in [0, 1]. Computes the implied probability from
the mid-price, the edge vs maximum uncertainty (0.5), a fair spread
estimate, and the complement edge between YES bid and NO ask.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.enums import OutcomeType
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_DEFAULT_BASE_SPREAD_FACTOR = 0.01


@register_feature
class BinaryImpliedProb(Feature):
    """Binary option implied probability and fair value metrics."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="pricing.binary_implied_prob",
            category="pricing",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=False,
            min_history_seconds=0,
            output_fields=(
                "implied_prob",
                "edge_vs_50",
                "fair_spread",
                "complement_edge",
                "real_complement_edge",
            ),
            parameters={"base_spread_factor": _DEFAULT_BASE_SPREAD_FACTOR},
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

        params = parameters or {}
        base_spread_factor = float(
            params.get("base_spread_factor", _DEFAULT_BASE_SPREAD_FACTOR)
        )

        # Use the most recent YES orderbook snapshot
        yes_df = orderbook_df.filter(pl.col("outcome") == OutcomeType.YES)
        if yes_df.is_empty():
            # Fallback: use all data if no outcome filtering available
            yes_df = orderbook_df
        df = yes_df.sort("timestamp")
        latest = df.tail(1)

        if latest.is_empty():
            return None

        row = latest.row(0, named=True)
        mid_price = row.get("mid_price")
        best_bid = row.get("best_bid")
        best_ask = row.get("best_ask")

        if mid_price is None or best_bid is None or best_ask is None:
            return None

        mid_price = float(mid_price)
        best_bid = float(best_bid)
        best_ask = float(best_ask)

        # In prediction markets, prices ARE probabilities in [0, 1]
        implied_prob = mid_price

        # Edge vs maximum uncertainty
        edge_vs_50 = abs(implied_prob - 0.5)

        # Fair spread: 2 * p * (1 - p) * base_spread_factor
        # Wider when probability is near 0.5 (maximum uncertainty)
        fair_spread = 2.0 * implied_prob * (1.0 - implied_prob) * base_spread_factor

        # Complement edge: YES bid vs NO ask complement (derived)
        # In a binary market: NO ask = 1 - YES ask, so
        # theoretical_edge = best_bid - (1 - best_ask)
        complement_edge = best_bid - (1.0 - best_ask)

        # Real complement edge from actual NO data when available
        real_complement_edge: float | None = None
        no_df = orderbook_df.filter(pl.col("outcome") == OutcomeType.NO)
        if not no_df.is_empty():
            no_latest = no_df.sort("timestamp").tail(1).row(0, named=True)
            no_best_ask = no_latest.get("best_ask")
            if no_best_ask is not None:
                # Real edge: YES bid - actual NO ask
                real_complement_edge = best_bid - float(no_best_ask)

        values: dict[str, float | None] = {
            "implied_prob": round(implied_prob, 6),
            "edge_vs_50": round(edge_vs_50, 6),
            "fair_spread": round(fair_spread, 8),
            "complement_edge": round(complement_edge, 8),
            "real_complement_edge": (
                round(real_complement_edge, 8) if real_complement_edge is not None else None
            ),
        }

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values=values,
        )
