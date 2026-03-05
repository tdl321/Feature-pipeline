"""Spread dynamics feature.

BBA (best-bid/ask) spread analysis including the current spread, its
relative size vs mid-price, its percentile rank within a rolling window,
and whether the spread is widening beyond a threshold.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_DEFAULT_WINDOW_SECONDS = 300
_DEFAULT_WIDEN_THRESHOLD = 0.005


@register_feature
class SpreadDynamics(Feature):
    """BBA spread percentile, widening detection, and relative spread."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="spread_analysis.spread_dynamics",
            category="spread_analysis",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=False,
            min_history_seconds=30,
            output_fields=(
                "current_spread",
                "spread_pct",
                "spread_percentile",
                "is_widening",
                "avg_spread",
            ),
            parameters={
                "window_seconds": _DEFAULT_WINDOW_SECONDS,
                "widen_threshold": _DEFAULT_WIDEN_THRESHOLD,
            },
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
        window_seconds = int(params.get("window_seconds", _DEFAULT_WINDOW_SECONDS))
        widen_threshold = float(params.get("widen_threshold", _DEFAULT_WIDEN_THRESHOLD))

        df = orderbook_df.sort("timestamp")
        if df.is_empty():
            return None

        # Compute spread column for all rows
        df = df.with_columns(
            (pl.col("best_ask") - pl.col("best_bid")).alias("spread_raw")
        )

        # Current spread from the latest row
        latest_row = df.tail(1).row(0, named=True)
        current_spread = latest_row.get("spread_raw")
        mid_price = latest_row.get("mid_price")

        if current_spread is None or mid_price is None:
            return None

        current_spread = float(current_spread)
        mid_price = float(mid_price)

        # Relative spread
        if mid_price == 0:
            spread_pct = 0.0
        else:
            spread_pct = current_spread / mid_price

        # Rolling window for percentile and average
        latest_ts = latest_row["timestamp"]
        window_start = latest_ts - timedelta(seconds=window_seconds)
        windowed = df.filter(pl.col("timestamp") >= window_start)

        spreads = windowed["spread_raw"].drop_nulls().to_list()
        spreads_float = [float(s) for s in spreads]

        if not spreads_float:
            return None

        avg_spread = sum(spreads_float) / len(spreads_float)

        # Percentile rank: fraction of historical spreads <= current spread
        count_leq = sum(1 for s in spreads_float if s <= current_spread)
        spread_percentile = count_leq / len(spreads_float)

        # Widening detection: compare current spread to previous spread
        is_widening = False
        if df.height >= 2:
            prev_spread = float(df["spread_raw"][-2])
            if prev_spread > 0:
                spread_change_pct = (current_spread - prev_spread) / prev_spread
                is_widening = spread_change_pct > widen_threshold

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values={
                "current_spread": round(current_spread, 8),
                "spread_pct": round(spread_pct, 6),
                "spread_percentile": round(spread_percentile, 4),
                "is_widening": is_widening,
                "avg_spread": round(avg_spread, 8),
            },
        )
