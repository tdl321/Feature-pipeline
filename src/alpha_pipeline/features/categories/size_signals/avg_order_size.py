"""Average order size feature with z-score normalization.

From the QuantArb doc: z-score normalized average trade size detects
outsized retail or institutional activity. A large z-score relative to
recent history flags unusual sizing patterns.
"""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_DEFAULT_WINDOW_SECONDS = 300
_DEFAULT_ZSCORE_THRESHOLD = 2.0


@register_feature
class AvgOrderSize(Feature):
    """Z-score normalized average trade size over a rolling window."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="size_signals.avg_order_size",
            category="size_signals",
            version="1.0.0",
            requires_orderbook=False,
            requires_trades=True,
            requires_cross_exchange=False,
            min_history_seconds=60,
            output_fields=(
                "avg_trade_size",
                "size_zscore",
                "is_retail_signal",
                "trade_count",
            ),
            parameters={
                "window_seconds": _DEFAULT_WINDOW_SECONDS,
                "zscore_threshold": _DEFAULT_ZSCORE_THRESHOLD,
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

        assert trades_df is not None

        params = parameters or {}
        window_seconds = int(params.get("window_seconds", _DEFAULT_WINDOW_SECONDS))
        zscore_threshold = float(params.get("zscore_threshold", _DEFAULT_ZSCORE_THRESHOLD))

        df = trades_df.sort("timestamp")
        if df.is_empty():
            return None

        latest_ts = df["timestamp"][-1]
        window_start = latest_ts - window_seconds
        windowed = df.filter(pl.col("timestamp") >= window_start)

        if windowed.is_empty():
            return None

        trade_count = windowed.height
        total_volume = float(windowed["size"].sum())
        avg_trade_size = total_volume / trade_count

        # Compute z-score by breaking the window into sub-intervals and
        # calculating the average size per sub-interval, then z-scoring
        # the latest sub-interval.  We use a simple approach: compute
        # rolling average sizes per trade, then z-score the current avg.
        sizes = windowed["size"].to_list()
        sizes_float = [float(s) for s in sizes]

        if len(sizes_float) < 2:
            size_zscore = 0.0
        else:
            mean_size = sum(sizes_float) / len(sizes_float)
            variance = sum((s - mean_size) ** 2 for s in sizes_float) / len(sizes_float)
            std_size = math.sqrt(variance)

            if std_size == 0:
                size_zscore = 0.0
            else:
                # Z-score of the most recent trade size vs the window
                latest_size = sizes_float[-1]
                size_zscore = (latest_size - mean_size) / std_size

        is_retail_signal = abs(size_zscore) > zscore_threshold

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values={
                "avg_trade_size": round(avg_trade_size, 6),
                "size_zscore": round(size_zscore, 4),
                "is_retail_signal": is_retail_signal,
                "trade_count": trade_count,
            },
        )
