"""Top-of-book imbalance feature.

The strongest short-term alpha signal per the QuantArb doc. Measures the
ratio of best bid size to total top-of-book liquidity. Values above 0.5
indicate buying pressure; below 0.5 indicate selling pressure.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_DEFAULT_LOOKBACK_ROWS = 20


@register_feature
class TobImbalance(Feature):
    """Top-of-book size imbalance with rolling mean and z-score."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="order_flow.tob_imbalance",
            category="order_flow",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=False,
            min_history_seconds=0,
            output_fields=(
                "tob_imbalance",
                "tob_imbalance_ma",
                "tob_imbalance_zscore",
            ),
            parameters={"lookback_rows": _DEFAULT_LOOKBACK_ROWS},
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
        lookback = int(params.get("lookback_rows", _DEFAULT_LOOKBACK_ROWS))

        # Compute imbalance for every row: bid_size / (bid_size + ask_size)
        df = orderbook_df.sort("timestamp").with_columns(
            (
                pl.col("best_bid_size")
                / (pl.col("best_bid_size") + pl.col("best_ask_size"))
            ).alias("imbalance")
        )

        # Drop rows where imbalance could not be computed (zero denominator / nulls)
        df = df.filter(pl.col("imbalance").is_not_null() & pl.col("imbalance").is_finite())

        if df.is_empty():
            return None

        # Current imbalance is the most recent row
        current_imbalance = float(df["imbalance"][-1])

        # Rolling statistics over the last `lookback` rows
        window = df.tail(lookback)
        imbalance_ma = float(window["imbalance"].mean())  # type: ignore[arg-type]

        imbalance_std = float(window["imbalance"].std())  # type: ignore[arg-type]
        if imbalance_std == 0 or math.isnan(imbalance_std):
            imbalance_zscore = 0.0
        else:
            imbalance_zscore = (current_imbalance - imbalance_ma) / imbalance_std

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values={
                "tob_imbalance": round(current_imbalance, 6),
                "tob_imbalance_ma": round(imbalance_ma, 6),
                "tob_imbalance_zscore": round(imbalance_zscore, 4),
            },
        )
