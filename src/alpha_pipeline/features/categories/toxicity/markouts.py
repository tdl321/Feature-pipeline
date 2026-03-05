"""Markout feature for flow toxicity analysis.

Post-fill price change at multiple horizons (1s, 5s, 30s, 60s). Measures
whether a trade's counterparty was informed. Positive markouts indicate
toxic flow (price moved in the trade direction); negative markouts indicate
non-toxic flow (favorable for market makers).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_MARKOUT_HORIZONS_SECONDS = (1, 5, 30, 60)


@register_feature
class Markouts(Feature):
    """Post-trade markout at multiple time horizons."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="toxicity.markouts",
            category="toxicity",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=True,
            requires_cross_exchange=False,
            min_history_seconds=60,
            output_fields=(
                "markout_1s",
                "markout_5s",
                "markout_30s",
                "markout_60s",
                "trade_price",
                "trade_side",
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
        assert trades_df is not None

        ob = orderbook_df.sort("timestamp")
        tr = trades_df.sort("timestamp")

        if ob.is_empty() or tr.is_empty():
            return None

        # Determine the latest orderbook timestamp to know how far forward we
        # can look. We need at least max-horizon seconds of post-trade mid data.
        max_horizon = max(_MARKOUT_HORIZONS_SECONDS)
        ob_max_ts = ob["timestamp"][-1]

        # Walk backwards through trades to find one with enough post-trade data
        trade_row = None
        for idx in range(tr.height - 1, -1, -1):
            candidate_ts = tr["timestamp"][idx]
            if candidate_ts + max_horizon <= ob_max_ts:
                trade_row = tr.row(idx, named=True)
                break

        if trade_row is None:
            return None

        trade_ts = trade_row["timestamp"]
        trade_price = float(trade_row["price"])
        trade_side = str(trade_row["side"])

        # Side multiplier: +1 for buys, -1 for sells
        side_multiplier = 1.0 if trade_side == "buy" else -1.0

        # Compute markout at each horizon by finding the closest mid_price
        markouts: dict[str, float | str | None] = {}
        all_horizons_filled = True

        for horizon in _MARKOUT_HORIZONS_SECONDS:
            target_ts = trade_ts + horizon

            # Find the orderbook snapshot closest to target_ts (at or after)
            after_target = ob.filter(pl.col("timestamp") >= target_ts)
            if after_target.is_empty():
                markouts[f"markout_{horizon}s"] = None
                all_horizons_filled = False
                continue

            mid_val = after_target["mid_price"][0]
            if mid_val is None:
                markouts[f"markout_{horizon}s"] = None
                all_horizons_filled = False
                continue
            mid_at_horizon = float(mid_val)
            raw_markout = mid_at_horizon - trade_price
            adjusted_markout = raw_markout * side_multiplier
            markouts[f"markout_{horizon}s"] = round(adjusted_markout, 8)

        if not all_horizons_filled:
            # We require all four horizons to produce a valid output
            return None

        markouts["trade_price"] = round(trade_price, 8)
        markouts["trade_side"] = trade_side

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values=markouts,
        )
