"""Wash trade detection feature.

Identifies trades that print inside the spread (between best bid and best
ask), which is impossible in a legitimate order book and a strong indicator
of wash trading.  Reports the ratio and volume of such trades over a
configurable rolling window.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_DEFAULT_WINDOW_SECONDS = 60


@register_feature
class WashDetection(Feature):
    """Rolling wash-trade detection from trades printed inside the spread."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="order_flow.wash_detection",
            category="order_flow",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=True,
            requires_cross_exchange=False,
            min_history_seconds=30,
            output_fields=(
                "wash_trade_ratio",
                "inside_spread_count",
                "total_trade_count",
                "wash_volume_ratio",
            ),
            parameters={"window_seconds": _DEFAULT_WINDOW_SECONDS},
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

        params = parameters or {}
        window_seconds = int(params.get("window_seconds", _DEFAULT_WINDOW_SECONDS))

        # Sort both frames by timestamp
        ob = orderbook_df.sort("timestamp")
        trades = trades_df.sort("timestamp")

        if trades.is_empty() or ob.is_empty():
            return None

        # Apply rolling window to trades
        latest_ts = trades["timestamp"][-1]
        window_start = latest_ts - window_seconds
        windowed = trades.filter(pl.col("timestamp") >= window_start)

        if windowed.is_empty():
            return None

        # Extract orderbook timestamps and best_bid/best_ask as lists
        # for efficient matching
        ob_timestamps = ob["timestamp"].to_list()
        ob_best_bids = ob["best_bid"].to_list()
        ob_best_asks = ob["best_ask"].to_list()

        inside_spread_count = 0
        inside_spread_volume = 0.0
        total_volume = 0.0
        evaluated_count = 0

        trade_timestamps = windowed["timestamp"].to_list()
        trade_prices = windowed["price"].to_list()
        trade_sizes = windowed["size"].to_list()

        for i in range(len(trade_timestamps)):
            trade_ts = trade_timestamps[i]
            trade_price = trade_prices[i]
            trade_size = trade_sizes[i]

            # Find the most recent orderbook snapshot before this trade
            best_bid, best_ask = _find_nearest_ob(
                trade_ts, ob_timestamps, ob_best_bids, ob_best_asks
            )

            if best_bid is None or best_ask is None:
                # No orderbook data available for this trade -- skip
                continue

            # Skip if spread is zero (bid == ask) -- no trade can be "inside"
            if best_bid >= best_ask:
                evaluated_count += 1
                total_volume += trade_size
                continue

            evaluated_count += 1
            total_volume += trade_size

            # Trade is inside the spread if strictly between bid and ask
            if best_bid < trade_price < best_ask:
                inside_spread_count += 1
                inside_spread_volume += trade_size

        if evaluated_count == 0:
            return None

        wash_trade_ratio = inside_spread_count / evaluated_count
        wash_volume_ratio = (
            inside_spread_volume / total_volume if total_volume > 0 else 0.0
        )

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values={
                "wash_trade_ratio": round(wash_trade_ratio, 6),
                "inside_spread_count": inside_spread_count,
                "total_trade_count": evaluated_count,
                "wash_volume_ratio": round(wash_volume_ratio, 6),
            },
        )


def _find_nearest_ob(
    trade_ts: float,
    ob_timestamps: list[float],
    ob_best_bids: list[float],
    ob_best_asks: list[float],
) -> tuple[float | None, float | None]:
    """Find the most recent orderbook snapshot at or before *trade_ts*.

    Uses a reverse linear scan.  For the typical buffer sizes (<10 000 rows)
    this is fast enough.  Returns ``(None, None)`` when no qualifying
    snapshot exists.
    """
    for j in range(len(ob_timestamps) - 1, -1, -1):
        if ob_timestamps[j] <= trade_ts:
            return ob_best_bids[j], ob_best_asks[j]
    return None, None
