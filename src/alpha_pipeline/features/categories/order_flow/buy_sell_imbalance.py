"""Buy/sell imbalance feature.

Rolling taker buy/sell ratio from trade data. Captures retail directional
bias by comparing buy-side vs sell-side volume over a configurable time
window.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec

_DEFAULT_WINDOW_SECONDS = 60


@register_feature
class BuySellImbalance(Feature):
    """Rolling buy/sell volume ratio from trade flow."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="order_flow.buy_sell_imbalance",
            category="order_flow",
            version="1.0.0",
            requires_orderbook=False,
            requires_trades=True,
            requires_cross_exchange=False,
            min_history_seconds=30,
            output_fields=(
                "buy_sell_ratio",
                "net_flow",
                "buy_volume",
                "sell_volume",
                "trade_count",
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

        assert trades_df is not None

        params = parameters or {}
        window_seconds = int(params.get("window_seconds", _DEFAULT_WINDOW_SECONDS))

        # Sort by timestamp and apply rolling window
        df = trades_df.sort("timestamp")
        if df.is_empty():
            return None

        latest_ts = df["timestamp"][-1]
        window_start = latest_ts - timedelta(seconds=window_seconds)
        windowed = df.filter(pl.col("timestamp") >= window_start)

        if windowed.is_empty():
            return None

        # Aggregate buy and sell volumes
        buy_trades = windowed.filter(pl.col("side") == "buy")
        sell_trades = windowed.filter(pl.col("side") == "sell")

        buy_volume = float(buy_trades["size"].sum()) if not buy_trades.is_empty() else 0.0
        sell_volume = float(sell_trades["size"].sum()) if not sell_trades.is_empty() else 0.0
        trade_count = windowed.height

        total_volume = buy_volume + sell_volume
        if total_volume == 0:
            buy_sell_ratio = 0.5  # neutral when no volume
        else:
            buy_sell_ratio = buy_volume / total_volume

        net_flow = buy_volume - sell_volume

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values={
                "buy_sell_ratio": round(buy_sell_ratio, 6),
                "net_flow": round(net_flow, 6),
                "buy_volume": round(buy_volume, 6),
                "sell_volume": round(sell_volume, 6),
                "trade_count": trade_count,
            },
        )
