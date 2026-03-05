"""Cross-exchange arbitrage spread feature.

The core alpha signal: for the same outcome across exchanges, compute the
spread between the best bid on one exchange and the best ask on another.
A positive arb_spread indicates a live arbitrage opportunity.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from alpha_pipeline.features.base import Feature, register_feature
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec


@register_feature
class ArbSpread(Feature):
    """Cross-exchange arbitrage spread for identical outcomes."""

    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="cross_exchange.arb_spread",
            category="cross_exchange",
            version="1.0.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=True,
            min_history_seconds=0,
            output_fields=(
                "arb_spread",
                "arb_spread_bps",
                "bid_exchange",
                "ask_exchange",
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

        assert orderbook_df is not None  # guaranteed by validate_input

        # Need at least two distinct exchanges to compute a cross-exchange spread
        exchanges = orderbook_df.select("exchange").unique()
        if exchanges.height < 2:
            return None

        # For each outcome, find the best bid and best ask across exchanges.
        # We pick the outcome with the widest arb spread.
        outcomes = orderbook_df.select("outcome").unique().to_series().to_list()
        if not outcomes:
            return None

        best_arb_spread: float | None = None
        best_result: dict[str, Any] | None = None

        for outcome in outcomes:
            outcome_df = orderbook_df.filter(pl.col("outcome") == outcome)

            # Best bid: highest bid across all exchanges
            best_bid_row = outcome_df.sort("best_bid", descending=True).head(1)
            # Best ask: lowest ask across all exchanges
            best_ask_row = outcome_df.sort("best_ask", descending=False).head(1)

            if best_bid_row.is_empty() or best_ask_row.is_empty():
                continue

            best_bid_val = best_bid_row["best_bid"][0]
            best_ask_val = best_ask_row["best_ask"][0]
            bid_exchange = best_bid_row["exchange"][0]
            ask_exchange = best_ask_row["exchange"][0]

            if best_bid_val is None or best_ask_val is None:
                continue

            # Only meaningful if bid and ask are on different exchanges
            if bid_exchange == ask_exchange:
                continue

            arb_spread = float(best_bid_val) - float(best_ask_val)
            arb_spread_bps = arb_spread * 10_000

            if best_arb_spread is None or arb_spread > best_arb_spread:
                best_arb_spread = arb_spread
                best_result = {
                    "arb_spread": round(arb_spread, 8),
                    "arb_spread_bps": round(arb_spread_bps, 4),
                    "bid_exchange": str(bid_exchange),
                    "ask_exchange": str(ask_exchange),
                }

        if best_result is None:
            return None

        return FeatureOutput(
            feature_name=self.spec().name,
            timestamp=datetime.now(timezone.utc),
            market_id=market_id,
            values=best_result,
        )
