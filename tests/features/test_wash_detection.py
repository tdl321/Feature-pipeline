"""Tests for wash trade detection feature."""
from __future__ import annotations

import time

import polars as pl
import pytest

from alpha_pipeline.features.categories.order_flow.wash_detection import WashDetection


def _make_orderbook_df(
    n: int,
    base_time: float,
    best_bid: float = 0.55,
    best_ask: float = 0.57,
) -> pl.DataFrame:
    """Create a synthetic orderbook DataFrame with controlled bid/ask."""
    return pl.DataFrame({
        "timestamp": [base_time + i for i in range(n)],
        "exchange": ["polymarket"] * n,
        "market_id": ["test_market"] * n,
        "asset_id": ["test_market"] * n,
        "outcome": ["yes"] * n,
        "best_bid": [best_bid] * n,
        "best_ask": [best_ask] * n,
        "mid_price": [(best_bid + best_ask) / 2.0] * n,
        "spread": [best_ask - best_bid] * n,
        "bid_depth": [5000.0] * n,
        "ask_depth": [4800.0] * n,
        "n_bid_levels": [5] * n,
        "n_ask_levels": [5] * n,
        "best_bid_size": [1000.0] * n,
        "best_ask_size": [800.0] * n,
    })


def _make_trades_df(
    base_time: float,
    prices: list[float],
    sizes: list[float] | None = None,
) -> pl.DataFrame:
    """Create a synthetic trades DataFrame with controlled prices."""
    n = len(prices)
    if sizes is None:
        sizes = [100.0] * n
    return pl.DataFrame({
        "timestamp": [base_time + i * 0.5 for i in range(n)],
        "exchange": ["polymarket"] * n,
        "market_id": ["test_market"] * n,
        "asset_id": ["test_market"] * n,
        "outcome": ["yes"] * n,
        "price": prices,
        "size": sizes,
        "side": ["buy"] * n,
        "usd_notional": [None] * n,
    })


class TestWashDetection:
    """Test suite for WashDetection feature."""

    def setup_method(self) -> None:
        self.feature = WashDetection()

    def test_spec(self) -> None:
        s = self.feature.spec()
        assert s.name == "order_flow.wash_detection"
        assert s.category == "order_flow"
        assert s.requires_orderbook is True
        assert s.requires_trades is True
        assert s.requires_cross_exchange is False
        assert s.min_history_seconds == 30
        assert "wash_trade_ratio" in s.output_fields
        assert "inside_spread_count" in s.output_fields
        assert "total_trade_count" in s.output_fields
        assert "wash_volume_ratio" in s.output_fields

    def test_trades_at_bid_ask_no_wash(self) -> None:
        """Trades exactly at best_bid or best_ask are NOT inside the spread."""
        t0 = time.time()
        ob = _make_orderbook_df(10, t0, best_bid=0.55, best_ask=0.57)
        # All trades at bid (0.55) or ask (0.57) -- none inside
        trades = _make_trades_df(
            t0,
            prices=[0.55, 0.57, 0.55, 0.57, 0.55, 0.57],
        )

        result = self.feature.compute(ob, trades, "test_market")
        assert result is not None
        assert result.values["wash_trade_ratio"] == 0.0
        assert result.values["inside_spread_count"] == 0
        assert result.values["total_trade_count"] == 6
        assert result.values["wash_volume_ratio"] == 0.0

    def test_trades_inside_spread_detected(self) -> None:
        """Trades strictly between bid and ask are flagged as wash."""
        t0 = time.time()
        ob = _make_orderbook_df(10, t0, best_bid=0.55, best_ask=0.57)
        # 3 trades inside (0.56), 2 at bid/ask
        trades = _make_trades_df(
            t0,
            prices=[0.56, 0.55, 0.56, 0.57, 0.56],
            sizes=[200.0, 100.0, 200.0, 100.0, 200.0],
        )

        result = self.feature.compute(ob, trades, "test_market")
        assert result is not None
        assert result.values["wash_trade_ratio"] == round(3 / 5, 6)
        assert result.values["inside_spread_count"] == 3
        assert result.values["total_trade_count"] == 5
        # wash volume = 3 * 200 = 600, total = 600 + 200 = 800
        assert result.values["wash_volume_ratio"] == round(600 / 800, 6)

    def test_no_trades_returns_none(self) -> None:
        """Returns None when there are no trades."""
        t0 = time.time()
        ob = _make_orderbook_df(10, t0)
        trades = pl.DataFrame({
            "timestamp": [],
            "exchange": [],
            "market_id": [],
            "asset_id": [],
            "outcome": [],
            "price": [],
            "size": [],
            "side": [],
            "usd_notional": [],
        })

        result = self.feature.compute(ob, trades, "test_market")
        assert result is None

    def test_zero_spread_no_wash(self) -> None:
        """When bid == ask (zero spread), no trade can be inside."""
        t0 = time.time()
        ob = _make_orderbook_df(10, t0, best_bid=0.56, best_ask=0.56)
        trades = _make_trades_df(
            t0,
            prices=[0.56, 0.56, 0.56],
        )

        result = self.feature.compute(ob, trades, "test_market")
        assert result is not None
        assert result.values["wash_trade_ratio"] == 0.0
        assert result.values["inside_spread_count"] == 0

    def test_no_orderbook_returns_none(self) -> None:
        """Returns None when orderbook is missing."""
        t0 = time.time()
        trades = _make_trades_df(t0, prices=[0.56])

        result = self.feature.compute(None, trades, "test_market")
        assert result is None

    def test_window_parameter(self) -> None:
        """Only trades within the rolling window are evaluated."""
        t0 = time.time()
        ob = _make_orderbook_df(200, t0, best_bid=0.55, best_ask=0.57)
        # Trade at t0 (old, outside 10s window) and trades near t0+190
        prices = [0.56] + [0.55] * 9  # first is wash, rest are clean
        timestamps = [t0] + [t0 + 190 + i * 0.5 for i in range(9)]
        trades = pl.DataFrame({
            "timestamp": timestamps,
            "exchange": ["polymarket"] * 10,
            "market_id": ["test_market"] * 10,
            "asset_id": ["test_market"] * 10,
            "outcome": ["yes"] * 10,
            "price": prices,
            "size": [100.0] * 10,
            "side": ["buy"] * 10,
            "usd_notional": [None] * 10,
        })

        result = self.feature.compute(
            ob, trades, "test_market", parameters={"window_seconds": 10}
        )
        assert result is not None
        # The old wash trade at t0 should be outside the window
        assert result.values["wash_trade_ratio"] == 0.0
        assert result.values["inside_spread_count"] == 0
