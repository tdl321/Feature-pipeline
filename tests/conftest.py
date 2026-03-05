"""Shared test fixtures and factories."""
from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
import polars as pl

from alpha_pipeline.schemas.enums import ExchangeId, OutcomeType, Side
from alpha_pipeline.schemas.orderbook import NormalizedOrderbook, OrderbookLevel
from alpha_pipeline.schemas.trade import NormalizedTrade
from alpha_pipeline.data.buffer import TimeSeriesBuffer


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------

def make_orderbook(
    exchange: ExchangeId = ExchangeId.POLYMARKET,
    market_id: str = "test_market",
    asset_id: str = "test_asset",
    outcome: OutcomeType = OutcomeType.YES,
    bids: list[tuple[float, float]] | None = None,
    asks: list[tuple[float, float]] | None = None,
) -> NormalizedOrderbook:
    if bids is None:
        bids = [(0.55, 1000.0), (0.54, 2000.0), (0.53, 500.0)]
    if asks is None:
        asks = [(0.57, 800.0), (0.58, 1500.0), (0.60, 2500.0)]

    return NormalizedOrderbook.from_raw(
        exchange=exchange,
        market_id=market_id,
        asset_id=asset_id,
        outcome=outcome,
        bids_raw=bids,
        asks_raw=asks,
    )


def make_trade(
    exchange: ExchangeId = ExchangeId.POLYMARKET,
    market_id: str = "test_market",
    asset_id: str = "test_asset",
    outcome: OutcomeType = OutcomeType.YES,
    price: float = 0.56,
    size: float = 100.0,
    side: Side = Side.BUY,
) -> NormalizedTrade:
    return NormalizedTrade(
        exchange=exchange,
        market_id=market_id,
        asset_id=asset_id,
        outcome=outcome,
        price=price,
        size=size,
        side=side,
        local_timestamp=datetime.now(tz=timezone.utc),
    )


def make_buffer(
    records: list[dict] | None = None,
    max_rows: int = 10_000,
) -> TimeSeriesBuffer:
    buf = TimeSeriesBuffer(max_rows=max_rows)
    if records:
        buf.append_many(records)
    return buf


def make_orderbook_df(
    n: int = 10,
    base_time: float | None = None,
    exchange: str = "polymarket",
    market_id: str = "test_market",
    best_bid: float = 0.55,
    best_ask: float = 0.57,
) -> pl.DataFrame:
    """Create a synthetic orderbook DataFrame for feature testing."""
    t0 = base_time or time.time()
    mid = (best_bid + best_ask) / 2.0
    spread = best_ask - best_bid
    return pl.DataFrame({
        "timestamp": [t0 + i for i in range(n)],
        "exchange": [exchange] * n,
        "market_id": [market_id] * n,
        "asset_id": [market_id] * n,
        "outcome": ["yes"] * n,
        "best_bid": [best_bid + i * 0.001 for i in range(n)],
        "best_ask": [best_ask + i * 0.001 for i in range(n)],
        "mid_price": [mid + i * 0.001 for i in range(n)],
        "spread": [spread] * n,
        "bid_depth": [5000.0] * n,
        "ask_depth": [4800.0] * n,
        "n_bid_levels": [5] * n,
        "n_ask_levels": [5] * n,
        "best_bid_size": [1000.0 + i * 50 for i in range(n)],
        "best_ask_size": [800.0 + i * 30 for i in range(n)],
    })


def make_trades_df(
    n: int = 20,
    base_time: float | None = None,
    market_id: str = "test_market",
) -> pl.DataFrame:
    """Create a synthetic trades DataFrame for feature testing."""
    t0 = base_time or time.time()
    return pl.DataFrame({
        "timestamp": [t0 + i * 0.5 for i in range(n)],
        "exchange": ["polymarket"] * n,
        "market_id": [market_id] * n,
        "asset_id": [market_id] * n,
        "outcome": ["yes"] * n,
        "price": [0.55 + (i % 5) * 0.01 for i in range(n)],
        "size": [100.0 + i * 10 for i in range(n)],
        "side": ["buy" if i % 3 != 0 else "sell" for i in range(n)],
        "usd_notional": [None] * n,
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_orderbook() -> NormalizedOrderbook:
    return make_orderbook()


@pytest.fixture
def sample_trade() -> NormalizedTrade:
    return make_trade()


@pytest.fixture
def orderbook_buffer() -> TimeSeriesBuffer:
    t0 = time.time()
    records = [
        {
            "timestamp": t0 + i,
            "exchange": "polymarket",
            "market_id": "test_market",
            "best_bid": 0.55 + i * 0.001,
            "best_ask": 0.57 + i * 0.001,
            "mid_price": 0.56 + i * 0.001,
            "spread": 0.02,
            "best_bid_size": 1000.0,
            "best_ask_size": 800.0,
        }
        for i in range(50)
    ]
    return make_buffer(records)


@pytest.fixture
def trade_buffer() -> TimeSeriesBuffer:
    t0 = time.time()
    records = [
        {
            "timestamp": t0 + i * 0.5,
            "exchange": "polymarket",
            "market_id": "test_market",
            "price": 0.55 + (i % 5) * 0.01,
            "size": 100.0 + i * 10,
            "side": "buy" if i % 3 != 0 else "sell",
        }
        for i in range(100)
    ]
    return make_buffer(records)


@pytest.fixture
def orderbook_df() -> pl.DataFrame:
    return make_orderbook_df(n=50)


@pytest.fixture
def trades_df() -> pl.DataFrame:
    return make_trades_df(n=50)
