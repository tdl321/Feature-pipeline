"""Tests for FeatureRunner."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import polars as pl
import pytest

from alpha_pipeline.data.buffer import TimeSeriesBuffer
from alpha_pipeline.features.base import Feature
from alpha_pipeline.features.registry import FeatureRegistry
from alpha_pipeline.features.runner import FeatureRunner
from alpha_pipeline.schemas.enums import DataEventType
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureSpec


class _MockFeature(Feature):
    def spec(self) -> FeatureSpec:
        return FeatureSpec(
            name="mock.feature",
            category="mock",
            version="1.0",
            requires_orderbook=True,
            requires_trades=False,
            requires_cross_exchange=False,
            min_history_seconds=0,
            output_fields=("test_value",),
        )

    def compute(
        self,
        orderbook_df: pl.DataFrame | None,
        trades_df: pl.DataFrame | None,
        market_id: str,
        parameters: dict[str, Any] | None = None,
    ) -> FeatureOutput | None:
        if orderbook_df is None or orderbook_df.is_empty():
            return None
        return FeatureOutput(
            feature_name="mock.feature",
            timestamp=datetime.now(tz=timezone.utc),
            market_id=market_id,
            values={"test_value": 42.0},
        )


@pytest.mark.asyncio
async def test_runner_processes_event() -> None:
    """End-to-end: event -> buffer read -> feature compute -> output."""
    registry = FeatureRegistry()
    registry.register(_MockFeature())

    market_id = "test_market"
    ob_buf = TimeSeriesBuffer(max_rows=100)
    t0 = time.time()
    for i in range(5):
        ob_buf.append({
            "timestamp": t0 + i,
            "exchange": "polymarket",
            "market_id": market_id,
            "best_bid": 0.55,
            "best_ask": 0.57,
            "mid_price": 0.56,
            "spread": 0.02,
            "bid_depth": 5000.0,
            "ask_depth": 4800.0,
            "n_bid_levels": 5,
            "n_ask_levels": 5,
            "best_bid_size": 1000.0,
            "best_ask_size": 800.0,
        })

    ob_buffers = {market_id: ob_buf}
    trade_buffers: dict[str, TimeSeriesBuffer] = {}
    event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    runner = FeatureRunner(
        registry=registry,
        orderbook_buffers=ob_buffers,
        trade_buffers=trade_buffers,
        event_queue=event_queue,
    )

    # Send an event
    await event_queue.put({
        "type": DataEventType.ORDERBOOK_SNAPSHOT,
        "exchange": "polymarket",
        "market_id": market_id,
        "timestamp": time.time(),
    })

    # Run for a brief period
    task = asyncio.create_task(runner.run())
    await asyncio.sleep(0.5)
    runner.stop()
    task.cancel()

    # Check output
    assert not runner.output_queue.empty()
    vector = await runner.output_queue.get()
    assert vector.market_id == market_id
    assert len(vector.features) == 1
    assert vector.features[0].values["test_value"] == 42.0
