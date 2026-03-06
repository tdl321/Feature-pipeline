"""Tests for WalletTracker polling loop."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from prometheus_client import CollectorRegistry

from alpha_pipeline.tracking.collector import WalletMetricsCollector
from alpha_pipeline.tracking.models import (
    PnlSummary,
    TradedVolume,
    WalletPosition,
    WalletSnapshot,
)
from alpha_pipeline.tracking.tracker import WalletTracker


def _make_snapshot(address: str = "0xABC") -> WalletSnapshot:
    return WalletSnapshot(
        address=address,
        exchange="mock",
        polled_at=datetime.now(timezone.utc),
        positions=[WalletPosition(market_slug="test-market", outcome="Yes", shares=10)],
        pnl_summary=PnlSummary(current_value=100.0, percent_change=5.0),
        traded_volume=TradedVolume(total_volume_usd=1000.0),
    )


class _MockFetcher:
    """Mock fetcher that returns a canned snapshot."""

    def __init__(self, fail: bool = False) -> None:
        self._fail = fail
        self.close_called = False

    @property
    def exchange_id(self) -> str:
        return "mock"

    async def fetch_positions(self, address: str) -> list[WalletPosition]:
        if self._fail:
            raise ConnectionError("mock error")
        return [WalletPosition(market_slug="m1", outcome="Yes", shares=10)]

    async def fetch_pnl_summary(
        self, address: str, timeframe: str = "7d",
    ) -> PnlSummary | None:
        if self._fail:
            raise ConnectionError("mock error")
        return PnlSummary(current_value=100.0, percent_change=5.0)

    async def fetch_traded_volume(self, address: str) -> TradedVolume | None:
        if self._fail:
            raise ConnectionError("mock error")
        return TradedVolume(total_volume_usd=1000.0)

    async def close(self) -> None:
        self.close_called = True


class TestWalletTracker:
    @pytest.mark.asyncio
    async def test_polls_and_observes(self) -> None:
        registry = CollectorRegistry()
        collector = WalletMetricsCollector(port=0, registry=registry)
        fetcher = _MockFetcher()

        tracker = WalletTracker(
            wallets=["0xABC"],
            fetchers=[fetcher],
            collector=collector,
            poll_interval=0.1,
        )

        # Run for a brief period then stop
        async def _run_briefly() -> None:
            task = asyncio.create_task(tracker.run())
            await asyncio.sleep(0.3)
            await tracker.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await _run_briefly()

        # Verify metrics were set
        labels = {"wallet": "0xABC", "exchange": "mock"}
        assert collector.position_count.labels(**labels)._value.get() >= 1

    @pytest.mark.asyncio
    async def test_error_handling(self) -> None:
        registry = CollectorRegistry()
        collector = WalletMetricsCollector(port=0, registry=registry)
        fetcher = _MockFetcher(fail=True)

        tracker = WalletTracker(
            wallets=["0xBAD"],
            fetchers=[fetcher],
            collector=collector,
            poll_interval=0.1,
        )

        async def _run_briefly() -> None:
            task = asyncio.create_task(tracker.run())
            await asyncio.sleep(0.25)
            await tracker.stop()
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await _run_briefly()

        # Error counter should have been incremented
        labels = {"wallet": "0xBAD", "exchange": "mock"}
        assert collector.poll_errors_total.labels(**labels)._value.get() >= 1

    @pytest.mark.asyncio
    async def test_stop_closes_fetchers(self) -> None:
        registry = CollectorRegistry()
        collector = WalletMetricsCollector(port=0, registry=registry)
        fetcher = _MockFetcher()

        tracker = WalletTracker(
            wallets=["0xABC"],
            fetchers=[fetcher],
            collector=collector,
        )

        await tracker.stop()
        assert fetcher.close_called
