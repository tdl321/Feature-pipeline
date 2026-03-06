"""Tests for WalletMetricsCollector with isolated registry."""
from __future__ import annotations

from datetime import datetime, timezone

from prometheus_client import CollectorRegistry

from alpha_pipeline.tracking.collector import WalletMetricsCollector
from alpha_pipeline.tracking.models import (
    PnlSummary,
    TradedVolume,
    WalletPosition,
    WalletSnapshot,
)


def _make_collector() -> tuple[WalletMetricsCollector, CollectorRegistry]:
    registry = CollectorRegistry()
    collector = WalletMetricsCollector(port=0, registry=registry)
    return collector, registry


def _sample_snapshot() -> WalletSnapshot:
    return WalletSnapshot(
        address="0xTEST",
        exchange="limitless",
        polled_at=datetime.now(timezone.utc),
        positions=[
            WalletPosition(
                market_slug="btc-100k",
                outcome="Yes",
                shares=100.0,
                cost_usd=50.0,
                fill_price=0.5,
                market_value_usd=60.0,
                realized_pnl_usd=5.0,
                unrealized_pnl_usd=10.0,
            ),
        ],
        pnl_summary=PnlSummary(
            current_value=120.0,
            previous_value=100.0,
            percent_change=20.0,
        ),
        traded_volume=TradedVolume(total_volume_usd=500.0),
    )


class TestObserveSnapshot:
    def test_sets_position_gauges(self) -> None:
        collector, registry = _make_collector()
        snap = _sample_snapshot()

        collector.observe_snapshot(snap)

        labels = {
            "wallet": "0xTEST",
            "exchange": "limitless",
            "market": "btc-100k",
            "outcome": "Yes",
        }
        assert collector.position_shares.labels(**labels)._value.get() == 100.0
        assert collector.position_cost_usd.labels(**labels)._value.get() == 50.0
        assert collector.position_market_value_usd.labels(**labels)._value.get() == 60.0
        assert collector.position_realized_pnl_usd.labels(**labels)._value.get() == 5.0
        assert collector.position_unrealized_pnl_usd.labels(**labels)._value.get() == 10.0
        assert collector.position_fill_price.labels(**labels)._value.get() == 0.5

    def test_sets_pnl_gauges(self) -> None:
        collector, _ = _make_collector()
        snap = _sample_snapshot()

        collector.observe_snapshot(snap)

        labels = {"wallet": "0xTEST", "exchange": "limitless"}
        assert collector.total_pnl_usd.labels(**labels)._value.get() == 120.0
        assert collector.pnl_percent_change.labels(**labels)._value.get() == 20.0

    def test_sets_volume_gauge(self) -> None:
        collector, _ = _make_collector()
        snap = _sample_snapshot()

        collector.observe_snapshot(snap)

        labels = {"wallet": "0xTEST", "exchange": "limitless"}
        assert collector.traded_volume_usd.labels(**labels)._value.get() == 500.0

    def test_sets_position_count(self) -> None:
        collector, _ = _make_collector()
        snap = _sample_snapshot()

        collector.observe_snapshot(snap)

        labels = {"wallet": "0xTEST", "exchange": "limitless"}
        assert collector.position_count.labels(**labels)._value.get() == 1

    def test_increments_poll_total(self) -> None:
        collector, _ = _make_collector()
        snap = _sample_snapshot()

        collector.observe_snapshot(snap)
        collector.observe_snapshot(snap)

        labels = {"wallet": "0xTEST", "exchange": "limitless"}
        assert collector.poll_total.labels(**labels)._value.get() == 2

    def test_no_pnl_or_volume(self) -> None:
        collector, _ = _make_collector()
        snap = WalletSnapshot(
            address="0xMIN",
            exchange="limitless",
            polled_at=datetime.now(timezone.utc),
        )

        collector.observe_snapshot(snap)

        labels = {"wallet": "0xMIN", "exchange": "limitless"}
        assert collector.position_count.labels(**labels)._value.get() == 0


class TestObserveError:
    def test_increments_error_counter(self) -> None:
        collector, _ = _make_collector()

        collector.observe_error("0xBAD", "limitless")
        collector.observe_error("0xBAD", "limitless")

        labels = {"wallet": "0xBAD", "exchange": "limitless"}
        assert collector.poll_errors_total.labels(**labels)._value.get() == 2
