"""Tests for snapshot and PnL history loading from Parquet files."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from alpha_pipeline.tracking.loader import load_pnl_history, load_snapshots
from alpha_pipeline.tracking.models import (
    PnlSummary,
    TradedVolume,
    WalletPosition,
    WalletSnapshot,
)
from alpha_pipeline.tracking.storage import WalletParquetStore


def _make_snapshot(
    address: str = "0xTEST123",
    polled_at: datetime | None = None,
) -> WalletSnapshot:
    return WalletSnapshot(
        address=address,
        exchange="limitless",
        polled_at=polled_at or datetime.now(timezone.utc),
        positions=[
            WalletPosition(
                market_slug="btc-100k",
                outcome="YES",
                shares=100.0,
                cost_usd=50.0,
                fill_price=0.5,
                market_value_usd=60.0,
                realized_pnl_usd=5.0,
                unrealized_pnl_usd=10.0,
            ),
        ],
        pnl_summary=PnlSummary(current_value=120.0, previous_value=100.0, percent_change=20.0),
        traded_volume=TradedVolume(total_volume_usd=500.0),
    )


class TestLoadSnapshots:
    def test_round_trip(self, tmp_path: Path) -> None:
        with WalletParquetStore(output_dir=tmp_path, flush_every=0) as store:
            store.write_snapshot(_make_snapshot())

        df = load_snapshots(tmp_path)
        assert len(df) == 1
        assert df["wallet"][0] == "0xTEST123"

    def test_filter_by_wallet(self, tmp_path: Path) -> None:
        with WalletParquetStore(output_dir=tmp_path, flush_every=0) as store:
            store.write_snapshot(_make_snapshot(address="0xAAA"))
            store.write_snapshot(_make_snapshot(address="0xBBB"))

        df = load_snapshots(tmp_path, wallet="0xAAA")
        assert len(df) == 1
        assert df["wallet"][0] == "0xAAA"

    def test_filter_by_time_range(self, tmp_path: Path) -> None:
        t1 = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
        t2 = datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)
        t3 = datetime(2025, 1, 3, 12, 0, tzinfo=timezone.utc)

        with WalletParquetStore(output_dir=tmp_path, flush_every=0) as store:
            store.write_snapshot(_make_snapshot(polled_at=t1))
            store.write_snapshot(_make_snapshot(polled_at=t2))
            store.write_snapshot(_make_snapshot(polled_at=t3))

        df = load_snapshots(tmp_path, start=t2, end=t2)
        assert len(df) == 1

    def test_empty_directory(self, tmp_path: Path) -> None:
        df = load_snapshots(tmp_path)
        assert df.is_empty()

    def test_nonexistent_directory(self) -> None:
        df = load_snapshots("/nonexistent/path")
        assert df.is_empty()


class TestLoadPnlHistory:
    def test_round_trip(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path)
        points = [
            {"timestamp": datetime(2025, 1, 1, tzinfo=timezone.utc), "value": 100.0},
            {"timestamp": datetime(2025, 1, 2, tzinfo=timezone.utc), "value": 110.0},
        ]
        store.write_pnl_history("0xTEST123", "limitless", points)

        df = load_pnl_history(tmp_path)
        assert len(df) == 2
        assert df["value"][0] == 100.0
        assert df["value"][1] == 110.0

    def test_filter_by_wallet(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path)
        pts = [{"timestamp": datetime(2025, 6, 1, tzinfo=timezone.utc), "value": 50.0}]
        store.write_pnl_history("0xAAAAAAA", "limitless", pts)
        store.write_pnl_history("0xBBBBBBB", "limitless", pts)

        df = load_pnl_history(tmp_path, wallet="0xAAAAAAA")
        assert len(df) == 1
        assert df["wallet"][0] == "0xAAAAAAA"

    def test_filter_by_time_range(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path)
        points = [
            {"timestamp": datetime(2025, 1, 1, tzinfo=timezone.utc), "value": 10.0},
            {"timestamp": datetime(2025, 6, 1, tzinfo=timezone.utc), "value": 20.0},
            {"timestamp":2025, "value": 30.0},  # invalid, will be in separate file
        ]
        # Only write valid points
        valid = [p for p in points if isinstance(p["timestamp"], datetime)]
        store.write_pnl_history("0xTEST123", "limitless", valid)

        start = datetime(2025, 3, 1, tzinfo=timezone.utc)
        df = load_pnl_history(tmp_path, start=start)
        assert len(df) == 1
        assert df["value"][0] == 20.0

    def test_empty_directory(self, tmp_path: Path) -> None:
        df = load_pnl_history(tmp_path)
        assert df.is_empty()
