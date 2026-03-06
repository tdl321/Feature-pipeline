"""Tests for WalletParquetStore — snapshot buffering and PnL history persistence."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from alpha_pipeline.tracking.models import (
    PnlSummary,
    TradedVolume,
    WalletPosition,
    WalletSnapshot,
)
from alpha_pipeline.tracking.storage import WalletParquetStore


def _make_snapshot(
    address: str = "0xTEST123",
    exchange: str = "limitless",
    positions: list[WalletPosition] | None = None,
) -> WalletSnapshot:
    return WalletSnapshot(
        address=address,
        exchange=exchange,
        polled_at=datetime.now(timezone.utc),
        positions=positions or [
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
        pnl_summary=PnlSummary(
            current_value=120.0,
            previous_value=100.0,
            percent_change=20.0,
        ),
        traded_volume=TradedVolume(total_volume_usd=500.0),
    )


class TestWriteSnapshot:
    def test_writes_parquet_on_flush(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path, flush_every=0)
        snap = _make_snapshot()

        store.write_snapshot(snap)
        store.flush()

        files = list(tmp_path.glob("snapshots_*.parquet"))
        assert len(files) == 1
        df = pl.read_parquet(files[0])
        assert len(df) == 1
        assert df["wallet"][0] == "0xTEST123"
        assert df["market_slug"][0] == "btc-100k"
        assert df["shares"][0] == 100.0

    def test_auto_flush_at_threshold(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path, flush_every=2)

        store.write_snapshot(_make_snapshot())
        assert list(tmp_path.glob("snapshots_*.parquet")) == []

        store.write_snapshot(_make_snapshot())
        files = list(tmp_path.glob("snapshots_*.parquet"))
        assert len(files) == 1

    def test_snapshot_without_positions(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path, flush_every=0)
        snap = _make_snapshot(positions=[])

        store.write_snapshot(snap)
        store.flush()

        files = list(tmp_path.glob("snapshots_*.parquet"))
        assert len(files) == 1
        df = pl.read_parquet(files[0])
        assert len(df) == 1
        assert df["market_slug"][0] == ""
        assert df["total_pnl_usd"][0] == 20.0  # 120 - 100

    def test_close_flushes_remaining(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path, flush_every=0)
        store.write_snapshot(_make_snapshot())
        store.close()

        files = list(tmp_path.glob("snapshots_*.parquet"))
        assert len(files) == 1

    def test_context_manager(self, tmp_path: Path) -> None:
        with WalletParquetStore(output_dir=tmp_path, flush_every=0) as store:
            store.write_snapshot(_make_snapshot())

        files = list(tmp_path.glob("snapshots_*.parquet"))
        assert len(files) == 1

    def test_snapshot_columns(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path, flush_every=0)
        store.write_snapshot(_make_snapshot())
        store.flush()

        df = pl.read_parquet(list(tmp_path.glob("snapshots_*.parquet"))[0])
        expected_cols = {
            "polled_at", "wallet", "exchange", "market_slug", "outcome",
            "shares", "cost_usd", "fill_price", "market_value_usd",
            "realized_pnl_usd", "unrealized_pnl_usd", "total_pnl_usd",
            "pnl_percent_change", "traded_volume_usd",
        }
        assert set(df.columns) == expected_cols


class TestWritePnlHistory:
    def test_writes_parquet(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path)
        points = [
            {"timestamp": datetime(2025, 1, 1, tzinfo=timezone.utc), "value": 100.0},
            {"timestamp": datetime(2025, 1, 2, tzinfo=timezone.utc), "value": 110.0},
            {"timestamp": datetime(2025, 1, 3, tzinfo=timezone.utc), "value": 105.0},
        ]

        store.write_pnl_history("0xABCDEF123", "limitless", points)

        files = list(tmp_path.glob("pnl_history_*.parquet"))
        assert len(files) == 1
        assert "0xABCD" in files[0].name
        df = pl.read_parquet(files[0])
        assert len(df) == 3
        assert df["wallet"][0] == "0xABCDEF123"
        assert df["value"][0] == 100.0

    def test_empty_points_no_file(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path)
        store.write_pnl_history("0xABC", "limitless", [])

        files = list(tmp_path.glob("pnl_history_*.parquet"))
        assert len(files) == 0

    def test_pnl_history_columns(self, tmp_path: Path) -> None:
        store = WalletParquetStore(output_dir=tmp_path)
        points = [
            {"timestamp": datetime(2025, 6, 1, tzinfo=timezone.utc), "value": 50.0},
        ]
        store.write_pnl_history("0xDEADBEEF", "limitless", points)

        df = pl.read_parquet(list(tmp_path.glob("pnl_history_*.parquet"))[0])
        assert set(df.columns) == {"timestamp", "wallet", "exchange", "value"}
