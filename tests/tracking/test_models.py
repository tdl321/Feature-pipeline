"""Tests for wallet tracking data models."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from alpha_pipeline.tracking.models import (
    PnlSummary,
    TradedVolume,
    WalletPosition,
    WalletSnapshot,
    raw_to_usd,
)


class TestRawToUsd:
    def test_string_integer(self) -> None:
        assert raw_to_usd("1500000") == 1.5

    def test_string_large(self) -> None:
        assert raw_to_usd("75250000") == 75.25

    def test_none(self) -> None:
        assert raw_to_usd(None) == 0.0

    def test_empty_string(self) -> None:
        assert raw_to_usd("") == 0.0

    def test_whitespace(self) -> None:
        assert raw_to_usd("  ") == 0.0

    def test_zero(self) -> None:
        assert raw_to_usd("0") == 0.0

    def test_float_passthrough(self) -> None:
        assert raw_to_usd(3.14) == 3.14

    def test_int_divides(self) -> None:
        assert raw_to_usd(2_000_000) == 2.0

    def test_negative_string(self) -> None:
        assert raw_to_usd("-10000000") == -10.0

    def test_float_string_fallback(self) -> None:
        assert raw_to_usd("1.5") == 1.5

    def test_unparseable(self) -> None:
        assert raw_to_usd("not_a_number") == 0.0


class TestWalletPosition:
    def test_construction(self) -> None:
        pos = WalletPosition(
            market_slug="btc-100k",
            title="BTC to 100k",
            outcome="Yes",
            shares=100.0,
            cost_usd=50.0,
            fill_price=0.5,
            market_value_usd=60.0,
            realized_pnl_usd=5.0,
            unrealized_pnl_usd=10.0,
        )
        assert pos.market_slug == "btc-100k"
        assert pos.shares == 100.0

    def test_frozen(self) -> None:
        pos = WalletPosition(market_slug="test")
        with pytest.raises(Exception):
            pos.shares = 999  # type: ignore[misc]

    def test_defaults(self) -> None:
        pos = WalletPosition(market_slug="x")
        assert pos.title == ""
        assert pos.shares == 0.0
        assert pos.cost_usd == 0.0


class TestPnlSummary:
    def test_construction(self) -> None:
        pnl = PnlSummary(
            current_value=120.0,
            previous_value=100.0,
            percent_change=20.0,
        )
        assert pnl.timeframe == "7d"
        assert pnl.percent_change == 20.0


class TestTradedVolume:
    def test_construction(self) -> None:
        vol = TradedVolume(total_volume_usd=500.0, raw_fields={"a": 1})
        assert vol.total_volume_usd == 500.0
        assert vol.raw_fields == {"a": 1}


class TestWalletSnapshot:
    def test_construction(self) -> None:
        now = datetime.now(timezone.utc)
        snap = WalletSnapshot(
            address="0xABC",
            exchange="limitless",
            polled_at=now,
            positions=[WalletPosition(market_slug="m1")],
            pnl_summary=PnlSummary(current_value=10.0),
            traded_volume=TradedVolume(total_volume_usd=100.0),
        )
        assert snap.address == "0xABC"
        assert len(snap.positions) == 1
        assert snap.pnl_summary is not None
        assert snap.traded_volume is not None

    def test_defaults(self) -> None:
        now = datetime.now(timezone.utc)
        snap = WalletSnapshot(
            address="0x1", exchange="test", polled_at=now,
        )
        assert snap.positions == []
        assert snap.pnl_summary is None
        assert snap.traded_volume is None
