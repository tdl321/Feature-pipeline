"""Tests for the Limitless wallet data fetcher."""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_pipeline.tracking.limitless_fetcher import LimitlessWalletFetcher

FIXTURES = Path(__file__).parent / "fixtures"


def _load_fixture(name: str) -> dict | list:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def fetcher() -> LimitlessWalletFetcher:
    return LimitlessWalletFetcher(base_url="https://mock.api")


class _FakeResponse:
    """Minimal mock for aiohttp.ClientResponse."""

    def __init__(self, data: object, status: int = 200) -> None:
        self._data = data
        self.status = status

    async def json(self) -> object:
        return self._data

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise Exception(f"HTTP {self.status}")

    async def __aenter__(self) -> _FakeResponse:
        return self

    async def __aexit__(self, *args: object) -> None:
        pass


class _FakeSession:
    """Minimal mock for aiohttp.ClientSession."""

    def __init__(self, responses: dict[str, _FakeResponse]) -> None:
        self._responses = responses
        self.closed = False

    def get(self, url: str, **kwargs: object) -> _FakeResponse:
        # Match by URL suffix
        for key, resp in self._responses.items():
            if key in url:
                return resp
        return _FakeResponse({}, status=404)

    async def close(self) -> None:
        self.closed = True


class TestFetchPositions:
    @pytest.mark.asyncio
    async def test_parses_clob_skips_amm(self, fetcher: LimitlessWalletFetcher) -> None:
        data = _load_fixture("limitless_positions.json")
        session = _FakeSession({"/positions": _FakeResponse(data)})
        fetcher._session = session  # type: ignore[assignment]

        positions = await fetcher.fetch_positions("0xABC")

        # 3 items in fixture, 1 AMM should be skipped
        assert len(positions) == 2
        assert positions[0].market_slug == "will-btc-reach-100k"
        assert positions[0].outcome == "Yes"
        assert positions[0].shares == 150.5
        assert positions[0].cost_usd == pytest.approx(75.25)
        assert positions[0].unrealized_pnl_usd == pytest.approx(15.05)

        assert positions[1].market_slug == "us-election-2028"
        assert positions[1].outcome == "No"
        assert positions[1].unrealized_pnl_usd == pytest.approx(-10.0)

    @pytest.mark.asyncio
    async def test_404_returns_empty(self, fetcher: LimitlessWalletFetcher) -> None:
        session = _FakeSession({"/positions": _FakeResponse([], status=404)})
        fetcher._session = session  # type: ignore[assignment]

        positions = await fetcher.fetch_positions("0xDEAD")
        assert positions == []


class TestFetchPnlSummary:
    @pytest.mark.asyncio
    async def test_parses_pnl(self, fetcher: LimitlessWalletFetcher) -> None:
        data = _load_fixture("limitless_pnl_chart.json")
        session = _FakeSession({"/pnl-chart": _FakeResponse(data)})
        fetcher._session = session  # type: ignore[assignment]

        pnl = await fetcher.fetch_pnl_summary("0xABC", timeframe="7d")

        assert pnl is not None
        assert pnl.current_value == pytest.approx(120.35)
        assert pnl.previous_value == pytest.approx(100.0)
        assert pnl.percent_change == pytest.approx(20.35)

    @pytest.mark.asyncio
    async def test_404_returns_none(self, fetcher: LimitlessWalletFetcher) -> None:
        session = _FakeSession({"/pnl-chart": _FakeResponse({}, status=404)})
        fetcher._session = session  # type: ignore[assignment]

        pnl = await fetcher.fetch_pnl_summary("0xDEAD")
        assert pnl is None


class TestFetchTradedVolume:
    @pytest.mark.asyncio
    async def test_parses_volume(self, fetcher: LimitlessWalletFetcher) -> None:
        data = _load_fixture("limitless_traded_volume.json")
        session = _FakeSession({"/traded-volume": _FakeResponse(data)})
        fetcher._session = session  # type: ignore[assignment]

        vol = await fetcher.fetch_traded_volume("0xABC")

        assert vol is not None
        assert vol.total_volume_usd == pytest.approx(500.0)
        assert "last7d" in vol.raw_fields

    @pytest.mark.asyncio
    async def test_404_returns_none(self, fetcher: LimitlessWalletFetcher) -> None:
        session = _FakeSession({"/traded-volume": _FakeResponse({}, status=404)})
        fetcher._session = session  # type: ignore[assignment]

        vol = await fetcher.fetch_traded_volume("0xDEAD")
        assert vol is None


class TestFetchSnapshot:
    @pytest.mark.asyncio
    async def test_concurrent_fetch(self, fetcher: LimitlessWalletFetcher) -> None:
        positions_data = _load_fixture("limitless_positions.json")
        pnl_data = _load_fixture("limitless_pnl_chart.json")
        volume_data = _load_fixture("limitless_traded_volume.json")

        session = _FakeSession({
            "/positions": _FakeResponse(positions_data),
            "/pnl-chart": _FakeResponse(pnl_data),
            "/traded-volume": _FakeResponse(volume_data),
        })
        fetcher._session = session  # type: ignore[assignment]

        snap = await fetcher.fetch_snapshot("0xABC")

        assert snap.address == "0xABC"
        assert snap.exchange == "limitless"
        assert len(snap.positions) == 2  # AMM skipped
        assert snap.pnl_summary is not None
        assert snap.traded_volume is not None


class TestClose:
    @pytest.mark.asyncio
    async def test_closes_session(self, fetcher: LimitlessWalletFetcher) -> None:
        session = _FakeSession({})
        fetcher._session = session  # type: ignore[assignment]

        await fetcher.close()
        assert session.closed
        assert fetcher._session is None
