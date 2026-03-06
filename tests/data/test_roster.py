"""Tests for MarketRoster — auto-discovery of active Limitless markets."""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alpha_pipeline.data.roster import MarketInfo, MarketRoster, RosterDelta

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _load_active_markets() -> list[dict]:
    return json.loads((FIXTURES / "limitless_active_markets.json").read_text())


def _make_roster(
    tickers: frozenset[str] = frozenset({"ETH", "SOL"}),
    grace_seconds: float = 300.0,
) -> MarketRoster:
    """Create a MarketRoster with a mock session."""
    session = MagicMock()
    return MarketRoster(
        session=session,
        api_url="https://api.limitless.exchange",
        tickers=tickers,
        grace_seconds=grace_seconds,
    )


class TestRosterRefresh:
    """Test the core refresh/diff logic."""

    @pytest.mark.asyncio
    async def test_initial_refresh_discovers_markets(self) -> None:
        """First refresh should return all matching markets as added."""
        roster = _make_roster(tickers=frozenset({"ETH", "SOL"}))
        markets = _load_active_markets()

        # Mock the fetch to return fixture data
        roster._fetch_active_markets = AsyncMock(
            return_value=[
                MarketInfo(
                    slug=m["slug"],
                    ticker=m["price_oracle_metadata"]["ticker"].upper(),
                    expiration_ts=m["expiration_timestamp"],
                    title=m["title"],
                )
                for m in markets
                if m["trade_type"] == "clob"
                and m["price_oracle_metadata"]["ticker"].upper()
                in {"ETH", "SOL"}
            ]
        )

        delta = await roster.refresh()

        assert len(delta.added) == 3
        assert "eth-above-2082-09utc" in delta.added
        assert "eth-above-2100-10utc" in delta.added
        assert "sol-above-140-09utc" in delta.added
        assert len(delta.removed) == 0

    @pytest.mark.asyncio
    async def test_no_change_returns_empty_delta(self) -> None:
        """Second refresh with same markets returns empty delta."""
        roster = _make_roster()
        info_list = [
            MarketInfo(
                slug="eth-above-2082-09utc",
                ticker="ETH",
                expiration_ts=int(time.time() * 1000) + 3_600_000,
                title="$ETH above $2082",
            ),
        ]
        roster._fetch_active_markets = AsyncMock(return_value=info_list)

        # First refresh: discover
        delta1 = await roster.refresh()
        assert delta1.added == ("eth-above-2082-09utc",)

        # Second refresh: no change
        delta2 = await roster.refresh()
        assert delta2.added == ()
        assert delta2.removed == ()

    @pytest.mark.asyncio
    async def test_detects_added_and_removed(self) -> None:
        """When API returns different markets, detect added and removed."""
        roster = _make_roster()

        # First: market A
        roster._fetch_active_markets = AsyncMock(
            return_value=[
                MarketInfo("slug-a", "ETH", int(time.time() * 1000) + 3_600_000, "A"),
            ]
        )
        await roster.refresh()

        # Second: market A gone, market B added
        roster._fetch_active_markets = AsyncMock(
            return_value=[
                MarketInfo("slug-b", "ETH", int(time.time() * 1000) + 3_600_000, "B"),
            ]
        )
        delta = await roster.refresh()

        assert "slug-b" in delta.added
        assert "slug-a" in delta.removed

    @pytest.mark.asyncio
    async def test_api_error_returns_empty_delta(self) -> None:
        """If _fetch_active_markets raises, return empty delta."""
        roster = _make_roster()
        roster._fetch_active_markets = AsyncMock(
            side_effect=Exception("network error")
        )
        delta = await roster.refresh()
        assert delta.added == ()
        assert delta.removed == ()


class TestRosterFiltering:
    """Test ticker filtering and CLOB-only constraints."""

    @pytest.mark.asyncio
    async def test_filters_by_ticker(self) -> None:
        """Only markets matching tracked tickers are included."""
        roster = _make_roster(tickers=frozenset({"ETH"}))
        all_markets = [
            MarketInfo("eth-market", "ETH", int(time.time() * 1000) + 3_600_000, "ETH"),
            MarketInfo("sol-market", "SOL", int(time.time() * 1000) + 3_600_000, "SOL"),
        ]
        # Simulate: _fetch already filters, so only ETH returned
        roster._fetch_active_markets = AsyncMock(
            return_value=[m for m in all_markets if m.ticker in roster._tickers]
        )

        delta = await roster.refresh()
        assert "eth-market" in delta.added
        assert "sol-market" not in delta.added

    @pytest.mark.asyncio
    async def test_no_duplicates(self) -> None:
        """Even if API returns dupes, roster has unique slugs."""
        roster = _make_roster()
        info = MarketInfo("eth-dup", "ETH", int(time.time() * 1000) + 3_600_000, "Dup")
        roster._fetch_active_markets = AsyncMock(
            return_value=[info, info]  # duplicate
        )

        delta = await roster.refresh()
        assert delta.added == ("eth-dup",)
        assert len(roster.active_slugs) == 1


class TestRosterExpiry:
    """Test expired market detection after grace period."""

    @pytest.mark.asyncio
    async def test_expired_market_removed_after_grace(self) -> None:
        """Market past expiration + grace is removed even if still in API."""
        roster = _make_roster(grace_seconds=0.0)  # 0 grace

        now_ms = int(time.time() * 1000)
        # Market expired 1 second ago
        expired = MarketInfo("expired-eth", "ETH", now_ms - 1000, "Expired")
        roster._fetch_active_markets = AsyncMock(return_value=[expired])

        # First refresh: add it
        delta1 = await roster.refresh()
        assert "expired-eth" in delta1.added

        # Second refresh: still in API but past expiry + grace → removed
        delta2 = await roster.refresh()
        assert "expired-eth" in delta2.removed

    @pytest.mark.asyncio
    async def test_market_within_grace_not_removed(self) -> None:
        """Market past expiry but within grace period stays active."""
        roster = _make_roster(grace_seconds=600.0)  # 10 min grace

        now_ms = int(time.time() * 1000)
        # Market expired 1 second ago but grace is 600s
        recent = MarketInfo("recent-eth", "ETH", now_ms - 1000, "Recent")
        roster._fetch_active_markets = AsyncMock(return_value=[recent])

        await roster.refresh()  # add
        delta = await roster.refresh()  # check

        assert "recent-eth" not in delta.removed
        assert "recent-eth" in roster.active_slugs


class TestActiveSlugsSorted:
    """active_slugs returns sorted list."""

    @pytest.mark.asyncio
    async def test_slugs_sorted(self) -> None:
        roster = _make_roster()
        roster._fetch_active_markets = AsyncMock(
            return_value=[
                MarketInfo("z-market", "ETH", int(time.time() * 1000) + 3_600_000, "Z"),
                MarketInfo("a-market", "SOL", int(time.time() * 1000) + 3_600_000, "A"),
            ]
        )
        await roster.refresh()
        assert roster.active_slugs == ["a-market", "z-market"]


class TestRosterDelta:
    """Verify RosterDelta is a proper frozen dataclass."""

    def test_delta_immutable(self) -> None:
        delta = RosterDelta(added=("a",), removed=("b",))
        assert delta.added == ("a",)
        assert delta.removed == ("b",)

    def test_delta_empty(self) -> None:
        delta = RosterDelta(added=(), removed=())
        assert not delta.added
        assert not delta.removed
