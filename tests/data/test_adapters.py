"""Tests for exchange adapter normalization."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from alpha_pipeline.data.adapters.limitless import LimitlessAdapter, _parse_timestamp
from alpha_pipeline.data.adapters.opinion import OpinionAdapter
from alpha_pipeline.data.adapters.polymarket import PolymarketAdapter
from alpha_pipeline.schemas.enums import ExchangeId, OutcomeType, Side
from alpha_pipeline.schemas.orderbook import NormalizedOrderbook


FIXTURES = Path(__file__).parent.parent / "fixtures"


class TestPolymarketNormalization:
    def test_normalize_book_fixture(self) -> None:
        raw = json.loads((FIXTURES / "polymarket_book.json").read_text())

        bids_raw = [(float(b["price"]), float(b["size"])) for b in raw["bids"]]
        asks_raw = [(float(a["price"]), float(a["size"])) for a in raw["asks"]]

        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.POLYMARKET,
            market_id=raw["market"],
            asset_id=raw["asset_id"],
            outcome=OutcomeType.YES,
            bids_raw=bids_raw,
            asks_raw=asks_raw,
        )

        assert ob.exchange == ExchangeId.POLYMARKET
        assert ob.market_id == "0x1234567890abcdef"
        assert ob.best_bid == 0.55
        assert ob.best_ask == 0.57
        assert ob.mid_price == pytest.approx(0.56)
        assert ob.spread == pytest.approx(0.02)

        # Verify ordering
        for i in range(1, len(ob.bids)):
            assert ob.bids[i].price <= ob.bids[i - 1].price
        for i in range(1, len(ob.asks)):
            assert ob.asks[i].price >= ob.asks[i - 1].price


class TestOpinionNormalization:
    def test_normalize_opinion_fixture(self) -> None:
        raw = json.loads((FIXTURES / "opinion_orderbook.json").read_text())

        bids_raw = [(float(b["price"]), float(b["size"])) for b in raw["bids"]]
        asks_raw = [(float(a["price"]), float(a["size"])) for a in raw["asks"]]

        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.OPINION,
            market_id=raw["market_id"],
            asset_id=raw["market_id"],
            outcome=OutcomeType.YES,
            bids_raw=bids_raw,
            asks_raw=asks_raw,
        )

        assert ob.exchange == ExchangeId.OPINION
        assert ob.best_bid == 0.52
        assert ob.best_ask == 0.58
        assert ob.spread == pytest.approx(0.06)


class TestSchemaValidation:
    def test_price_bounds(self) -> None:
        with pytest.raises(Exception):
            NormalizedOrderbook.from_raw(
                exchange=ExchangeId.POLYMARKET,
                market_id="m",
                asset_id="a",
                outcome=OutcomeType.YES,
                bids_raw=[(1.5, 100.0)],  # price > 1
                asks_raw=[],
            )

    def test_frozen_model(self) -> None:
        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.POLYMARKET,
            market_id="m",
            asset_id="a",
            outcome=OutcomeType.YES,
            bids_raw=[(0.5, 100.0)],
            asks_raw=[(0.6, 100.0)],
        )
        with pytest.raises(Exception):
            ob.market_id = "changed"  # type: ignore[misc]

    def test_empty_book(self) -> None:
        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.POLYMARKET,
            market_id="m",
            asset_id="a",
            outcome=OutcomeType.YES,
            bids_raw=[],
            asks_raw=[],
        )
        assert ob.best_bid is None
        assert ob.best_ask is None
        assert ob.mid_price is None
        assert ob.spread is None

    def test_json_roundtrip(self) -> None:
        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.POLYMARKET,
            market_id="m",
            asset_id="a",
            outcome=OutcomeType.YES,
            bids_raw=[(0.5, 100.0)],
            asks_raw=[(0.6, 200.0)],
        )
        data = ob.to_json_bytes()
        restored = NormalizedOrderbook.from_json_bytes(data)
        assert restored.market_id == ob.market_id
        assert restored.best_bid == ob.best_bid
        assert restored.best_ask == ob.best_ask


def _make_adapter(orderbook_depth_levels: int = 10) -> PolymarketAdapter:
    settings = MagicMock()
    settings.orderbook_depth_levels = orderbook_depth_levels
    return PolymarketAdapter(settings)


class TestPolymarketIncrementalBook:
    """Tests for incremental book state via _handle_book and _handle_price_change."""

    def test_full_snapshot_sets_book_state(self) -> None:
        adapter = _make_adapter()
        book_msg = {
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "1709654400000",
            "bids": [
                {"price": "0.50", "size": "1000"},
                {"price": "0.49", "size": "2000"},
            ],
            "asks": [
                {"price": "0.55", "size": "500"},
            ],
        }
        asyncio.run(adapter._handle_book(book_msg))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.50
        assert ob.best_ask == 0.55
        assert len(ob.bids) == 2
        assert len(ob.asks) == 1

    def test_price_change_updates_existing_level(self) -> None:
        adapter = _make_adapter()
        # Seed with snapshot
        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "1000",
            "bids": [{"price": "0.50", "size": "1000"}],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()  # drain snapshot

        # Delta: update bid size at 0.50
        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "asset1", "price": "0.50", "side": "BUY", "size": "2000"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.50
        assert ob.bids[0].size == 2000.0
        # Ask side preserved from snapshot
        assert ob.best_ask == 0.55
        assert ob.asks[0].size == 500.0

    def test_price_change_adds_new_level(self) -> None:
        adapter = _make_adapter()
        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "1000",
            "bids": [{"price": "0.50", "size": "1000"}],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()

        # Add a new higher bid
        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "asset1", "price": "0.52", "side": "BUY", "size": "800"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.52
        assert len(ob.bids) == 2

    def test_price_change_deletes_level_with_zero_size(self) -> None:
        adapter = _make_adapter()
        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "1000",
            "bids": [
                {"price": "0.50", "size": "1000"},
                {"price": "0.48", "size": "500"},
            ],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()

        # Remove the top bid
        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "asset1", "price": "0.50", "side": "BUY", "size": "0"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.48
        assert len(ob.bids) == 1

    def test_price_change_without_prior_snapshot(self) -> None:
        """Delta arriving before snapshot should initialize book from scratch."""
        adapter = _make_adapter()
        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "1000",
            "price_changes": [
                {"asset_id": "asset1", "price": "0.45", "side": "BUY", "size": "300"},
                {"asset_id": "asset1", "price": "0.60", "side": "SELL", "size": "200"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.45
        assert ob.best_ask == 0.60

    def test_snapshot_replaces_accumulated_state(self) -> None:
        """A new full snapshot should discard any previously accumulated deltas."""
        adapter = _make_adapter()
        # Seed + delta
        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "1000",
            "bids": [{"price": "0.50", "size": "1000"}],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()

        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "asset1", "price": "0.52", "side": "BUY", "size": "800"},
            ],
        }))
        adapter._ob_queue.get_nowait()

        # New snapshot replaces everything
        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "3000",
            "bids": [{"price": "0.40", "size": "100"}],
            "asks": [{"price": "0.70", "size": "100"}],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.40
        assert ob.best_ask == 0.70
        assert len(ob.bids) == 1  # delta level at 0.52 is gone

    def test_message_routing(self) -> None:
        """_handle_message routes price_change to _handle_price_change."""
        adapter = _make_adapter()
        # Send snapshot then price_change via _handle_message
        asyncio.run(adapter._handle_message({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "asset1",
            "timestamp": "1000",
            "bids": [{"price": "0.50", "size": "1000"}],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()

        asyncio.run(adapter._handle_message({
            "event_type": "price_change",
            "market": "0xabc",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "asset1", "price": "0.51", "side": "BUY", "size": "600"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.51
        assert ob.best_ask == 0.55
        assert len(ob.bids) == 2


class TestOrderbookDepthTruncation:
    """Verify that _emit_book() truncates to the configured depth."""

    def test_book_truncated_to_max_depth(self) -> None:
        """A snapshot with 15 levels per side should be capped at N=5."""
        adapter = _make_adapter(orderbook_depth_levels=5)

        bids = [
            {"price": str(round(0.50 - i * 0.01, 2)), "size": str((i + 1) * 100)}
            for i in range(15)
        ]
        asks = [
            {"price": str(round(0.55 + i * 0.01, 2)), "size": str((i + 1) * 100)}
            for i in range(15)
        ]

        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xdeep",
            "asset_id": "deep_asset",
            "timestamp": "1709654400000",
            "bids": bids,
            "asks": asks,
        }))

        ob = adapter._ob_queue.get_nowait()
        assert len(ob.bids) == 5
        assert len(ob.asks) == 5
        # Best bid is the highest price (0.50)
        assert ob.best_bid == 0.50
        # Best ask is the lowest price (0.55)
        assert ob.best_ask == 0.55
        # Verify only top 5 bids (highest prices)
        expected_bid_prices = [0.50, 0.49, 0.48, 0.47, 0.46]
        for level, expected in zip(ob.bids, expected_bid_prices):
            assert level.price == pytest.approx(expected)
        # Verify only top 5 asks (lowest prices)
        expected_ask_prices = [0.55, 0.56, 0.57, 0.58, 0.59]
        for level, expected in zip(ob.asks, expected_ask_prices):
            assert level.price == pytest.approx(expected)

    def test_book_under_max_depth_not_padded(self) -> None:
        """A book with fewer levels than max_depth keeps all levels."""
        adapter = _make_adapter(orderbook_depth_levels=10)

        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xshallow",
            "asset_id": "shallow_asset",
            "timestamp": "1709654400000",
            "bids": [
                {"price": "0.50", "size": "1000"},
                {"price": "0.49", "size": "500"},
            ],
            "asks": [
                {"price": "0.55", "size": "800"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert len(ob.bids) == 2
        assert len(ob.asks) == 1

    def test_incremental_update_respects_depth(self) -> None:
        """Incremental updates that grow the book beyond max_depth are truncated on emit."""
        adapter = _make_adapter(orderbook_depth_levels=3)

        # Seed with 3 bid levels
        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xincr",
            "asset_id": "incr_asset",
            "timestamp": "1000",
            "bids": [
                {"price": "0.50", "size": "100"},
                {"price": "0.49", "size": "200"},
                {"price": "0.48", "size": "300"},
            ],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()

        # Add two more bid levels via delta — internal state grows to 5 levels
        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xincr",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "incr_asset", "price": "0.47", "side": "BUY", "size": "400"},
                {"asset_id": "incr_asset", "price": "0.46", "side": "BUY", "size": "500"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        # Internal state has 5 bid levels but emit should truncate to 3
        assert len(ob.bids) == 3
        assert ob.best_bid == 0.50
        # Only the top 3 prices emitted
        assert [level.price for level in ob.bids] == [0.50, 0.49, 0.48]

    def test_full_state_preserved_after_truncated_emit(self) -> None:
        """Internal book state keeps all levels even after a truncated emit."""
        adapter = _make_adapter(orderbook_depth_levels=2)

        asyncio.run(adapter._handle_book({
            "event_type": "book",
            "market": "0xfull",
            "asset_id": "full_asset",
            "timestamp": "1000",
            "bids": [
                {"price": "0.50", "size": "100"},
                {"price": "0.49", "size": "200"},
                {"price": "0.48", "size": "300"},
                {"price": "0.47", "size": "400"},
            ],
            "asks": [{"price": "0.55", "size": "500"}],
        }))
        adapter._ob_queue.get_nowait()

        # Internal state should still have all 4 bid levels
        assert len(adapter._books["full_asset"]["bids"]) == 4

        # Now delete the top bid — next-best should surface
        asyncio.run(adapter._handle_price_change({
            "event_type": "price_change",
            "market": "0xfull",
            "timestamp": "2000",
            "price_changes": [
                {"asset_id": "full_asset", "price": "0.50", "side": "BUY", "size": "0"},
            ],
        }))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.49
        assert len(ob.bids) == 2  # truncated from 3 remaining to max_depth=2


# ---------------------------------------------------------------------------
# Limitless adapter tests
# ---------------------------------------------------------------------------


def _make_limitless_adapter(orderbook_depth_levels: int = 10) -> LimitlessAdapter:
    settings = MagicMock()
    settings.orderbook_depth_levels = orderbook_depth_levels
    settings.limitless_api_key = ""
    settings.limitless_api_url = "https://api.limitless.exchange"
    settings.limitless_ws_url = "wss://ws.limitless.exchange"
    return LimitlessAdapter(settings)


class TestLimitlessNormalization:
    """REST orderbook fixture normalizes to correct NormalizedOrderbook."""

    def test_normalize_rest_fixture(self) -> None:
        raw = json.loads((FIXTURES / "limitless_orderbook.json").read_text())

        adapter = _make_limitless_adapter()
        ob = adapter._normalize_orderbook("btc-above-100k", raw)

        assert ob is not None
        assert ob.exchange == ExchangeId.LIMITLESS
        assert ob.market_id == "btc-above-100k"
        assert ob.best_bid == 0.62
        assert ob.best_ask == 0.65
        assert ob.mid_price == pytest.approx(0.635)
        assert ob.spread == pytest.approx(0.03)

        # Verify ordering
        for i in range(1, len(ob.bids)):
            assert ob.bids[i].price <= ob.bids[i - 1].price
        for i in range(1, len(ob.asks)):
            assert ob.asks[i].price >= ob.asks[i - 1].price

    def test_normalize_dict_levels(self) -> None:
        """Limitless sends levels as {price, size} objects, not tuples."""
        adapter = _make_limitless_adapter()
        data = {
            "bids": [
                {"price": 0.45, "size": 100},
                {"price": 0.43, "size": 200},
            ],
            "asks": [
                {"price": 0.55, "size": 150},
            ],
        }
        ob = adapter._normalize_orderbook("test-market", data)
        assert ob is not None
        assert ob.best_bid == 0.45
        assert ob.best_ask == 0.55
        assert len(ob.bids) == 2
        assert len(ob.asks) == 1


class TestLimitlessOrderbookUpdate:
    """WS orderbookUpdate event normalizes correctly."""

    def test_ws_orderbook_update_event(self) -> None:
        adapter = _make_limitless_adapter()
        ws_data = {
            "marketSlug": "eth-merge-date",
            "orderbook": {
                "bids": [
                    {"price": "0.70", "size": "500"},
                    {"price": "0.68", "size": "300"},
                ],
                "asks": [
                    {"price": "0.75", "size": "400"},
                ],
            },
            "timestamp": "2024-06-15T12:30:00.000Z",
        }
        asyncio.run(adapter._handle_orderbook_update(ws_data))

        ob = adapter._ob_queue.get_nowait()
        assert ob.exchange == ExchangeId.LIMITLESS
        assert ob.market_id == "eth-merge-date"
        assert ob.best_bid == 0.70
        assert ob.best_ask == 0.75
        assert ob.exchange_timestamp is not None

    def test_ws_update_with_nested_orderbook(self) -> None:
        """WS data wraps bids/asks inside an 'orderbook' key."""
        adapter = _make_limitless_adapter()
        ws_data = {
            "marketSlug": "test-slug",
            "orderbook": {
                "bids": [{"price": "0.40", "size": "100"}],
                "asks": [{"price": "0.60", "size": "200"}],
            },
        }
        asyncio.run(adapter._handle_orderbook_update(ws_data))
        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.40
        assert ob.best_ask == 0.60

    def test_missing_slug_ignored(self) -> None:
        """Events without a market slug are dropped with a warning."""
        adapter = _make_limitless_adapter()
        asyncio.run(adapter._handle_orderbook_update({"orderbook": {}}))
        assert adapter._ob_queue.empty()


class TestLimitlessSyntheticTradeDetection:
    """Consecutive orderbook updates with crossed prices produce trades."""

    def test_buy_detected_when_bid_crosses_ask(self) -> None:
        adapter = _make_limitless_adapter()
        slug = "test-cross"

        # First update: bid=0.50, ask=0.55
        prev = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=slug, asset_id=slug, outcome=OutcomeType.YES,
            bids_raw=[(0.50, 100.0)], asks_raw=[(0.55, 100.0)],
        )
        # Second update: bid jumped to 0.56 (>= prev ask 0.55) → BUY
        curr = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=slug, asset_id=slug, outcome=OutcomeType.YES,
            bids_raw=[(0.56, 100.0)], asks_raw=[(0.58, 100.0)],
        )

        adapter._detect_synthetic_trades(prev, curr, slug)

        trade = adapter._trade_queue.get_nowait()
        assert trade.side == Side.BUY
        assert trade.price == 0.55  # prev best ask
        assert trade.exchange == ExchangeId.LIMITLESS

    def test_sell_detected_when_ask_drops_below_bid(self) -> None:
        adapter = _make_limitless_adapter()
        slug = "test-cross"

        prev = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=slug, asset_id=slug, outcome=OutcomeType.YES,
            bids_raw=[(0.50, 100.0)], asks_raw=[(0.55, 100.0)],
        )
        # Ask dropped to 0.49 (<= prev bid 0.50) → SELL
        curr = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=slug, asset_id=slug, outcome=OutcomeType.YES,
            bids_raw=[(0.47, 100.0)], asks_raw=[(0.49, 100.0)],
        )

        adapter._detect_synthetic_trades(prev, curr, slug)

        trade = adapter._trade_queue.get_nowait()
        assert trade.side == Side.SELL
        assert trade.price == 0.50  # prev best bid

    def test_no_trade_when_no_cross(self) -> None:
        adapter = _make_limitless_adapter()
        slug = "test-no-cross"

        prev = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=slug, asset_id=slug, outcome=OutcomeType.YES,
            bids_raw=[(0.50, 100.0)], asks_raw=[(0.55, 100.0)],
        )
        curr = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=slug, asset_id=slug, outcome=OutcomeType.YES,
            bids_raw=[(0.51, 100.0)], asks_raw=[(0.54, 100.0)],
        )

        adapter._detect_synthetic_trades(prev, curr, slug)
        assert adapter._trade_queue.empty()


class TestLimitlessEmptyOrderbook:
    """Graceful handling of empty bids/asks."""

    def test_empty_bids_and_asks(self) -> None:
        adapter = _make_limitless_adapter()
        ob = adapter._normalize_orderbook("empty-market", {"bids": [], "asks": []})
        assert ob is not None
        assert ob.best_bid is None
        assert ob.best_ask is None
        assert ob.mid_price is None
        assert ob.spread is None

    def test_empty_bids_only(self) -> None:
        adapter = _make_limitless_adapter()
        ob = adapter._normalize_orderbook(
            "one-sided",
            {"bids": [], "asks": [{"price": "0.60", "size": "100"}]},
        )
        assert ob is not None
        assert ob.best_bid is None
        assert ob.best_ask == 0.60


class TestIsoTimestampParsing:
    """Parse ISO 8601 timestamps including Z suffix and None."""

    def test_iso_with_z_suffix(self) -> None:
        ts = _parse_timestamp("2024-01-01T00:00:00.000Z")
        assert ts is not None
        assert ts.year == 2024
        assert ts.month == 1
        assert ts.day == 1
        assert ts.tzinfo is not None

    def test_iso_with_offset(self) -> None:
        ts = _parse_timestamp("2024-06-15T12:30:00+00:00")
        assert ts is not None
        assert ts.hour == 12
        assert ts.minute == 30

    def test_none_returns_none(self) -> None:
        assert _parse_timestamp(None) is None

    def test_empty_string_returns_none(self) -> None:
        assert _parse_timestamp("") is None

    def test_invalid_string_returns_none(self) -> None:
        assert _parse_timestamp("not-a-date") is None


class TestLimitlessResubscribe:
    """Test hot-swap resubscribe() for roster rollover."""

    def test_resubscribe_updates_market_slugs(self) -> None:
        adapter = _make_limitless_adapter()
        adapter._market_slugs = ["old-slug-a", "old-slug-b"]
        adapter._session = MagicMock()  # mock session for REST fetches

        # Mock REST fetch to avoid real HTTP
        adapter._fetch_rest_orderbook = AsyncMock()

        asyncio.run(adapter.resubscribe(["new-slug-c", "old-slug-b"]))

        assert adapter._market_slugs == ["new-slug-c", "old-slug-b"]

    def test_resubscribe_fetches_snapshots_for_new_slugs(self) -> None:
        adapter = _make_limitless_adapter()
        adapter._market_slugs = ["existing"]
        adapter._session = MagicMock()
        adapter._fetch_rest_orderbook = AsyncMock()

        asyncio.run(adapter.resubscribe(["existing", "brand-new"]))

        # Should fetch snapshot for brand-new only
        adapter._fetch_rest_orderbook.assert_called_once_with("brand-new")

    def test_resubscribe_cleans_up_removed_slugs(self) -> None:
        adapter = _make_limitless_adapter()
        adapter._market_slugs = ["to-remove", "to-keep"]
        adapter._last_books = {
            "to-remove:yes": MagicMock(),
            "to-remove:no": MagicMock(),
            "to-keep:yes": MagicMock(),
        }
        adapter._sequence_counters = {"to-remove": 5, "to-keep": 10}
        adapter._session = MagicMock()
        adapter._fetch_rest_orderbook = AsyncMock()

        asyncio.run(adapter.resubscribe(["to-keep"]))

        assert "to-remove:yes" not in adapter._last_books
        assert "to-remove:no" not in adapter._last_books
        assert "to-remove" not in adapter._sequence_counters
        assert "to-keep:yes" in adapter._last_books
        assert "to-keep" in adapter._sequence_counters

    def test_resubscribe_emits_ws_subscription(self) -> None:
        adapter = _make_limitless_adapter()
        adapter._market_slugs = ["old"]
        adapter._session = MagicMock()
        adapter._fetch_rest_orderbook = AsyncMock()

        # Mock Socket.IO client
        mock_sio = MagicMock()
        mock_sio.emit = AsyncMock()
        adapter._sio = mock_sio

        asyncio.run(adapter.resubscribe(["new-a", "new-b"]))

        mock_sio.emit.assert_called_once_with(
            "subscribe_market_prices",
            {"marketSlugs": ["new-a", "new-b"]},
            namespace="/markets",
        )

    def test_resubscribe_no_ws_when_sio_none(self) -> None:
        adapter = _make_limitless_adapter()
        adapter._market_slugs = ["old"]
        adapter._session = MagicMock()
        adapter._fetch_rest_orderbook = AsyncMock()
        adapter._sio = None  # no WS connected

        # Should not raise
        asyncio.run(adapter.resubscribe(["new"]))
        assert adapter._market_slugs == ["new"]


# ---------------------------------------------------------------------------
# Opinion WebSocket adapter tests
# ---------------------------------------------------------------------------


def _make_opinion_adapter(orderbook_depth_levels: int = 10) -> OpinionAdapter:
    settings = MagicMock()
    settings.orderbook_depth_levels = orderbook_depth_levels
    settings.opinion_api_url = "https://api.opinion.xyz"
    settings.opinion_ws_url = "wss://ws.opinion.trade"
    settings.opinion_api_key = ""
    settings.opinion_poll_interval_seconds = 2.0
    return OpinionAdapter(settings)


class TestOpinionDepthDiff:
    """Verify _handle_depth_diff normalizes and updates book state."""

    def test_depth_diff_applies_to_empty_book(self) -> None:
        adapter = _make_opinion_adapter()
        diff = json.loads((FIXTURES / "opinion_depth_diff.json").read_text())

        asyncio.run(adapter._handle_depth_diff(diff))

        ob = adapter._ob_queue.get_nowait()
        assert ob.exchange == ExchangeId.OPINION
        assert ob.market_id == "42"
        # Bids: 0.55 (size=300), 0.52 was size=0 so removed
        assert ob.best_bid == 0.55
        assert len(ob.bids) == 1
        # Asks: 0.60 (size=400), 0.62 (size=250)
        assert ob.best_ask == 0.60
        assert len(ob.asks) == 2

    def test_depth_diff_updates_existing_levels(self) -> None:
        adapter = _make_opinion_adapter()

        # Seed book with initial state
        adapter._books["42"] = {
            "bids": {0.52: 500.0, 0.50: 800.0},
            "asks": {0.58: 600.0, 0.60: 400.0},
        }

        diff = {
            "msgType": "market.depth.diff",
            "marketId": 42,
            "outcomeSide": 1,
            "bids": [
                {"price": "0.52", "size": "700"},  # update existing
            ],
            "asks": [
                {"price": "0.58", "size": "0"},  # remove level
            ],
        }

        asyncio.run(adapter._handle_depth_diff(diff))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.52
        assert ob.bids[0].size == 700.0
        # 0.58 was removed, best ask is now 0.60
        assert ob.best_ask == 0.60
        assert len(ob.asks) == 1

    def test_depth_diff_no_outcome_maps(self) -> None:
        """outcomeSide=2 maps to NO."""
        adapter = _make_opinion_adapter()
        diff = {
            "msgType": "market.depth.diff",
            "marketId": 99,
            "outcomeSide": 2,
            "bids": [{"price": "0.40", "size": "100"}],
            "asks": [{"price": "0.65", "size": "200"}],
        }

        asyncio.run(adapter._handle_depth_diff(diff))

        ob = adapter._ob_queue.get_nowait()
        assert ob.outcome == OutcomeType.NO

    def test_depth_diff_respects_max_depth(self) -> None:
        """Emitted snapshot should be truncated to max depth."""
        adapter = _make_opinion_adapter(orderbook_depth_levels=2)

        # Seed book with many levels
        adapter._books["42"] = {
            "bids": {0.50: 100, 0.49: 200, 0.48: 300, 0.47: 400},
            "asks": {0.55: 100, 0.56: 200, 0.57: 300},
        }

        diff = {
            "msgType": "market.depth.diff",
            "marketId": 42,
            "outcomeSide": 1,
            "bids": [{"price": "0.51", "size": "50"}],
            "asks": [],
        }

        asyncio.run(adapter._handle_depth_diff(diff))

        ob = adapter._ob_queue.get_nowait()
        assert len(ob.bids) == 2
        assert len(ob.asks) == 2
        assert ob.best_bid == 0.51


class TestOpinionTradeEvent:
    """Verify _handle_trade maps real trade events correctly."""

    def test_buy_trade_normalizes(self) -> None:
        adapter = _make_opinion_adapter()
        trade_data = json.loads((FIXTURES / "opinion_trade.json").read_text())

        asyncio.run(adapter._handle_trade(trade_data))

        trade = adapter._trade_queue.get_nowait()
        assert trade.exchange == ExchangeId.OPINION
        assert trade.market_id == "42"
        assert trade.side == Side.BUY
        assert trade.price == 0.58
        assert trade.size == 150.0
        assert trade.usd_notional == 87.0
        assert trade.outcome == OutcomeType.YES

    def test_sell_trade_normalizes(self) -> None:
        adapter = _make_opinion_adapter()
        trade_data = {
            "msgType": "market.last.trade",
            "marketId": 42,
            "outcomeSide": 2,
            "side": "Sell",
            "price": 0.35,
            "shares": 200.0,
            "amount": 70.0,
        }

        asyncio.run(adapter._handle_trade(trade_data))

        trade = adapter._trade_queue.get_nowait()
        assert trade.side == Side.SELL
        assert trade.outcome == OutcomeType.NO
        assert trade.size == 200.0

    def test_split_merge_skipped(self) -> None:
        """Split and Merge trades should be skipped."""
        adapter = _make_opinion_adapter()

        for side_str in ("Split", "Merge", "Unknown"):
            trade_data = {
                "msgType": "market.last.trade",
                "marketId": 42,
                "outcomeSide": 1,
                "side": side_str,
                "price": 0.50,
                "shares": 100.0,
            }
            asyncio.run(adapter._handle_trade(trade_data))

        assert adapter._trade_queue.empty()

    def test_trade_without_amount(self) -> None:
        """Trade without amount field should have usd_notional=None."""
        adapter = _make_opinion_adapter()
        trade_data = {
            "msgType": "market.last.trade",
            "marketId": 42,
            "outcomeSide": 1,
            "side": "Buy",
            "price": 0.50,
            "shares": 100.0,
        }

        asyncio.run(adapter._handle_trade(trade_data))

        trade = adapter._trade_queue.get_nowait()
        assert trade.usd_notional is None


class TestOpinionHeartbeat:
    """Verify heartbeat sends correctly via _heartbeat_loop."""

    def test_heartbeat_sends_message(self) -> None:
        """Heartbeat loop should send HEARTBEAT action."""
        import unittest.mock as mock

        adapter = _make_opinion_adapter()

        ws = mock.AsyncMock()
        ws.send_str = mock.AsyncMock()

        async def run_heartbeat() -> None:
            # Run heartbeat loop for a very short time
            task = asyncio.create_task(adapter._heartbeat_loop(ws))
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        asyncio.run(run_heartbeat())

        ws.send_str.assert_called()
        call_arg = ws.send_str.call_args[0][0]
        data = json.loads(call_arg)
        assert data["action"] == "HEARTBEAT"


class TestOpinionWsMessageRouting:
    """Verify _handle_ws_message routes to correct handlers."""

    def test_depth_diff_routed(self) -> None:
        adapter = _make_opinion_adapter()
        msg = json.dumps({
            "msgType": "market.depth.diff",
            "marketId": 42,
            "outcomeSide": 1,
            "bids": [{"price": "0.55", "size": "300"}],
            "asks": [],
        })

        asyncio.run(adapter._handle_ws_message(msg))

        ob = adapter._ob_queue.get_nowait()
        assert ob.best_bid == 0.55

    def test_trade_routed(self) -> None:
        adapter = _make_opinion_adapter()
        msg = json.dumps({
            "msgType": "market.last.trade",
            "marketId": 42,
            "outcomeSide": 1,
            "side": "Buy",
            "price": 0.60,
            "shares": 50.0,
        })

        asyncio.run(adapter._handle_ws_message(msg))

        trade = adapter._trade_queue.get_nowait()
        assert trade.side == Side.BUY

    def test_heartbeat_ack_ignored(self) -> None:
        adapter = _make_opinion_adapter()
        msg = json.dumps({"msgType": "HEARTBEAT"})

        asyncio.run(adapter._handle_ws_message(msg))

        assert adapter._ob_queue.empty()
        assert adapter._trade_queue.empty()

    def test_invalid_json_handled(self) -> None:
        adapter = _make_opinion_adapter()
        asyncio.run(adapter._handle_ws_message("not json at all"))

        assert adapter._ob_queue.empty()
        assert adapter._trade_queue.empty()
