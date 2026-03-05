"""Tests for exchange adapter normalization."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from alpha_pipeline.data.adapters.polymarket import PolymarketAdapter
from alpha_pipeline.schemas.enums import ExchangeId, OutcomeType
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
