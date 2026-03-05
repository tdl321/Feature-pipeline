"""Tests for exchange adapter normalization."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

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
