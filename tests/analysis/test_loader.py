"""Tests for JSONL loader and DataFrame conversion."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from alpha_pipeline.analysis.loader import load_feature_vectors, vectors_to_feature_frames
from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector


def _write_fixture(path: Path, vectors: list[FeatureVector]) -> Path:
    """Write vectors to a JSONL file."""
    with open(path, "wb") as f:
        for v in vectors:
            f.write(v.to_json_bytes() + b"\n")
    return path


def _make_vector(
    ts: datetime,
    market_id: str = "mkt_A",
    features: tuple[FeatureOutput, ...] | None = None,
) -> FeatureVector:
    if features is None:
        features = (
            FeatureOutput(
                feature_name="cross_exchange.arb_spread",
                timestamp=ts,
                market_id=market_id,
                values={"arb_spread": 0.01, "arb_spread_bps": 100.0,
                        "bid_exchange": "polymarket", "ask_exchange": "opinion"},
            ),
            FeatureOutput(
                feature_name="order_flow.tob_imbalance",
                timestamp=ts,
                market_id=market_id,
                values={"tob_imbalance": 0.6, "tob_imbalance_ma": 0.55,
                        "tob_imbalance_zscore": 1.2},
            ),
        )
    return FeatureVector(
        timestamp=ts,
        market_id=market_id,
        exchange=ExchangeId.POLYMARKET,
        features=features,
    )


@pytest.fixture
def sample_jsonl(tmp_path) -> Path:
    ts1 = datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 3, 5, 10, 1, tzinfo=timezone.utc)
    ts3 = datetime(2026, 3, 5, 10, 2, tzinfo=timezone.utc)
    vectors = [
        _make_vector(ts1, "mkt_A"),
        _make_vector(ts2, "mkt_A"),
        _make_vector(ts3, "mkt_B"),
    ]
    return _write_fixture(tmp_path / "features_2026-03-05.jsonl", vectors)


def test_load_all(sample_jsonl):
    vectors = load_feature_vectors([sample_jsonl])
    assert len(vectors) == 3


def test_load_filter_market(sample_jsonl):
    vectors = load_feature_vectors([sample_jsonl], market_id="mkt_A")
    assert len(vectors) == 2
    assert all(v.market_id == "mkt_A" for v in vectors)


def test_load_filter_time(sample_jsonl):
    start = datetime(2026, 3, 5, 10, 1, tzinfo=timezone.utc)
    vectors = load_feature_vectors([sample_jsonl], start=start)
    assert len(vectors) == 2


def test_load_from_directory(sample_jsonl):
    """Loading from a directory globs all .jsonl files."""
    vectors = load_feature_vectors([sample_jsonl.parent])
    assert len(vectors) == 3


def test_vectors_to_feature_frames(sample_jsonl):
    vectors = load_feature_vectors([sample_jsonl])
    frames = vectors_to_feature_frames(vectors)

    assert "cross_exchange.arb_spread" in frames
    assert "order_flow.tob_imbalance" in frames

    arb_df = frames["cross_exchange.arb_spread"]
    assert "arb_spread_bps" in arb_df.columns
    assert "timestamp" in arb_df.columns
    assert len(arb_df) == 3

    imb_df = frames["order_flow.tob_imbalance"]
    assert "tob_imbalance_zscore" in imb_df.columns
    assert len(imb_df) == 3


def test_empty_input(tmp_path):
    empty = tmp_path / "empty.jsonl"
    empty.touch()
    vectors = load_feature_vectors([empty])
    assert vectors == []


def test_frames_sorted_by_timestamp(sample_jsonl):
    vectors = load_feature_vectors([sample_jsonl])
    frames = vectors_to_feature_frames(vectors)
    arb_df = frames["cross_exchange.arb_spread"]
    ts = arb_df["timestamp"].to_list()
    assert ts == sorted(ts)
