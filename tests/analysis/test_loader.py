"""Tests for Parquet loader and DataFrame conversion."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from alpha_pipeline.analysis.loader import load_feature_vectors, vectors_to_feature_frames
from alpha_pipeline.data.collector import FeatureCollector
from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector


def _write_fixture(path_dir: Path, vectors: list[FeatureVector]) -> Path:
    """Write vectors to a Parquet file via the collector."""
    collector = FeatureCollector(path_dir, flush_every=0)
    for v in vectors:
        collector.write(v)
    collector.close()
    files = sorted(path_dir.glob("*.parquet"))
    assert len(files) >= 1
    return files[0]


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
def sample_parquet(tmp_path) -> Path:
    ts1 = datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)
    ts2 = datetime(2026, 3, 5, 10, 1, tzinfo=timezone.utc)
    ts3 = datetime(2026, 3, 5, 10, 2, tzinfo=timezone.utc)
    vectors = [
        _make_vector(ts1, "mkt_A"),
        _make_vector(ts2, "mkt_A"),
        _make_vector(ts3, "mkt_B"),
    ]
    return _write_fixture(tmp_path, vectors)


def test_load_all(sample_parquet):
    vectors = load_feature_vectors([sample_parquet])
    assert len(vectors) == 3


def test_load_filter_market(sample_parquet):
    vectors = load_feature_vectors([sample_parquet], market_id="mkt_A")
    assert len(vectors) == 2
    assert all(v.market_id == "mkt_A" for v in vectors)


def test_load_filter_time(sample_parquet):
    start = datetime(2026, 3, 5, 10, 1, tzinfo=timezone.utc)
    vectors = load_feature_vectors([sample_parquet], start=start)
    assert len(vectors) == 2


def test_load_from_directory(sample_parquet):
    """Loading from a directory globs all .parquet files."""
    vectors = load_feature_vectors([sample_parquet.parent])
    assert len(vectors) == 3


def test_vectors_to_feature_frames(sample_parquet):
    vectors = load_feature_vectors([sample_parquet])
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
    """Empty directory produces no vectors."""
    vectors = load_feature_vectors([tmp_path])
    assert vectors == []


def test_frames_sorted_by_timestamp(sample_parquet):
    vectors = load_feature_vectors([sample_parquet])
    frames = vectors_to_feature_frames(vectors)
    arb_df = frames["cross_exchange.arb_spread"]
    ts = arb_df["timestamp"].to_list()
    assert ts == sorted(ts)
