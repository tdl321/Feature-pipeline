"""Tests for FeatureCollector Parquet persistence."""
from __future__ import annotations

from datetime import datetime, timezone

import polars as pl

from alpha_pipeline.data.collector import FeatureCollector
from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector


def _make_vector(
    ts: datetime | None = None,
    market_id: str = "test_market",
) -> FeatureVector:
    ts = ts or datetime.now(tz=timezone.utc)
    return FeatureVector(
        timestamp=ts,
        market_id=market_id,
        exchange=ExchangeId.POLYMARKET,
        features=(
            FeatureOutput(
                feature_name="cross_exchange.arb_spread",
                timestamp=ts,
                market_id=market_id,
                values={"arb_spread": 0.02, "arb_spread_bps": 200.0},
            ),
        ),
    )


def test_write_and_read_back(tmp_path):
    """Collector writes Parquet that can be read back."""
    collector = FeatureCollector(tmp_path)
    vec = _make_vector()
    collector.write(vec)
    collector.close()

    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 1

    df = pl.read_parquet(files[0])
    assert len(df) == 1
    assert df["market_id"][0] == vec.market_id
    assert df["feature_name"][0] == "cross_exchange.arb_spread"


def test_multiple_writes(tmp_path):
    """Multiple writes within flush threshold stay in one chunk."""
    collector = FeatureCollector(tmp_path, flush_every=0)
    for _ in range(5):
        collector.write(_make_vector())
    collector.close()

    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 1

    df = pl.read_parquet(files[0])
    assert len(df) == 5


def test_date_rotation(tmp_path):
    """Different dates produce different chunk files."""
    collector = FeatureCollector(tmp_path, flush_every=0)
    collector.write(_make_vector(ts=datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)))
    collector.flush()
    collector.write(_make_vector(ts=datetime(2026, 3, 6, 10, 0, tzinfo=timezone.utc)))
    collector.close()

    files = sorted(tmp_path.glob("*.parquet"))
    assert len(files) == 2
    assert "2026-03-05" in files[0].name
    assert "2026-03-06" in files[1].name


def test_context_manager(tmp_path):
    """Context manager closes cleanly."""
    with FeatureCollector(tmp_path) as collector:
        collector.write(_make_vector())

    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 1


def test_creates_output_dir(tmp_path):
    """Collector creates output directory if it doesn't exist."""
    nested = tmp_path / "a" / "b" / "c"
    collector = FeatureCollector(nested)
    collector.write(_make_vector())
    collector.close()
    assert nested.exists()
    assert len(list(nested.glob("*.parquet"))) == 1


def test_flush_every_threshold(tmp_path):
    """Buffer flushes automatically when threshold is reached."""
    collector = FeatureCollector(tmp_path, flush_every=3)
    for _ in range(3):
        collector.write(_make_vector())

    # Should have auto-flushed after the 3rd write
    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 1

    df = pl.read_parquet(files[0])
    assert len(df) == 3

    collector.close()


def test_empty_close(tmp_path):
    """Closing without any writes produces no files."""
    collector = FeatureCollector(tmp_path)
    collector.close()
    files = list(tmp_path.glob("*.parquet"))
    assert len(files) == 0
