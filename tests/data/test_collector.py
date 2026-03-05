"""Tests for FeatureCollector JSONL persistence."""
from __future__ import annotations

from datetime import datetime, timezone

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
    """Collector writes JSONL that can be deserialized back."""
    collector = FeatureCollector(tmp_path)
    vec = _make_vector()
    collector.write(vec)
    collector.close()

    files = list(tmp_path.glob("*.jsonl"))
    assert len(files) == 1

    with open(files[0], "rb") as f:
        lines = [line.strip() for line in f if line.strip()]
    assert len(lines) == 1

    restored = FeatureVector.from_json_bytes(lines[0])
    assert restored.market_id == vec.market_id
    assert restored.features[0].values == vec.features[0].values


def test_multiple_writes(tmp_path):
    """Multiple writes append to the same file."""
    collector = FeatureCollector(tmp_path)
    for _ in range(5):
        collector.write(_make_vector())
    collector.close()

    files = list(tmp_path.glob("*.jsonl"))
    assert len(files) == 1

    with open(files[0], "rb") as f:
        lines = [line for line in f if line.strip()]
    assert len(lines) == 5


def test_date_rotation(tmp_path):
    """Different dates produce different files."""
    collector = FeatureCollector(tmp_path)
    collector.write(_make_vector(ts=datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)))
    collector.write(_make_vector(ts=datetime(2026, 3, 6, 10, 0, tzinfo=timezone.utc)))
    collector.close()

    files = sorted(tmp_path.glob("*.jsonl"))
    assert len(files) == 2
    assert "2026-03-05" in files[0].name
    assert "2026-03-06" in files[1].name


def test_context_manager(tmp_path):
    """Context manager closes cleanly."""
    with FeatureCollector(tmp_path) as collector:
        collector.write(_make_vector())

    files = list(tmp_path.glob("*.jsonl"))
    assert len(files) == 1


def test_creates_output_dir(tmp_path):
    """Collector creates output directory if it doesn't exist."""
    nested = tmp_path / "a" / "b" / "c"
    collector = FeatureCollector(nested)
    collector.write(_make_vector())
    collector.close()
    assert nested.exists()
    assert len(list(nested.glob("*.jsonl"))) == 1
