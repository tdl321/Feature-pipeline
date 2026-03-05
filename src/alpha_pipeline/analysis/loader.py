"""Load persisted JSONL feature data into polars DataFrames."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl

from alpha_pipeline.schemas.feature import FeatureVector


def load_feature_vectors(
    paths: list[Path],
    *,
    market_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[FeatureVector]:
    """Read JSONL files and return filtered FeatureVectors.

    Parameters
    ----------
    paths:
        JSONL file paths (or a single directory to glob ``*.jsonl`` from).
    market_id:
        If set, keep only vectors matching this market.
    start / end:
        If set, keep only vectors within the time range (inclusive).
    """
    resolved: list[Path] = []
    for p in paths:
        if p.is_dir():
            resolved.extend(sorted(p.glob("*.jsonl")))
        else:
            resolved.append(p)

    vectors: list[FeatureVector] = []
    for path in resolved:
        with open(path, "rb") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                vec = FeatureVector.from_json_bytes(line)
                if market_id and vec.market_id != market_id:
                    continue
                if start and vec.timestamp < start:
                    continue
                if end and vec.timestamp > end:
                    continue
                vectors.append(vec)

    return vectors


def vectors_to_feature_frames(
    vectors: list[FeatureVector],
) -> dict[str, pl.DataFrame]:
    """Flatten FeatureVectors into per-feature polars DataFrames.

    Returns a dict keyed by ``feature_name``.  Each DataFrame has columns:
    ``timestamp``, ``market_id``, ``exchange``, plus all keys from
    ``FeatureOutput.values`` expanded to top-level columns.
    """
    buckets: dict[str, list[dict]] = {}

    for vec in vectors:
        for feat in vec.features:
            row = {
                "timestamp": feat.timestamp,
                "market_id": feat.market_id,
                "exchange": vec.exchange.value,
                **feat.values,
            }
            buckets.setdefault(feat.feature_name, []).append(row)

    frames: dict[str, pl.DataFrame] = {}
    for name, rows in buckets.items():
        df = pl.DataFrame(rows)
        if "timestamp" in df.columns:
            df = df.sort("timestamp")
        frames[name] = df

    return frames
