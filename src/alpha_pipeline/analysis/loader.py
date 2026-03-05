"""Load persisted Parquet feature data into polars DataFrames."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import orjson
import polars as pl

from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector


def load_feature_vectors(
    paths: list[Path],
    *,
    market_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[FeatureVector]:
    """Read Parquet files and return filtered FeatureVectors.

    Parameters
    ----------
    paths:
        Parquet file paths (or a single directory to glob ``*.parquet`` from).
    market_id:
        If set, keep only vectors matching this market.
    start / end:
        If set, keep only vectors within the time range (inclusive).
    """
    resolved: list[Path] = []
    for p in paths:
        if p.is_dir():
            resolved.extend(sorted(p.glob("*.parquet")))
        else:
            resolved.append(p)

    if not resolved:
        return []

    dfs: list[pl.DataFrame] = []
    for path in resolved:
        try:
            df = pl.read_parquet(path)
        except Exception:
            continue
        if df.is_empty():
            continue
        dfs.append(df)

    if not dfs:
        return []

    combined = pl.concat(dfs)

    # Apply filters at the DataFrame level before reconstructing objects.
    if market_id:
        combined = combined.filter(pl.col("market_id") == market_id)
    if start:
        combined = combined.filter(pl.col("timestamp") >= start)
    if end:
        combined = combined.filter(pl.col("timestamp") <= end)

    if combined.is_empty():
        return []

    # Group rows back into FeatureVectors (one vector = all rows sharing
    # the same timestamp + market_id + exchange).
    vectors: list[FeatureVector] = []
    grouped = combined.group_by(["timestamp", "market_id", "exchange"], maintain_order=True)
    for keys, group_df in grouped:
        ts, mid, exch = keys
        features: list[FeatureOutput] = []
        for row in group_df.iter_rows(named=True):
            values = orjson.loads(row["values_json"])
            features.append(
                FeatureOutput(
                    feature_name=row["feature_name"],
                    timestamp=ts,
                    market_id=mid,
                    values=values,
                )
            )
        vectors.append(
            FeatureVector(
                timestamp=ts,
                market_id=mid,
                exchange=ExchangeId(exch),
                features=tuple(features),
            )
        )

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
