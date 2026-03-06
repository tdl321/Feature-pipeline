"""Load persisted wallet Parquet data into polars DataFrames."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl


def _resolve_parquets(directory: str | Path, prefix: str) -> list[Path]:
    """Glob Parquet files matching *prefix* from *directory*."""
    d = Path(directory)
    if not d.is_dir():
        return []
    return sorted(d.glob(f"{prefix}*.parquet"))


def load_snapshots(
    directory: str | Path = "./output/wallets",
    *,
    wallet: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pl.DataFrame:
    """Read snapshot Parquet files and return a filtered DataFrame.

    Parameters
    ----------
    directory:
        Directory containing ``snapshots_*.parquet`` files.
    wallet:
        If set, keep only rows matching this wallet address.
    start / end:
        If set, keep only rows within the time range (inclusive).
    """
    paths = _resolve_parquets(directory, "snapshots_")
    if not paths:
        return pl.DataFrame()

    dfs: list[pl.DataFrame] = []
    for path in paths:
        try:
            df = pl.read_parquet(path)
        except Exception:
            continue
        if df.is_empty():
            continue
        dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    combined = pl.concat(dfs)

    if wallet:
        combined = combined.filter(pl.col("wallet") == wallet)
    if start:
        combined = combined.filter(pl.col("polled_at") >= start)
    if end:
        combined = combined.filter(pl.col("polled_at") <= end)

    return combined.sort("polled_at")


def load_pnl_history(
    directory: str | Path = "./output/wallets",
    *,
    wallet: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> pl.DataFrame:
    """Read PnL history Parquet files and return a filtered DataFrame.

    Parameters
    ----------
    directory:
        Directory containing ``pnl_history_*.parquet`` files.
    wallet:
        If set, keep only rows matching this wallet address.
    start / end:
        If set, keep only rows within the time range (inclusive).
    """
    paths = _resolve_parquets(directory, "pnl_history_")
    if not paths:
        return pl.DataFrame()

    dfs: list[pl.DataFrame] = []
    for path in paths:
        try:
            df = pl.read_parquet(path)
        except Exception:
            continue
        if df.is_empty():
            continue
        dfs.append(df)

    if not dfs:
        return pl.DataFrame()

    combined = pl.concat(dfs)

    if wallet:
        combined = combined.filter(pl.col("wallet") == wallet)
    if start:
        combined = combined.filter(pl.col("timestamp") >= start)
    if end:
        combined = combined.filter(pl.col("timestamp") <= end)

    return combined.sort("timestamp")
