from __future__ import annotations

import time
from collections import deque
from typing import Any

import polars as pl


class TimeSeriesBuffer:
    """Ring buffer for time-series data with Polars export."""

    def __init__(self, max_rows: int = 100_000, ttl_seconds: int = 3600) -> None:
        self._max_rows = max_rows
        self._ttl_seconds = ttl_seconds
        self._buffer: deque[dict[str, Any]] = deque(maxlen=max_rows)

    def append(self, record: dict[str, Any]) -> None:
        """Append a record. Must contain 'timestamp' key (epoch float)."""
        if "timestamp" not in record:
            raise ValueError("Record must contain 'timestamp' key")
        self._buffer.append(record)

    def append_many(self, records: list[dict[str, Any]]) -> None:
        for r in records:
            self.append(r)

    def to_polars(self, last_n_seconds: float | None = None) -> pl.DataFrame:
        """Export buffer to Polars DataFrame, optionally filtering by recency."""
        if not self._buffer:
            return pl.DataFrame()

        df = pl.DataFrame(list(self._buffer))

        if last_n_seconds is not None:
            cutoff = time.time() - last_n_seconds
            df = df.filter(pl.col("timestamp") >= cutoff)

        return df

    def evict_expired(self) -> int:
        """Remove records older than TTL. Returns count of evicted records."""
        if not self._buffer:
            return 0
        cutoff = time.time() - self._ttl_seconds
        initial = len(self._buffer)
        while self._buffer and self._buffer[0].get("timestamp", float("inf")) < cutoff:
            self._buffer.popleft()
        return initial - len(self._buffer)

    @property
    def size(self) -> int:
        return len(self._buffer)

    @property
    def is_empty(self) -> bool:
        return len(self._buffer) == 0
