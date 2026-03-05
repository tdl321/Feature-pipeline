"""Tests for TimeSeriesBuffer."""
from __future__ import annotations

import time

import polars as pl
import pytest

from alpha_pipeline.data.buffer import TimeSeriesBuffer


class TestTimeSeriesBuffer:
    def test_append_and_size(self) -> None:
        buf = TimeSeriesBuffer(max_rows=100)
        assert buf.size == 0
        assert buf.is_empty

        buf.append({"timestamp": time.time(), "value": 1})
        assert buf.size == 1
        assert not buf.is_empty

    def test_append_requires_timestamp(self) -> None:
        buf = TimeSeriesBuffer()
        with pytest.raises(ValueError, match="timestamp"):
            buf.append({"value": 1})

    def test_ring_buffer_eviction(self) -> None:
        buf = TimeSeriesBuffer(max_rows=5)
        t0 = time.time()
        for i in range(10):
            buf.append({"timestamp": t0 + i, "value": i})

        assert buf.size == 5
        df = buf.to_polars()
        # Should contain the last 5 records (values 5-9)
        assert df["value"].to_list() == [5, 6, 7, 8, 9]

    def test_to_polars_empty(self) -> None:
        buf = TimeSeriesBuffer()
        df = buf.to_polars()
        assert isinstance(df, pl.DataFrame)
        assert df.is_empty()

    def test_to_polars_with_data(self) -> None:
        buf = TimeSeriesBuffer()
        t0 = time.time()
        for i in range(3):
            buf.append({"timestamp": t0 + i, "price": 0.5 + i * 0.01})

        df = buf.to_polars()
        assert len(df) == 3
        assert "timestamp" in df.columns
        assert "price" in df.columns

    def test_to_polars_last_n_seconds(self) -> None:
        buf = TimeSeriesBuffer()
        now = time.time()
        buf.append({"timestamp": now - 120, "value": "old"})
        buf.append({"timestamp": now - 5, "value": "recent1"})
        buf.append({"timestamp": now, "value": "recent2"})

        df = buf.to_polars(last_n_seconds=10)
        assert len(df) == 2
        assert df["value"].to_list() == ["recent1", "recent2"]

    def test_evict_expired(self) -> None:
        buf = TimeSeriesBuffer(ttl_seconds=10)
        now = time.time()
        buf.append({"timestamp": now - 20, "value": "expired1"})
        buf.append({"timestamp": now - 15, "value": "expired2"})
        buf.append({"timestamp": now - 5, "value": "valid"})

        evicted = buf.evict_expired()
        assert evicted == 2
        assert buf.size == 1

    def test_append_many(self) -> None:
        buf = TimeSeriesBuffer()
        t0 = time.time()
        records = [{"timestamp": t0 + i, "v": i} for i in range(5)]
        buf.append_many(records)
        assert buf.size == 5
