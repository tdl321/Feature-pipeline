"""Parquet sink for persisting FeatureVectors to disk."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import orjson
import polars as pl

from alpha_pipeline.schemas.feature import FeatureVector
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

# Default number of vectors buffered before flushing to a Parquet chunk file.
_DEFAULT_FLUSH_EVERY = 100


class FeatureCollector:
    """Buffers FeatureVectors and flushes them to date-partitioned Parquet files.

    Each flush creates a new chunk file named
    ``features_YYYY-MM-DD_HHMMSS.parquet`` under *output_dir*.  The loader
    globs ``*.parquet`` so multiple chunks per day are fine.

    Parameters
    ----------
    output_dir:
        Directory where Parquet files are written.  Created if it does not
        exist.
    flush_every:
        Number of vectors to buffer before writing a Parquet chunk.  Set to
        ``0`` to disable automatic flushing (only flushes on ``close()``).
    """

    def __init__(
        self,
        output_dir: str | Path,
        *,
        flush_every: int = _DEFAULT_FLUSH_EVERY,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._flush_every = flush_every
        self._buffer: list[dict] = []

    # -- internal helpers ----------------------------------------------------

    @staticmethod
    def _vector_to_rows(vector: FeatureVector) -> list[dict]:
        """Flatten a FeatureVector into one dict per FeatureOutput."""
        rows: list[dict] = []
        for feat in vector.features:
            rows.append({
                "timestamp": vector.timestamp,
                "market_id": vector.market_id,
                "exchange": vector.exchange.value,
                "feature_name": feat.feature_name,
                "values_json": orjson.dumps(feat.values).decode("utf-8"),
                "correlation_id": vector.correlation_id,
                "trigger_event_ids": orjson.dumps(
                    list(vector.trigger_event_ids)
                ).decode("utf-8"),
            })
        return rows

    def _flush_buffer(self) -> None:
        """Write the current buffer to a new Parquet chunk file."""
        if not self._buffer:
            return

        df = pl.DataFrame(self._buffer)
        # Use the date from the first row and current time for the filename.
        first_ts = self._buffer[0]["timestamp"]
        d = first_ts.date() if isinstance(first_ts, datetime) else first_ts
        now = datetime.now(tz=timezone.utc)
        chunk_name = f"features_{d.isoformat()}_{now.strftime('%H%M%S')}.parquet"
        path = self._output_dir / chunk_name
        df.write_parquet(path)
        logger.info("collector_chunk_written", path=str(path), rows=len(self._buffer))
        self._buffer.clear()

    # -- public API ----------------------------------------------------------

    def write(self, vector: FeatureVector) -> None:
        """Buffer a single FeatureVector for later Parquet flush."""
        self._buffer.extend(self._vector_to_rows(vector))
        if self._flush_every > 0 and len(self._buffer) >= self._flush_every:
            self._flush_buffer()

    def flush(self) -> None:
        """Force-flush any buffered rows to a Parquet chunk file."""
        self._flush_buffer()

    def close(self) -> None:
        """Flush remaining buffer and release resources."""
        self._flush_buffer()

    def __enter__(self) -> FeatureCollector:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
