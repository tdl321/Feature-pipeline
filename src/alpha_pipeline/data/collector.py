"""JSONL sink for persisting FeatureVectors to disk."""
from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from alpha_pipeline.schemas.feature import FeatureVector
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


class FeatureCollector:
    """Appends FeatureVectors as JSONL lines to date-partitioned files.

    Files are named ``features_YYYY-MM-DD.jsonl`` under *output_dir*.
    Uses append-binary mode with flush-per-write for crash safety.
    Automatically rotates to a new file when the date changes.
    """

    def __init__(self, output_dir: str | Path) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._current_date: date | None = None
        self._file = None

    def _filename_for_date(self, d: date) -> Path:
        return self._output_dir / f"features_{d.isoformat()}.jsonl"

    def _ensure_file(self, d: date) -> None:
        """Open or rotate the output file for the given date."""
        if self._current_date == d and self._file is not None:
            return
        self.close()
        path = self._filename_for_date(d)
        self._file = open(path, "ab")  # noqa: SIM115
        self._current_date = d
        logger.info("collector_file_opened", path=str(path))

    def write(self, vector: FeatureVector) -> None:
        """Persist a single FeatureVector as a JSONL line."""
        today = vector.timestamp.date()
        self._ensure_file(today)
        line = vector.to_json_bytes() + b"\n"
        self._file.write(line)
        self._file.flush()

    def close(self) -> None:
        """Flush and close the current file handle."""
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None
            self._current_date = None

    def __enter__(self) -> FeatureCollector:
        return self

    def __exit__(self, *exc) -> None:
        self.close()
