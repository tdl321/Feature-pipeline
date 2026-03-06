"""Parquet sink for persisting wallet tracking snapshots to disk."""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from alpha_pipeline.tracking.models import WalletSnapshot
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_DEFAULT_FLUSH_EVERY = 50


class WalletParquetStore:
    """Buffer wallet snapshots and flush them to date-partitioned Parquet files.

    Mirrors the ``FeatureCollector`` pattern from ``data/collector.py``.

    Each flush creates a new chunk file named
    ``snapshots_YYYY-MM-DD_HHMMSS.parquet`` under *output_dir*.  PnL history
    backfills are written immediately to
    ``pnl_history_{addr_first6}.parquet``.

    Parameters
    ----------
    output_dir:
        Directory where Parquet files are written.  Created if missing.
    flush_every:
        Number of rows to buffer before writing a Parquet chunk.
        Set to ``0`` to disable automatic flushing (only on ``close()``).
    """

    def __init__(
        self,
        output_dir: str | Path = "./output/wallets",
        *,
        flush_every: int = _DEFAULT_FLUSH_EVERY,
    ) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._flush_every = flush_every
        self._buffer: list[dict] = []

    # -- internal helpers ----------------------------------------------------

    @staticmethod
    def _snapshot_to_rows(snapshot: WalletSnapshot) -> list[dict]:
        """Flatten a WalletSnapshot into one dict per position."""
        total_pnl = 0.0
        pnl_pct = 0.0
        volume = 0.0

        if snapshot.pnl_summary:
            total_pnl = (
                snapshot.pnl_summary.current_value
                - snapshot.pnl_summary.previous_value
            )
            pnl_pct = snapshot.pnl_summary.percent_change
        if snapshot.traded_volume:
            volume = snapshot.traded_volume.total_volume_usd

        rows: list[dict] = []
        for pos in snapshot.positions:
            rows.append({
                "polled_at": snapshot.polled_at,
                "wallet": snapshot.address,
                "exchange": snapshot.exchange,
                "market_slug": pos.market_slug,
                "outcome": pos.outcome,
                "shares": pos.shares,
                "cost_usd": pos.cost_usd,
                "fill_price": pos.fill_price,
                "market_value_usd": pos.market_value_usd,
                "realized_pnl_usd": pos.realized_pnl_usd,
                "unrealized_pnl_usd": pos.unrealized_pnl_usd,
                "total_pnl_usd": total_pnl,
                "pnl_percent_change": pnl_pct,
                "traded_volume_usd": volume,
            })

        # If no positions, write a summary-only row so we never lose poll data.
        if not rows:
            rows.append({
                "polled_at": snapshot.polled_at,
                "wallet": snapshot.address,
                "exchange": snapshot.exchange,
                "market_slug": "",
                "outcome": "",
                "shares": 0.0,
                "cost_usd": 0.0,
                "fill_price": 0.0,
                "market_value_usd": 0.0,
                "realized_pnl_usd": 0.0,
                "unrealized_pnl_usd": 0.0,
                "total_pnl_usd": total_pnl,
                "pnl_percent_change": pnl_pct,
                "traded_volume_usd": volume,
            })

        return rows

    def _flush_buffer(self) -> None:
        """Write the current buffer to a new Parquet chunk file."""
        if not self._buffer:
            return

        df = pl.DataFrame(self._buffer)
        first_ts = self._buffer[0]["polled_at"]
        d = first_ts.date() if isinstance(first_ts, datetime) else first_ts
        now = datetime.now(tz=timezone.utc)
        chunk_name = f"snapshots_{d.isoformat()}_{now.strftime('%H%M%S')}.parquet"
        path = self._output_dir / chunk_name
        df.write_parquet(path)
        logger.info("wallet_chunk_written", path=str(path), rows=len(self._buffer))
        self._buffer.clear()

    # -- public API ----------------------------------------------------------

    def write_snapshot(self, snapshot: WalletSnapshot) -> None:
        """Buffer a wallet snapshot for later Parquet flush."""
        self._buffer.extend(self._snapshot_to_rows(snapshot))
        if self._flush_every > 0 and len(self._buffer) >= self._flush_every:
            self._flush_buffer()

    def write_pnl_history(
        self,
        address: str,
        exchange: str,
        points: list[dict],
    ) -> None:
        """Write API backfill PnL data points directly to a Parquet file.

        Parameters
        ----------
        address:
            Wallet address.
        exchange:
            Exchange identifier.
        points:
            List of ``{"timestamp": datetime, "value": float}`` dicts from
            the ``/pnl-chart?timeframe=all`` ``data[]`` array.
        """
        if not points:
            return

        rows = [
            {
                "timestamp": pt["timestamp"],
                "wallet": address,
                "exchange": exchange,
                "value": pt["value"],
            }
            for pt in points
        ]
        df = pl.DataFrame(rows)
        addr_short = address[:6]
        path = self._output_dir / f"pnl_history_{addr_short}.parquet"
        df.write_parquet(path)
        logger.info(
            "pnl_history_written",
            path=str(path),
            wallet=address,
            points=len(points),
        )

    def flush(self) -> None:
        """Force-flush any buffered snapshot rows to a Parquet chunk file."""
        self._flush_buffer()

    def close(self) -> None:
        """Flush remaining buffer and release resources."""
        self._flush_buffer()

    def __enter__(self) -> WalletParquetStore:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
