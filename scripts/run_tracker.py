#!/usr/bin/env python3
"""Standalone CLI for the wallet tracker.

Usage:
    python scripts/run_tracker.py --wallets 0xABC,0xDEF
    python scripts/run_tracker.py --wallets-file wallets.txt
    WALLET_TRACKING_ADDRESSES=0xABC,0xDEF python scripts/run_tracker.py
"""
from __future__ import annotations

import argparse
import asyncio
import os
import signal
import sys
from pathlib import Path

# Ensure src/ is importable when running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from alpha_pipeline.config import get_settings
from alpha_pipeline.tracking.collector import WalletMetricsCollector
from alpha_pipeline.tracking.limitless_fetcher import LimitlessWalletFetcher
from alpha_pipeline.tracking.storage import WalletParquetStore
from alpha_pipeline.tracking.tracker import WalletTracker
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


def _parse_wallets(args: argparse.Namespace) -> list[str]:
    """Resolve wallet addresses from CLI flags, file, or env var."""
    if args.wallets:
        return [w.strip() for w in args.wallets.split(",") if w.strip()]

    if args.wallets_file:
        path = Path(args.wallets_file)
        if not path.exists():
            logger.error("wallets_file_not_found", path=str(path))
            sys.exit(1)
        return [
            line.strip()
            for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]

    env = os.environ.get("WALLET_TRACKING_ADDRESSES", "")
    if env:
        return [w.strip() for w in env.split(",") if w.strip()]

    settings = get_settings()
    if settings.wallet_tracking_addresses:
        return [
            w.strip()
            for w in settings.wallet_tracking_addresses.split(",")
            if w.strip()
        ]

    logger.error("no_wallets", msg="Provide --wallets, --wallets-file, or set WALLET_TRACKING_ADDRESSES")
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Wallet Tracker")
    parser.add_argument("--wallets", type=str, help="Comma-separated wallet addresses")
    parser.add_argument("--wallets-file", type=str, help="File with one address per line")
    parser.add_argument("--poll-interval", type=float, default=60.0)
    parser.add_argument("--pnl-timeframe", type=str, default="7d")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--output-dir", type=str, default=None, help="Parquet output directory")
    args = parser.parse_args()

    wallets = _parse_wallets(args)
    logger.info("wallet_tracker_init", wallets=wallets, count=len(wallets))

    settings = get_settings()
    collector = WalletMetricsCollector(port=args.port)
    collector.start()

    output_dir = args.output_dir or settings.wallet_tracking_output_dir
    store = WalletParquetStore(output_dir=output_dir)
    logger.info("wallet_store_init", output_dir=output_dir)

    fetcher = LimitlessWalletFetcher(base_url=settings.limitless_api_url)
    tracker = WalletTracker(
        wallets=wallets,
        fetchers=[fetcher],
        collector=collector,
        poll_interval=args.poll_interval,
        pnl_timeframe=args.pnl_timeframe,
        store=store,
    )

    loop = asyncio.new_event_loop()

    def _shutdown(signum: int, frame: object) -> None:
        logger.info("shutdown_signal", signal=signum)
        loop.call_soon_threadsafe(loop.create_task, tracker.stop())

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(tracker.run())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
