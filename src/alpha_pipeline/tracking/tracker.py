"""WalletTracker — batch polling loop across wallets and exchanges."""
from __future__ import annotations

import asyncio

from alpha_pipeline.tracking.collector import WalletMetricsCollector
from alpha_pipeline.tracking.limitless_fetcher import LimitlessWalletFetcher
from alpha_pipeline.tracking.protocol import WalletDataFetcher
from alpha_pipeline.tracking.storage import WalletParquetStore
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


class WalletTracker:
    """Poll wallet portfolio data at a fixed interval and export metrics."""

    def __init__(
        self,
        wallets: list[str],
        fetchers: list[WalletDataFetcher],
        collector: WalletMetricsCollector,
        poll_interval: float = 60.0,
        pnl_timeframe: str = "7d",
        store: WalletParquetStore | None = None,
    ) -> None:
        self._wallets = wallets
        self._fetchers = fetchers
        self._collector = collector
        self._poll_interval = poll_interval
        self._pnl_timeframe = pnl_timeframe
        self._store = store
        self._running = False
        self._backfilled: set[tuple[str, str]] = set()

    async def run(self) -> None:
        """Main loop: poll all wallets, observe metrics, sleep."""
        self._running = True
        logger.info(
            "wallet_tracker_started",
            wallets=self._wallets,
            exchanges=[f.exchange_id for f in self._fetchers],
            poll_interval=self._poll_interval,
        )
        while self._running:
            await self._poll_all()
            await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        """Signal the polling loop to exit."""
        self._running = False
        for fetcher in self._fetchers:
            await fetcher.close()
        if self._store:
            self._store.close()
        logger.info("wallet_tracker_stopped")

    async def _poll_all(self) -> None:
        """Poll every (wallet, fetcher) pair concurrently."""
        tasks = [
            self._fetch_and_observe(wallet, fetcher)
            for wallet in self._wallets
            for fetcher in self._fetchers
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_and_observe(
        self, wallet: str, fetcher: WalletDataFetcher,
    ) -> None:
        """Fetch a snapshot for one wallet from one exchange."""
        try:
            if isinstance(fetcher, LimitlessWalletFetcher):
                snapshot = await fetcher.fetch_snapshot(
                    wallet, self._pnl_timeframe,
                )
            else:
                # Generic path for future fetchers
                from datetime import datetime, timezone

                from alpha_pipeline.tracking.models import WalletSnapshot

                positions, pnl, volume = await asyncio.gather(
                    fetcher.fetch_positions(wallet),
                    fetcher.fetch_pnl_summary(wallet, self._pnl_timeframe),
                    fetcher.fetch_traded_volume(wallet),
                )
                snapshot = WalletSnapshot(
                    address=wallet,
                    exchange=fetcher.exchange_id,
                    polled_at=datetime.now(timezone.utc),
                    positions=positions,
                    pnl_summary=pnl,
                    traded_volume=volume,
                )

            self._collector.observe_snapshot(snapshot)

            # Persist snapshot to Parquet
            if self._store:
                self._store.write_snapshot(snapshot)

            # Backfill PnL history on first poll per (wallet, exchange)
            key = (wallet, fetcher.exchange_id)
            if self._store and key not in self._backfilled:
                self._backfilled.add(key)
                if isinstance(fetcher, LimitlessWalletFetcher):
                    points = await fetcher.fetch_pnl_history(wallet, "all")
                    if points:
                        self._store.write_pnl_history(
                            wallet, fetcher.exchange_id, points,
                        )

            logger.debug(
                "wallet_polled",
                wallet=wallet,
                exchange=fetcher.exchange_id,
                positions=len(snapshot.positions),
            )
        except Exception:
            logger.exception(
                "wallet_poll_error",
                wallet=wallet,
                exchange=fetcher.exchange_id,
            )
            self._collector.observe_error(wallet, fetcher.exchange_id)
