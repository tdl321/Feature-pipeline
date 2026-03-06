"""Protocol for wallet data fetchers — one implementation per exchange."""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from alpha_pipeline.tracking.models import PnlSummary, TradedVolume, WalletPosition


@runtime_checkable
class WalletDataFetcher(Protocol):
    """Structural interface for fetching wallet portfolio data."""

    @property
    def exchange_id(self) -> str: ...

    async def fetch_positions(self, address: str) -> list[WalletPosition]: ...

    async def fetch_pnl_summary(
        self, address: str, timeframe: str = "7d",
    ) -> PnlSummary | None: ...

    async def fetch_traded_volume(self, address: str) -> TradedVolume | None: ...

    async def close(self) -> None: ...
