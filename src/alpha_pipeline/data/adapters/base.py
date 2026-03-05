from __future__ import annotations

from typing import AsyncIterator, Protocol, runtime_checkable

from alpha_pipeline.schemas import NormalizedOrderbook, NormalizedTrade


@runtime_checkable
class ExchangeAdapter(Protocol):
    """Protocol for exchange data adapters."""

    @property
    def exchange_id(self) -> str: ...

    async def connect(self) -> None:
        """Establish connection to exchange."""
        ...

    async def disconnect(self) -> None:
        """Disconnect from exchange."""
        ...

    async def subscribe(self, market_ids: list[str]) -> None:
        """Subscribe to market data for given market IDs."""
        ...

    async def stream_orderbooks(self) -> AsyncIterator[NormalizedOrderbook]:
        """Yield normalized orderbook snapshots."""
        ...

    async def stream_trades(self) -> AsyncIterator[NormalizedTrade]:
        """Yield normalized trades."""
        ...
