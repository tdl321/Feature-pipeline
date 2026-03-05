"""DataManager — orchestrator connecting exchange adapters to buffers.

Runs background tasks that consume normalised orderbooks and trades
from each adapter, store records in ``TimeSeriesBuffer`` instances,
and publish lightweight events onto a shared ``asyncio.Queue``.
"""
from __future__ import annotations

import asyncio
import time
from typing import Any

from alpha_pipeline.config import Settings
from alpha_pipeline.data.buffer import TimeSeriesBuffer
from alpha_pipeline.schemas import DataEventType, NormalizedOrderbook, NormalizedTrade
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_EVICTION_INTERVAL_SECONDS = 60


class DataManager:
    """Connects exchange adapters to time-series buffers and an event queue.

    Usage::

        dm = DataManager(settings)
        dm.add_adapter(polymarket_adapter)
        dm.add_adapter(opinion_adapter)
        await dm.start()
        ...
        await dm.stop()
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._adapters: list[Any] = []  # ExchangeAdapter instances
        self._orderbook_buffers: dict[str, TimeSeriesBuffer] = {}
        self._trade_buffers: dict[str, TimeSeriesBuffer] = {}
        self._event_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []

    # -- Adapter registration -----------------------------------------------

    def add_adapter(self, adapter: Any) -> None:
        """Register an exchange adapter to be managed."""
        self._adapters.append(adapter)

    # -- Buffer access ------------------------------------------------------

    def get_orderbook_buffer(self, market_id: str) -> TimeSeriesBuffer:
        """Return (or lazily create) the orderbook buffer for *market_id*."""
        if market_id not in self._orderbook_buffers:
            self._orderbook_buffers[market_id] = TimeSeriesBuffer(
                max_rows=self._settings.buffer_max_rows,
                ttl_seconds=self._settings.buffer_ttl_seconds,
            )
        return self._orderbook_buffers[market_id]

    def get_trade_buffer(self, market_id: str) -> TimeSeriesBuffer:
        """Return (or lazily create) the trade buffer for *market_id*."""
        if market_id not in self._trade_buffers:
            self._trade_buffers[market_id] = TimeSeriesBuffer(
                max_rows=self._settings.buffer_max_rows,
                ttl_seconds=self._settings.buffer_ttl_seconds,
            )
        return self._trade_buffers[market_id]

    @property
    def orderbook_buffers(self) -> dict[str, TimeSeriesBuffer]:
        return self._orderbook_buffers

    @property
    def trade_buffers(self) -> dict[str, TimeSeriesBuffer]:
        return self._trade_buffers

    @property
    def event_queue(self) -> asyncio.Queue[dict[str, Any]]:
        return self._event_queue

    # -- Lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """Connect all adapters and launch consumption tasks."""
        self._running = True
        for adapter in self._adapters:
            await adapter.connect()
            await adapter.start_listening()
            self._tasks.append(
                asyncio.create_task(self._consume_orderbooks(adapter))
            )
            self._tasks.append(
                asyncio.create_task(self._consume_trades(adapter))
            )
        self._tasks.append(asyncio.create_task(self._eviction_loop()))
        logger.info("data_manager_started", adapter_count=len(self._adapters))

    async def stop(self) -> None:
        """Disconnect all adapters and cancel background tasks."""
        self._running = False
        for adapter in self._adapters:
            await adapter.disconnect()
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        logger.info("data_manager_stopped")

    # -- Consumption loops --------------------------------------------------

    async def _consume_orderbooks(self, adapter: Any) -> None:
        """Read from an adapter's orderbook stream and buffer records."""
        max_depth = self._settings.orderbook_depth_levels
        async for ob in adapter.stream_orderbooks():
            if not self._running:
                break
            buf = self.get_orderbook_buffer(ob.market_id)
            record = _orderbook_to_record(ob, max_depth=max_depth)
            buf.append(record)
            await self._event_queue.put({
                "type": DataEventType.ORDERBOOK_SNAPSHOT,
                "exchange": ob.exchange,
                "market_id": ob.market_id,
                "timestamp": record["timestamp"],
            })

    async def _consume_trades(self, adapter: Any) -> None:
        """Read from an adapter's trade stream and buffer records."""
        async for trade in adapter.stream_trades():
            if not self._running:
                break
            buf = self.get_trade_buffer(trade.market_id)
            record = _trade_to_record(trade)
            buf.append(record)
            await self._event_queue.put({
                "type": DataEventType.TRADE,
                "exchange": trade.exchange,
                "market_id": trade.market_id,
                "timestamp": record["timestamp"],
            })

    async def _eviction_loop(self) -> None:
        """Periodically evict expired records from all buffers."""
        while self._running:
            all_buffers = (
                list(self._orderbook_buffers.values())
                + list(self._trade_buffers.values())
            )
            for buf in all_buffers:
                buf.evict_expired()
            await asyncio.sleep(_EVICTION_INTERVAL_SECONDS)


# -- Record builders (pure functions) ---------------------------------------

def _orderbook_to_record(
    ob: NormalizedOrderbook, *, max_depth: int = 10
) -> dict[str, Any]:
    """Flatten a ``NormalizedOrderbook`` into a dict suitable for buffering.

    ``max_depth`` controls how many top levels are included in the
    ``top_n_bid_depth`` / ``top_n_ask_depth`` aggregate fields.  The full
    ``bid_depth`` / ``ask_depth`` fields sum *all* levels in the book for
    backward compatibility.
    """
    top_bids = ob.bids[:max_depth]
    top_asks = ob.asks[:max_depth]
    return {
        "timestamp": time.time(),
        "exchange": ob.exchange,
        "market_id": ob.market_id,
        "asset_id": ob.asset_id,
        "outcome": ob.outcome,
        "best_bid": ob.best_bid,
        "best_ask": ob.best_ask,
        "mid_price": ob.mid_price,
        "spread": ob.spread,
        "bid_depth": sum(level.size for level in ob.bids),
        "ask_depth": sum(level.size for level in ob.asks),
        "top_n_bid_depth": sum(level.size for level in top_bids),
        "top_n_ask_depth": sum(level.size for level in top_asks),
        "n_bid_levels": len(ob.bids),
        "n_ask_levels": len(ob.asks),
        "best_bid_size": ob.bids[0].size if ob.bids else 0.0,
        "best_ask_size": ob.asks[0].size if ob.asks else 0.0,
    }


def _trade_to_record(trade: NormalizedTrade) -> dict[str, Any]:
    """Flatten a ``NormalizedTrade`` into a dict suitable for buffering."""
    return {
        "timestamp": time.time(),
        "exchange": trade.exchange,
        "market_id": trade.market_id,
        "asset_id": trade.asset_id,
        "outcome": trade.outcome,
        "price": trade.price,
        "size": trade.size,
        "side": trade.side,
        "usd_notional": trade.usd_notional,
    }
