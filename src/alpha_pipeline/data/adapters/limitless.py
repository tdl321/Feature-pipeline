"""Limitless exchange Socket.IO adapter.

Connects to the Limitless real-time API over Socket.IO (WebSocket
transport) with JWT authentication, subscribes to orderbook and
trade channels, and normalises events into canonical schemas.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import socketio

from alpha_pipeline.config import Settings
from alpha_pipeline.schemas import (
    ExchangeId,
    NormalizedOrderbook,
    NormalizedTrade,
    OutcomeType,
    Side,
)
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_QUEUE_READ_TIMEOUT = 1.0


class LimitlessAdapter:
    """Exchange adapter for the Limitless Socket.IO API.

    Lifecycle:
        1. ``connect()``  — establish Socket.IO connection with JWT auth
        2. ``subscribe()`` — join orderbook + trade channels per market
        3. ``start_listening()`` — no-op (Socket.IO is event-driven)
        4. iterate via ``stream_orderbooks()`` / ``stream_trades()``
        5. ``disconnect()`` — tear down
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._sio = socketio.AsyncClient()
        self._market_ids: list[str] = []
        self._ob_queue: asyncio.Queue[NormalizedOrderbook] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[NormalizedTrade] = asyncio.Queue()
        self._running = False
        self._setup_handlers()

    # -- Event handlers (registered once at init) ---------------------------

    def _setup_handlers(self) -> None:
        @self._sio.on("orderbook")
        async def _on_orderbook(data: dict) -> None:
            await self._handle_orderbook(data)

        @self._sio.on("trade")
        async def _on_trade(data: dict) -> None:
            await self._handle_trade(data)

        @self._sio.event
        async def connect() -> None:
            logger.info("limitless_sio_connected")

        @self._sio.event
        async def disconnect() -> None:
            logger.info("limitless_sio_disconnected")

    # -- Protocol properties / lifecycle ------------------------------------

    @property
    def exchange_id(self) -> str:
        return ExchangeId.LIMITLESS

    async def connect(self) -> None:
        """Establish a Socket.IO connection, using JWT if configured."""
        auth: dict[str, str] = {}
        if self._settings.limitless_jwt_token:
            auth["token"] = self._settings.limitless_jwt_token
        await self._sio.connect(
            self._settings.limitless_ws_url,
            auth=auth,
            transports=["websocket"],
        )
        self._running = True
        logger.info("limitless_connected", url=self._settings.limitless_ws_url)

    async def disconnect(self) -> None:
        """Disconnect from the Socket.IO server."""
        self._running = False
        await self._sio.disconnect()
        logger.info("limitless_disconnected")

    async def subscribe(self, market_ids: list[str]) -> None:
        """Subscribe to orderbook and trade channels for each market."""
        self._market_ids = market_ids
        for market_id in market_ids:
            await self._sio.emit(
                "subscribe",
                {"market_id": market_id, "channels": ["orderbook", "trade"]},
            )
        logger.info("limitless_subscribed", market_ids=market_ids)

    # -- Background tasks ---------------------------------------------------

    async def start_listening(self) -> None:
        """No-op: Socket.IO dispatches events internally via handlers."""

    # -- Message handling ---------------------------------------------------

    async def _handle_orderbook(self, data: dict) -> None:
        market_id = data.get("market_id", "")
        asset_id = data.get("asset_id", market_id)
        outcome = _parse_outcome(data.get("outcome", "yes"))

        bids_raw = [
            (float(b[0]), float(b[1])) for b in data.get("bids", [])
        ]
        asks_raw = [
            (float(a[0]), float(a[1])) for a in data.get("asks", [])
        ]

        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.LIMITLESS,
            market_id=market_id,
            asset_id=asset_id,
            outcome=outcome,
            bids_raw=bids_raw,
            asks_raw=asks_raw,
        )
        await self._ob_queue.put(ob)

    async def _handle_trade(self, data: dict) -> None:
        market_id = data.get("market_id", "")
        asset_id = data.get("asset_id", market_id)
        outcome = _parse_outcome(data.get("outcome", "yes"))

        side = (
            Side.BUY
            if str(data.get("side", "")).lower() == "buy"
            else Side.SELL
        )

        trade = NormalizedTrade(
            exchange=ExchangeId.LIMITLESS,
            market_id=market_id,
            asset_id=asset_id,
            outcome=outcome,
            price=float(data.get("price", 0)),
            size=float(data.get("size", 0)),
            side=side,
            local_timestamp=datetime.now(timezone.utc),
        )
        await self._trade_queue.put(trade)

    # -- Streaming iterators ------------------------------------------------

    async def stream_orderbooks(self) -> AsyncIterator[NormalizedOrderbook]:
        """Yield normalised orderbook snapshots as they arrive."""
        while self._running:
            try:
                ob = await asyncio.wait_for(
                    self._ob_queue.get(), timeout=_QUEUE_READ_TIMEOUT,
                )
                yield ob
            except asyncio.TimeoutError:
                continue

    async def stream_trades(self) -> AsyncIterator[NormalizedTrade]:
        """Yield normalised trades as they arrive."""
        while self._running:
            try:
                trade = await asyncio.wait_for(
                    self._trade_queue.get(), timeout=_QUEUE_READ_TIMEOUT,
                )
                yield trade
            except asyncio.TimeoutError:
                continue


# -- Helpers ----------------------------------------------------------------

def _parse_outcome(value: str) -> OutcomeType:
    """Convert a raw outcome string to ``OutcomeType``."""
    return OutcomeType.YES if value.lower() == "yes" else OutcomeType.NO
