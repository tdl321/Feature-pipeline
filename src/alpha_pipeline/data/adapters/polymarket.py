"""Polymarket CLOB WebSocket adapter.

Connects to the Polymarket WebSocket feed for real-time orderbook
snapshots, price changes, and trade notifications.  Messages are
parsed with *orjson* and normalised to the pipeline's canonical schemas.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import orjson
import websockets
from websockets.asyncio.client import ClientConnection

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

_PING_INTERVAL_SECONDS = 30
_QUEUE_READ_TIMEOUT = 1.0


class PolymarketAdapter:
    """Exchange adapter for the Polymarket CLOB WebSocket API.

    Lifecycle:
        1. ``connect()``  — open WS connection
        2. ``subscribe()`` — join market channels
        3. ``start_listening()`` — spawn background listener + keepalive
        4. iterate via ``stream_orderbooks()`` / ``stream_trades()``
        5. ``disconnect()`` — tear down
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._ws: ClientConnection | None = None
        self._market_ids: list[str] = []
        self._ob_queue: asyncio.Queue[NormalizedOrderbook] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[NormalizedTrade] = asyncio.Queue()
        self._running = False
        self._tasks: list[asyncio.Task[None]] = []
        # Incremental book state: {asset_id: {"bids": {price: size}, "asks": {price: size}, "market": str}}
        self._books: dict[str, dict] = {}
        # Per-asset monotonic sequence counter for message ordering
        self._sequence_counters: dict[str, int] = {}

    # -- Protocol properties / lifecycle ------------------------------------

    @property
    def exchange_id(self) -> str:
        return ExchangeId.POLYMARKET

    async def connect(self) -> None:
        """Open a WebSocket connection to Polymarket."""
        url = self._settings.polymarket_ws_url
        self._ws = await websockets.connect(url)
        self._running = True
        logger.info("polymarket_connected", url=url)

    async def disconnect(self) -> None:
        """Close the WebSocket and cancel background tasks."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        self._books.clear()
        self._sequence_counters.clear()
        if self._ws:
            await self._ws.close()
            self._ws = None
        logger.info("polymarket_disconnected")

    async def subscribe(self, market_ids: list[str]) -> None:
        """Subscribe to market channels.

        Polymarket expects the field ``assets_ids`` (their actual API name).
        """
        self._market_ids = market_ids
        if not self._ws:
            raise RuntimeError("Must call connect() before subscribe()")
        msg = orjson.dumps({
            "type": "subscribe",
            "channel": "market",
            "assets_ids": market_ids,
        })
        await self._ws.send(msg)
        logger.info("polymarket_subscribed", assets_ids=market_ids)

    # -- Background tasks ---------------------------------------------------

    async def start_listening(self) -> None:
        """Spawn the background listener and keepalive tasks."""
        self._tasks.append(asyncio.create_task(self._listen()))
        self._tasks.append(asyncio.create_task(self._keepalive()))

    async def _listen(self) -> None:
        """Read from the WS and dispatch messages to the appropriate queue."""
        if not self._ws:
            return
        try:
            async for raw in self._ws:
                if not self._running:
                    break
                try:
                    data = orjson.loads(raw)
                    # Polymarket sends arrays for initial book snapshots
                    if isinstance(data, list):
                        for item in data:
                            await self._handle_message(item)
                    else:
                        await self._handle_message(data)
                except Exception:
                    logger.exception("polymarket_message_parse_error")
        except websockets.ConnectionClosed as exc:
            logger.warning("polymarket_ws_closed", code=exc.code, reason=exc.reason)

    async def _keepalive(self) -> None:
        """Send WebSocket ping frames at a fixed interval."""
        while self._running and self._ws:
            try:
                await self._ws.ping()
                await asyncio.sleep(_PING_INTERVAL_SECONDS)
            except Exception:
                break

    def _next_sequence(self, asset_id: str) -> int:
        """Return the next monotonic sequence number for *asset_id*."""
        seq = self._sequence_counters.get(asset_id, 0) + 1
        self._sequence_counters[asset_id] = seq
        return seq

    # -- Message handling ---------------------------------------------------

    async def _handle_message(self, data: dict) -> None:
        event_type = data.get("event_type", "")

        if event_type == "book":
            await self._handle_book(data)
        elif event_type == "price_change":
            await self._handle_price_change(data)
        elif event_type in ("trade", "last_trade_price"):
            await self._handle_trade(data)

    async def _handle_book(self, data: dict) -> None:
        """Handle a full book snapshot — replaces book state entirely."""
        market_id = data.get("market", "")
        asset_id = data.get("asset_id", market_id)

        bids = {b["price"]: b["size"] for b in data.get("bids", [])}
        asks = {a["price"]: a["size"] for a in data.get("asks", [])}
        self._books[asset_id] = {"bids": bids, "asks": asks, "market": market_id}

        ts_raw = data.get("timestamp")
        exchange_ts = (
            datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
            if ts_raw
            else None
        )
        await self._emit_book(asset_id, exchange_ts)

    async def _handle_price_change(self, data: dict) -> None:
        """Handle incremental price_change — upsert/delete individual levels."""
        market_id = data.get("market", "")
        ts_raw = data.get("timestamp")
        exchange_ts = (
            datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
            if ts_raw
            else None
        )

        changed_assets: set[str] = set()
        for change in data.get("price_changes", []):
            asset_id = change.get("asset_id", market_id)
            price = change["price"]
            size = change["size"]
            side_str = change.get("side", "").upper()
            side_key = "bids" if side_str == "BUY" else "asks"

            # Initialize book if we missed the snapshot
            if asset_id not in self._books:
                self._books[asset_id] = {"bids": {}, "asks": {}, "market": market_id}

            book_side = self._books[asset_id][side_key]
            if size == "0":
                book_side.pop(price, None)
            else:
                book_side[price] = size

            changed_assets.add(asset_id)

        for asset_id in changed_assets:
            await self._emit_book(asset_id, exchange_ts)

    async def _emit_book(
        self, asset_id: str, exchange_ts: datetime | None
    ) -> None:
        """Convert accumulated book state to NormalizedOrderbook and enqueue.

        The full book state is kept in ``_books`` for correct incremental
        updates, but we truncate to the top N levels (from
        ``settings.orderbook_depth_levels``) before emitting.  This avoids
        creating ``OrderbookLevel`` objects for deep levels that are never
        used by downstream features.
        """
        book = self._books.get(asset_id)
        if book is None:
            return

        max_depth = self._settings.orderbook_depth_levels

        # Sort bids descending by price, take top N
        bids_raw = sorted(
            (
                (float(p), float(s))
                for p, s in book["bids"].items()
                if float(s) > 0
            ),
            key=lambda x: x[0],
            reverse=True,
        )[:max_depth]

        # Sort asks ascending by price, take top N
        asks_raw = sorted(
            (
                (float(p), float(s))
                for p, s in book["asks"].items()
                if float(s) > 0
            ),
            key=lambda x: x[0],
        )[:max_depth]

        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.POLYMARKET,
            market_id=book["market"],
            asset_id=asset_id,
            outcome=OutcomeType.YES,
            bids_raw=bids_raw,
            asks_raw=asks_raw,
            exchange_timestamp=exchange_ts,
            sequence_num=self._next_sequence(asset_id),
        )
        await self._ob_queue.put(ob)

    async def _handle_trade(self, data: dict) -> None:
        market_id = data.get("market", "")
        asset_id = data.get("asset_id", market_id)

        raw_side = data.get("side", "")
        side = (
            Side.BUY
            if str(raw_side).lower() in ("buy", "bid")
            else Side.SELL
        )

        trade = NormalizedTrade(
            exchange=ExchangeId.POLYMARKET,
            market_id=market_id,
            asset_id=asset_id,
            outcome=OutcomeType.YES,
            price=float(data.get("price", 0)),
            size=float(data.get("size", 0)),
            side=side,
            local_timestamp=datetime.now(timezone.utc),
            sequence_num=self._next_sequence(asset_id),
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
