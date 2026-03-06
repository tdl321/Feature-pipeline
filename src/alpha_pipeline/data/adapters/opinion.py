"""Opinion exchange adapter with WebSocket primary + REST polling fallback.

Connects via plain WebSocket (``wss://ws.opinion.trade``) for real-time
orderbook diffs and trade events.  Falls back to REST polling when the
WebSocket connection cannot be established.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import AsyncIterator

import aiohttp
import orjson

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
_HEARTBEAT_INTERVAL_SECONDS = 25
_WS_RECONNECT_DELAY_SECONDS = 5

# Opinion outcomeSide mapping: 1=YES, 2=NO
_OUTCOME_SIDE_MAP: dict[int, OutcomeType] = {
    1: OutcomeType.YES,
    2: OutcomeType.NO,
}

# Opinion trade side mapping
_TRADE_SIDE_MAP: dict[str, Side] = {
    "Buy": Side.BUY,
    "Sell": Side.SELL,
}


class OpinionAdapter:
    """Exchange adapter for Opinion using WebSocket + REST fallback.

    Lifecycle:
        1. ``connect()``  — create HTTP session (with optional API key)
        2. ``subscribe()`` — fetch REST snapshots, store market IDs
        3. ``start_listening()`` — spawn WS listener (or REST fallback)
        4. iterate via ``stream_orderbooks()`` / ``stream_trades()``
        5. ``disconnect()`` — close session and WS
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session: aiohttp.ClientSession | None = None
        self._market_ids: list[str] = []
        self._ob_queue: asyncio.Queue[NormalizedOrderbook] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[NormalizedTrade] = asyncio.Queue()
        self._running = False
        self._last_books: dict[str, NormalizedOrderbook] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._sequence_counters: dict[str, int] = {}

        # Internal orderbook state for applying WS deltas
        # Structure: {market_id: {"bids": {price: size}, "asks": {price: size}}}
        self._books: dict[str, dict[str, dict[float, float]]] = {}

    # -- Protocol properties / lifecycle ------------------------------------

    @property
    def exchange_id(self) -> str:
        return ExchangeId.OPINION

    async def connect(self) -> None:
        """Create an ``aiohttp`` session, injecting API key if configured."""
        headers: dict[str, str] = {}
        if self._settings.opinion_api_key:
            headers["Authorization"] = f"Bearer {self._settings.opinion_api_key}"
        self._session = aiohttp.ClientSession(headers=headers)
        self._running = True
        logger.info(
            "opinion_connected",
            base_url=self._settings.opinion_api_url,
            ws_url=self._settings.opinion_ws_url,
        )

    async def disconnect(self) -> None:
        """Stop listeners and close the HTTP session."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("opinion_disconnected")

    async def subscribe(self, market_ids: list[str]) -> None:
        """Register market IDs and fetch initial REST snapshots.

        The REST snapshot seeds the internal book state before WS deltas
        are applied.
        """
        self._market_ids = list(market_ids)

        for market_id in self._market_ids:
            try:
                await self._fetch_orderbook(market_id)
            except Exception:
                logger.exception(
                    "opinion_rest_snapshot_error", market_id=market_id,
                )

        logger.info("opinion_subscribed", market_ids=self._market_ids)

    # -- Background tasks ---------------------------------------------------

    async def start_listening(self) -> None:
        """Start the WebSocket listener (falls back to REST polling)."""
        self._tasks.append(asyncio.create_task(self._ws_listen()))

    # -- WebSocket listener -------------------------------------------------

    async def _ws_listen(self) -> None:
        """Connect to Opinion WS and listen for orderbook diffs and trades.

        Uses aiohttp native WebSocket (Opinion uses plain WS, not Socket.IO).
        Automatically reconnects on disconnection.
        """
        if not self._session:
            return

        ws_url = self._settings.opinion_ws_url
        api_key = self._settings.opinion_api_key
        url = f"{ws_url}?apikey={api_key}" if api_key else ws_url

        while self._running:
            try:
                async with self._session.ws_connect(url) as ws:
                    logger.info("opinion_ws_connected", url=ws_url)

                    # Subscribe to channels for each market
                    for market_id in self._market_ids:
                        await self._ws_subscribe(ws, market_id)

                    # Spawn heartbeat task
                    heartbeat_task = asyncio.create_task(
                        self._heartbeat_loop(ws),
                    )

                    try:
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_ws_message(msg.data)
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                break
                    finally:
                        heartbeat_task.cancel()
                        try:
                            await heartbeat_task
                        except asyncio.CancelledError:
                            pass

            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("opinion_ws_error")

            if self._running:
                logger.info(
                    "opinion_ws_reconnecting",
                    delay=_WS_RECONNECT_DELAY_SECONDS,
                )
                await asyncio.sleep(_WS_RECONNECT_DELAY_SECONDS)

    async def _ws_subscribe(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        market_id: str,
    ) -> None:
        """Send subscription messages for a market's channels."""
        for channel in ("market.depth.diff", "market.last.trade"):
            payload = orjson.dumps({
                "action": "SUBSCRIBE",
                "channel": channel,
                "marketId": int(market_id) if market_id.isdigit() else market_id,
            })
            await ws.send_bytes(payload)
            logger.debug(
                "opinion_ws_subscribed",
                channel=channel,
                market_id=market_id,
            )

    async def _heartbeat_loop(
        self,
        ws: aiohttp.ClientWebSocketResponse,
    ) -> None:
        """Send periodic heartbeat to keep the WS connection alive."""
        while True:
            try:
                await ws.send_str(orjson.dumps({"action": "HEARTBEAT"}).decode())
                await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("opinion_heartbeat_error")
                break

    async def _handle_ws_message(self, raw: str) -> None:
        """Route an incoming WS message to the appropriate handler."""
        try:
            data = orjson.loads(raw)
        except Exception:
            logger.warning("opinion_ws_invalid_json", raw=raw[:200])
            return

        msg_type = data.get("msgType", "")

        if msg_type == "market.depth.diff":
            await self._handle_depth_diff(data)
        elif msg_type == "market.last.trade":
            await self._handle_trade(data)
        elif msg_type == "HEARTBEAT":
            logger.debug("opinion_heartbeat_ack")
        else:
            logger.debug("opinion_ws_unknown_msg", msg_type=msg_type)

    # -- Depth diff handler -------------------------------------------------

    async def _handle_depth_diff(self, data: dict) -> None:
        """Apply an orderbook depth diff and emit updated snapshot.

        Opinion sends incremental updates with bids/asks arrays containing
        {price, size, outcomeSide} objects.  size=0 means remove the level.
        """
        market_id = str(data.get("marketId", ""))
        if not market_id:
            return

        outcome_side = data.get("outcomeSide", 1)
        outcome = _OUTCOME_SIDE_MAP.get(outcome_side, OutcomeType.YES)

        # Ensure we have a book for this market
        if market_id not in self._books:
            self._books[market_id] = {"bids": {}, "asks": {}}

        book = self._books[market_id]

        # Apply bid deltas
        for entry in data.get("bids", []):
            price = float(entry["price"])
            size = float(entry["size"])
            if size == 0:
                book["bids"].pop(price, None)
            else:
                book["bids"][price] = size

        # Apply ask deltas
        for entry in data.get("asks", []):
            price = float(entry["price"])
            size = float(entry["size"])
            if size == 0:
                book["asks"].pop(price, None)
            else:
                book["asks"][price] = size

        # Emit full snapshot from internal state
        await self._emit_book(market_id, outcome)

    async def _emit_book(
        self,
        market_id: str,
        outcome: OutcomeType = OutcomeType.YES,
    ) -> None:
        """Build and enqueue a NormalizedOrderbook from internal state."""
        book = self._books.get(market_id, {"bids": {}, "asks": {}})
        max_depth = self._settings.orderbook_depth_levels

        # Sort bids descending, asks ascending
        bids_raw = sorted(book["bids"].items(), key=lambda x: x[0], reverse=True)
        asks_raw = sorted(book["asks"].items(), key=lambda x: x[0])

        try:
            ob = NormalizedOrderbook.from_raw(
                exchange=ExchangeId.OPINION,
                market_id=market_id,
                asset_id=market_id,
                outcome=outcome,
                bids_raw=bids_raw[:max_depth],
                asks_raw=asks_raw[:max_depth],
                sequence_num=self._next_sequence(market_id),
            )
        except Exception:
            logger.exception("opinion_emit_book_error", market_id=market_id)
            return

        # Detect synthetic trades from book changes (fallback enrichment)
        prev = self._last_books.get(market_id)
        if prev is not None:
            self._detect_synthetic_trades(prev, ob, market_id)
        self._last_books[market_id] = ob

        await self._ob_queue.put(ob)

    # -- Real trade handler -------------------------------------------------

    async def _handle_trade(self, data: dict) -> None:
        """Normalize a real trade event from the WS stream.

        Opinion trade payload fields:
        - marketId: int
        - outcomeSide: 1=YES, 2=NO
        - side: "Buy", "Sell", "Split", "Merge"
        - price: float (0-1)
        - shares: float (actual size!)
        - amount: float (USD notional)
        """
        market_id = str(data.get("marketId", ""))
        if not market_id:
            return

        side_str = data.get("side", "")
        side = _TRADE_SIDE_MAP.get(side_str)
        if side is None:
            # Skip Split/Merge and unknown sides
            logger.debug(
                "opinion_trade_skipped",
                side=side_str,
                market_id=market_id,
            )
            return

        outcome_side = data.get("outcomeSide", 1)
        outcome = _OUTCOME_SIDE_MAP.get(outcome_side, OutcomeType.YES)

        price = float(data.get("price", 0))
        size = float(data.get("shares", 0))
        amount = data.get("amount")
        usd_notional = float(amount) if amount is not None else None

        now = datetime.now(timezone.utc)

        trade = NormalizedTrade(
            exchange=ExchangeId.OPINION,
            market_id=market_id,
            asset_id=market_id,
            outcome=outcome,
            price=price,
            size=size,
            side=side,
            usd_notional=usd_notional,
            local_timestamp=now,
        )

        self._trade_queue.put_nowait(trade)

    # -- REST fallback (poll loop) ------------------------------------------

    async def _poll_loop(self) -> None:
        """Continuously poll each subscribed market at the configured cadence."""
        while self._running and self._session:
            for market_id in self._market_ids:
                try:
                    await self._fetch_orderbook(market_id)
                except Exception:
                    logger.exception(
                        "opinion_poll_error", market_id=market_id,
                    )
            await asyncio.sleep(self._settings.opinion_poll_interval_seconds)

    # -- REST orderbook fetch + synthetic trade detection --------------------

    async def _fetch_orderbook(self, market_id: str) -> None:
        """Fetch orderbook snapshot via REST API and seed internal state."""
        if not self._session:
            return

        url = (
            f"{self._settings.opinion_api_url}"
            f"/v1/markets/{market_id}/orderbook"
        )
        async with self._session.get(url) as resp:
            if resp.status != 200:
                logger.warning(
                    "opinion_http_error",
                    market_id=market_id,
                    status=resp.status,
                )
                return
            raw = await resp.read()
            data = orjson.loads(raw)

        # Seed internal book state from REST snapshot
        self._books[market_id] = {
            "bids": {
                float(b["price"]): float(b["size"])
                for b in data.get("bids", [])
            },
            "asks": {
                float(a["price"]): float(a["size"])
                for a in data.get("asks", [])
            },
        }

        bids_raw = [
            (float(b["price"]), float(b["size"]))
            for b in data.get("bids", [])
        ]
        asks_raw = [
            (float(a["price"]), float(a["size"]))
            for a in data.get("asks", [])
        ]

        ob = NormalizedOrderbook.from_raw(
            exchange=ExchangeId.OPINION,
            market_id=market_id,
            asset_id=market_id,
            outcome=OutcomeType.YES,
            bids_raw=bids_raw,
            asks_raw=asks_raw,
        )

        # Detect synthetic trades by diffing consecutive snapshots
        prev = self._last_books.get(market_id)
        if prev is not None:
            self._detect_synthetic_trades(prev, ob, market_id)
        self._last_books[market_id] = ob

        await self._ob_queue.put(ob)

    def _detect_synthetic_trades(
        self,
        prev: NormalizedOrderbook,
        curr: NormalizedOrderbook,
        market_id: str,
    ) -> None:
        """Infer trades from changes between two consecutive snapshots.

        Heuristic: if the current best bid >= previous best ask, a buy
        likely occurred (and vice-versa for sells).  Size is unknown from
        the diff alone, so it is set to ``0.0``.
        """
        now = datetime.now(timezone.utc)

        # Likely buy: current best bid crossed or matched previous best ask
        if prev.best_ask is not None and curr.best_bid is not None:
            if curr.best_bid >= prev.best_ask:
                trade = NormalizedTrade(
                    exchange=ExchangeId.OPINION,
                    market_id=market_id,
                    asset_id=market_id,
                    outcome=OutcomeType.YES,
                    price=prev.best_ask,
                    size=0.0,
                    side=Side.BUY,
                    local_timestamp=now,
                )
                self._trade_queue.put_nowait(trade)

        # Likely sell: current best ask dropped to or below previous best bid
        if prev.best_bid is not None and curr.best_ask is not None:
            if curr.best_ask <= prev.best_bid:
                trade = NormalizedTrade(
                    exchange=ExchangeId.OPINION,
                    market_id=market_id,
                    asset_id=market_id,
                    outcome=OutcomeType.YES,
                    price=prev.best_bid,
                    size=0.0,
                    side=Side.SELL,
                    local_timestamp=now,
                )
                self._trade_queue.put_nowait(trade)

    # -- Helpers ------------------------------------------------------------

    def _next_sequence(self, market_id: str) -> int:
        """Return the next monotonic sequence number for a market."""
        seq = self._sequence_counters.get(market_id, 0) + 1
        self._sequence_counters[market_id] = seq
        return seq

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
