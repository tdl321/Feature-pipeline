"""Limitless exchange adapter using the official limitless-sdk.

Connects via SDK WebSocketClient for real-time ``orderbookUpdate`` events
and uses HttpClient + MarketFetcher for REST orderbook snapshots.
Trades are synthesised from consecutive orderbook diffs (same pattern
as OpinionAdapter) since Limitless has no native trade event for CLOB.
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


class LimitlessAdapter:
    """Exchange adapter for Limitless using SDK WebSocket + REST.

    Lifecycle:
        1. ``connect()``  — create HTTP session for REST calls
        2. ``subscribe()`` — fetch REST snapshots, then subscribe WS
        3. ``start_listening()`` — spawn WS connection as background task
        4. iterate via ``stream_orderbooks()`` / ``stream_trades()``
        5. ``disconnect()`` — tear down
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._market_slugs: list[str] = []
        self._ob_queue: asyncio.Queue[NormalizedOrderbook] = asyncio.Queue()
        self._trade_queue: asyncio.Queue[NormalizedTrade] = asyncio.Queue()
        self._running = False
        self._last_books: dict[str, NormalizedOrderbook] = {}
        self._sequence_counters: dict[str, int] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._session: aiohttp.ClientSession | None = None
        self._ws_client: object | None = None
        self._sio: object | None = None  # Socket.IO client for resubscribe

    # -- Protocol properties -----------------------------------------------

    @property
    def exchange_id(self) -> str:
        return ExchangeId.LIMITLESS

    # -- Lifecycle ----------------------------------------------------------

    async def connect(self) -> None:
        """Create HTTP session for REST orderbook fetches."""
        headers: dict[str, str] = {}
        if self._settings.limitless_api_key:
            headers["X-API-Key"] = self._settings.limitless_api_key
        self._session = aiohttp.ClientSession(
            base_url=self._settings.limitless_api_url,
            headers=headers,
        )
        self._running = True
        logger.info(
            "limitless_connected",
            api_url=self._settings.limitless_api_url,
            ws_url=self._settings.limitless_ws_url,
        )

    async def disconnect(self) -> None:
        """Tear down WS connection and HTTP session."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("limitless_disconnected")

    async def subscribe(self, market_ids: list[str]) -> None:
        """Subscribe to market slugs.

        Fetches an initial REST snapshot for each slug, then sets up
        the WS subscription for real-time updates.
        """
        self._market_slugs = list(market_ids)

        # Fetch initial REST snapshots
        for slug in self._market_slugs:
            try:
                await self._fetch_rest_orderbook(slug)
            except Exception:
                logger.exception("limitless_rest_snapshot_error", slug=slug)

        logger.info("limitless_subscribed", slugs=self._market_slugs)

    async def start_listening(self) -> None:
        """Spawn the WebSocket listener as a background task."""
        self._tasks.append(asyncio.create_task(self._ws_listen()))

    # -- Hot-swap subscriptions ---------------------------------------------

    async def resubscribe(self, slugs: list[str]) -> None:
        """Hot-swap market subscriptions for roster rollover.

        Computes added/removed vs current slugs, fetches REST snapshots
        for new markets, cleans up state for removed ones, and re-emits
        the Socket.IO subscription with the full new list.
        """
        old_set = set(self._market_slugs)
        new_set = set(slugs)
        added = new_set - old_set
        removed = old_set - new_set

        self._market_slugs = list(slugs)

        # Fetch REST snapshots for newly added markets
        for slug in sorted(added):
            try:
                await self._fetch_rest_orderbook(slug)
                logger.info("roster_market_added", slug=slug)
            except Exception:
                logger.exception("roster_snapshot_error", slug=slug)

        # Clean up internal state for removed markets
        for slug in sorted(removed):
            for key in [f"{slug}:yes", f"{slug}:no"]:
                self._last_books.pop(key, None)
            self._sequence_counters.pop(slug, None)
            logger.info("roster_market_removed", slug=slug)

        # Re-emit WS subscription with full new list
        if self._sio is not None:
            try:
                await self._sio.emit(
                    "subscribe_market_prices",
                    {"marketSlugs": self._market_slugs},
                    namespace="/markets",
                )
                logger.info(
                    "roster_ws_resubscribed", slugs=self._market_slugs,
                )
            except Exception:
                logger.exception("roster_ws_resubscribe_error")

    # -- REST orderbook fetch -----------------------------------------------

    async def _fetch_rest_orderbook(self, slug: str) -> None:
        """Fetch orderbook snapshot via REST API and emit YES + derived NO."""
        if not self._session:
            return

        url = f"/markets/{slug}/orderbook"
        async with self._session.get(url) as resp:
            if resp.status != 200:
                logger.warning(
                    "limitless_rest_error", slug=slug, status=resp.status,
                )
                return
            raw = await resp.read()
            data = orjson.loads(raw)

        # YES orderbook (direct from API)
        yes_ob = self._normalize_orderbook(slug, data, outcome=OutcomeType.YES)
        if yes_ob is not None:
            yes_key = f"{slug}:yes"
            prev = self._last_books.get(yes_key)
            if prev is not None:
                self._detect_synthetic_trades(prev, yes_ob, slug, OutcomeType.YES)
            self._last_books[yes_key] = yes_ob
            await self._ob_queue.put(yes_ob)

        # NO orderbook (derived: NO_bid = 1 - YES_ask, NO_ask = 1 - YES_bid)
        no_ob = self._derive_no_orderbook(slug, data)
        if no_ob is not None:
            no_key = f"{slug}:no"
            prev = self._last_books.get(no_key)
            if prev is not None:
                self._detect_synthetic_trades(prev, no_ob, slug, OutcomeType.NO)
            self._last_books[no_key] = no_ob
            await self._ob_queue.put(no_ob)

    # -- WebSocket listener -------------------------------------------------

    async def _ws_listen(self) -> None:
        """Connect to Limitless WS (Socket.IO) and listen for events.

        Uses python-socketio directly (the SDK's WebSocketClient wraps
        this same transport) for maximum control over the event loop.
        """
        try:
            import socketio
        except ImportError:
            logger.error(
                "limitless_ws_socketio_missing",
                msg="Install python-socketio[asyncio] for WS support; "
                    "falling back to REST-only polling",
            )
            await self._poll_loop()
            return

        sio = socketio.AsyncClient()
        self._sio = sio

        @sio.on("orderbookUpdate", namespace="/markets")
        async def _on_orderbook_update(data: dict) -> None:
            await self._handle_orderbook_update(data)

        @sio.on("newPriceData", namespace="/markets")
        async def _on_price_data(data: dict) -> None:
            # AMM markets — log and skip (no orderbook depth)
            logger.debug("limitless_amm_price", data=data)

        @sio.event(namespace="/markets")
        async def connect() -> None:
            logger.info("limitless_ws_connected")
            # Subscribe to all markets in a single call (subs replace)
            if self._market_slugs:
                await sio.emit(
                    "subscribe_market_prices",
                    {"marketSlugs": self._market_slugs},
                    namespace="/markets",
                )
                logger.info(
                    "limitless_ws_subscribed", slugs=self._market_slugs,
                )

        @sio.event(namespace="/markets")
        async def disconnect() -> None:
            logger.info("limitless_ws_disconnected")

        headers: dict[str, str] = {}
        if self._settings.limitless_api_key:
            headers["X-API-Key"] = self._settings.limitless_api_key

        try:
            await sio.connect(
                self._settings.limitless_ws_url,
                transports=["websocket"],
                namespaces=["/markets"],
                headers=headers,
            )
            # Block until disconnected or adapter stopped
            while self._running and sio.connected:
                await asyncio.sleep(1.0)
        except Exception:
            logger.exception("limitless_ws_error")
        finally:
            if sio.connected:
                await sio.disconnect()

    async def _poll_loop(self) -> None:
        """Fallback: poll REST API if WebSocket is unavailable."""
        while self._running:
            for slug in self._market_slugs:
                try:
                    await self._fetch_rest_orderbook(slug)
                except Exception:
                    logger.exception("limitless_poll_error", slug=slug)
            await asyncio.sleep(2.0)

    # -- Event handling -----------------------------------------------------

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle an ``orderbookUpdate`` WS event — emit YES + derived NO."""
        slug = data.get("marketSlug", data.get("slug", ""))
        if not slug:
            logger.warning("limitless_ws_no_slug", data=data)
            return

        # YES orderbook (direct from WS)
        yes_ob = self._normalize_orderbook(slug, data, outcome=OutcomeType.YES)
        if yes_ob is not None:
            yes_key = f"{slug}:yes"
            prev = self._last_books.get(yes_key)
            if prev is not None:
                self._detect_synthetic_trades(prev, yes_ob, slug, OutcomeType.YES)
            self._last_books[yes_key] = yes_ob
            await self._ob_queue.put(yes_ob)

        # NO orderbook (derived from YES)
        no_ob = self._derive_no_orderbook(slug, data)
        if no_ob is not None:
            no_key = f"{slug}:no"
            prev = self._last_books.get(no_key)
            if prev is not None:
                self._detect_synthetic_trades(prev, no_ob, slug, OutcomeType.NO)
            self._last_books[no_key] = no_ob
            await self._ob_queue.put(no_ob)

    # -- Normalization ------------------------------------------------------

    def _normalize_orderbook(
        self,
        slug: str,
        data: dict,
        outcome: OutcomeType = OutcomeType.YES,
    ) -> NormalizedOrderbook | None:
        """Convert raw orderbook data to NormalizedOrderbook.

        Handles both REST and WS payloads. Limitless sends levels as
        ``{"price": float, "size": float}`` objects (not tuples).
        """
        orderbook = data.get("orderbook", data)
        raw_bids = orderbook.get("bids", [])
        raw_asks = orderbook.get("asks", [])

        bids_raw: list[tuple[float, float]] = []
        for b in raw_bids:
            if isinstance(b, dict):
                bids_raw.append((float(b["price"]), float(b["size"])))
            else:
                bids_raw.append((float(b[0]), float(b[1])))

        asks_raw: list[tuple[float, float]] = []
        for a in raw_asks:
            if isinstance(a, dict):
                asks_raw.append((float(a["price"]), float(a["size"])))
            else:
                asks_raw.append((float(a[0]), float(a[1])))

        # Depth truncation
        max_depth = self._settings.orderbook_depth_levels

        exchange_ts = _parse_timestamp(data.get("timestamp"))

        try:
            ob = NormalizedOrderbook.from_raw(
                exchange=ExchangeId.LIMITLESS,
                market_id=slug,
                asset_id=slug,
                outcome=outcome,
                bids_raw=bids_raw[:max_depth],
                asks_raw=asks_raw[:max_depth],
                exchange_timestamp=exchange_ts,
                sequence_num=self._next_sequence(slug),
            )
        except Exception:
            logger.exception("limitless_normalize_error", slug=slug, outcome=outcome)
            return None

        return ob

    def _derive_no_orderbook(
        self, slug: str, data: dict,
    ) -> NormalizedOrderbook | None:
        """Derive a NO-side orderbook from YES data.

        In a binary prediction market:
        - NO bids = 1 - YES asks (someone willing to buy NO at 1-ask)
        - NO asks = 1 - YES bids (someone willing to sell NO at 1-bid)
        """
        orderbook = data.get("orderbook", data)
        raw_bids = orderbook.get("bids", [])
        raw_asks = orderbook.get("asks", [])

        def _parse_level(level: dict | list) -> tuple[float, float]:
            if isinstance(level, dict):
                return (float(level["price"]), float(level["size"]))
            return (float(level[0]), float(level[1]))

        # NO bids from YES asks: price = 1 - yes_ask_price
        no_bids: list[tuple[float, float]] = []
        for a in raw_asks:
            price, size = _parse_level(a)
            no_price = round(1.0 - price, 6)
            if 0.0 < no_price < 1.0:
                no_bids.append((no_price, size))

        # NO asks from YES bids: price = 1 - yes_bid_price
        no_asks: list[tuple[float, float]] = []
        for b in raw_bids:
            price, size = _parse_level(b)
            no_price = round(1.0 - price, 6)
            if 0.0 < no_price < 1.0:
                no_asks.append((no_price, size))

        if not no_bids and not no_asks:
            return None

        max_depth = self._settings.orderbook_depth_levels
        exchange_ts = _parse_timestamp(data.get("timestamp"))

        try:
            ob = NormalizedOrderbook.from_raw(
                exchange=ExchangeId.LIMITLESS,
                market_id=slug,
                asset_id=slug,
                outcome=OutcomeType.NO,
                bids_raw=no_bids[:max_depth],
                asks_raw=no_asks[:max_depth],
                exchange_timestamp=exchange_ts,
                sequence_num=self._next_sequence(slug),
            )
        except Exception:
            logger.exception("limitless_no_derive_error", slug=slug)
            return None

        return ob

    # -- Synthetic trade detection (from OpinionAdapter) --------------------

    def _detect_synthetic_trades(
        self,
        prev: NormalizedOrderbook,
        curr: NormalizedOrderbook,
        slug: str,
        outcome: OutcomeType = OutcomeType.YES,
    ) -> None:
        """Infer trades from changes between two consecutive snapshots.

        Heuristic: if the current best bid >= previous best ask, a buy
        likely occurred (and vice-versa for sells).  Size is unknown from
        the diff alone, so it is set to 0.0.
        """
        now = datetime.now(timezone.utc)

        # Likely buy: current best bid crossed or matched previous best ask
        if prev.best_ask is not None and curr.best_bid is not None:
            if curr.best_bid >= prev.best_ask:
                trade = NormalizedTrade(
                    exchange=ExchangeId.LIMITLESS,
                    market_id=slug,
                    asset_id=slug,
                    outcome=outcome,
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
                    exchange=ExchangeId.LIMITLESS,
                    market_id=slug,
                    asset_id=slug,
                    outcome=outcome,
                    price=prev.best_bid,
                    size=0.0,
                    side=Side.SELL,
                    local_timestamp=now,
                )
                self._trade_queue.put_nowait(trade)

    # -- Helpers ------------------------------------------------------------

    def _next_sequence(self, slug: str) -> int:
        """Return the next monotonic sequence number for a market slug."""
        seq = self._sequence_counters.get(slug, 0) + 1
        self._sequence_counters[slug] = seq
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
        """Yield normalised (synthetic) trades as they arrive."""
        while self._running:
            try:
                trade = await asyncio.wait_for(
                    self._trade_queue.get(), timeout=_QUEUE_READ_TIMEOUT,
                )
                yield trade
            except asyncio.TimeoutError:
                continue


# -- Module-level helpers ---------------------------------------------------


def _parse_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO 8601 timestamp string to a tz-aware datetime."""
    if not value:
        return None
    try:
        # Handle ISO format with or without trailing Z
        cleaned = value.replace("Z", "+00:00")
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None
