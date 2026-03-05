"""Opinion exchange REST-polling adapter.

Since Opinion exposes only a REST API (no WebSocket), this adapter polls
the orderbook endpoint at a configurable interval and detects synthetic
trades by diffing consecutive snapshots.
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


class OpinionAdapter:
    """Exchange adapter that polls the Opinion REST API.

    Lifecycle:
        1. ``connect()``  — create HTTP session (with optional API key)
        2. ``subscribe()`` — register market IDs to poll
        3. ``start_listening()`` — begin the poll loop
        4. iterate via ``stream_orderbooks()`` / ``stream_trades()``
        5. ``disconnect()`` — close session
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
        logger.info("opinion_connected", base_url=self._settings.opinion_api_url)

    async def disconnect(self) -> None:
        """Stop polling and close the HTTP session."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()
        if self._session:
            await self._session.close()
            self._session = None
        logger.info("opinion_disconnected")

    async def subscribe(self, market_ids: list[str]) -> None:
        """Register market IDs to be polled.

        For a REST adapter this simply stores the IDs; no server-side
        subscription is required.
        """
        self._market_ids = market_ids
        logger.info("opinion_subscribed", market_ids=market_ids)

    # -- Background tasks ---------------------------------------------------

    async def start_listening(self) -> None:
        """Start the background poll loop."""
        self._tasks.append(asyncio.create_task(self._poll_loop()))

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

    # -- Orderbook fetch + synthetic trade detection ------------------------

    async def _fetch_orderbook(self, market_id: str) -> None:
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
