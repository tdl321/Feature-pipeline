"""MarketRoster — auto-discovers and tracks active Limitless markets.

Polls the Limitless ``/markets/active`` endpoint, filters by ticker and
trade type, and computes diffs so the adapter can hot-swap subscriptions
when hourly binary options expire and get replaced.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

import aiohttp
import orjson

from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_PAGE_LIMIT = 25


@dataclass(frozen=True)
class MarketInfo:
    """Immutable snapshot of an active Limitless market."""

    slug: str
    ticker: str
    expiration_ts: int  # milliseconds since epoch
    title: str


@dataclass(frozen=True)
class RosterDelta:
    """Diff between two roster snapshots."""

    added: tuple[str, ...]
    removed: tuple[str, ...]


class MarketRoster:
    """Discovers active Limitless markets and tracks rollover.

    Parameters
    ----------
    session:
        Shared ``aiohttp.ClientSession`` (should have base_url and auth
        headers already configured).
    api_url:
        Limitless API base URL (used only for logging).
    tickers:
        Set of ticker symbols to track (e.g. ``{"ETH", "SOL"}``).
    grace_seconds:
        Seconds after ``expiration_ts`` before a market is considered
        removed.  Allows the pipeline to finish processing the last
        few updates after expiry.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_url: str,
        tickers: frozenset[str],
        grace_seconds: float = 300.0,
    ) -> None:
        self._session = session
        self._api_url = api_url
        self._tickers = tickers
        self._grace_seconds = grace_seconds
        # slug -> MarketInfo  (immutable — replaced on each refresh)
        self._active: dict[str, MarketInfo] = {}

    # -- Public API ---------------------------------------------------------

    @property
    def active_slugs(self) -> list[str]:
        """Return currently active market slugs (sorted for determinism)."""
        return sorted(self._active.keys())

    async def refresh(self) -> RosterDelta:
        """Poll the API, diff against current state, return changes.

        Returns a ``RosterDelta`` with newly discovered and expired slugs.
        """
        try:
            fresh = await self._fetch_active_markets()
        except Exception:
            logger.exception("roster_fetch_error")
            return RosterDelta(added=(), removed=())

        now_ms = int(time.time() * 1000)
        grace_ms = int(self._grace_seconds * 1000)

        # Build new active set: markets from API that match our tickers
        new_active: dict[str, MarketInfo] = {}
        for info in fresh:
            new_active[info.slug] = info

        # Detect removed: in old set but not in new, OR past expiry + grace
        old_slugs = set(self._active.keys())
        new_slugs = set(new_active.keys())

        removed_slugs: list[str] = []
        for slug in old_slugs:
            if slug not in new_slugs:
                removed_slugs.append(slug)
            elif self._active[slug].expiration_ts + grace_ms < now_ms:
                removed_slugs.append(slug)

        # Detect added: in new set but not in old
        added_slugs = [s for s in new_slugs if s not in old_slugs]

        # Remove expired from new_active as well
        for slug in removed_slugs:
            new_active.pop(slug, None)

        # Immutable update
        self._active = dict(new_active)

        delta = RosterDelta(
            added=tuple(sorted(added_slugs)),
            removed=tuple(sorted(removed_slugs)),
        )

        if delta.added or delta.removed:
            logger.info(
                "roster_delta",
                added=delta.added,
                removed=delta.removed,
                active_count=len(self._active),
            )

        return delta

    # -- Internal -----------------------------------------------------------

    async def _fetch_active_markets(self) -> list[MarketInfo]:
        """Fetch active CLOB markets matching tracked tickers.

        Paginates through ``/markets/active`` if there are more than
        ``_PAGE_LIMIT`` results.
        """
        results: list[MarketInfo] = []
        offset = 0

        while True:
            url = (
                f"/markets/active"
                f"?sortBy=ending_soon&limit={_PAGE_LIMIT}&offset={offset}"
            )
            async with self._session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(
                        "roster_api_error",
                        status=resp.status,
                        url=url,
                    )
                    break
                raw = await resp.read()
                data = orjson.loads(raw)

            # data is expected to be a list of market objects
            markets = data if isinstance(data, list) else data.get("markets", [])

            if not markets:
                break

            for m in markets:
                trade_type = m.get("trade_type", m.get("tradeType", ""))
                if trade_type != "clob":
                    continue

                oracle_meta = m.get("price_oracle_metadata", {}) or {}
                ticker = oracle_meta.get("ticker", "")
                if not ticker or ticker.upper() not in self._tickers:
                    continue

                slug = m.get("slug", "")
                if not slug:
                    continue

                expiration = m.get("expiration_timestamp", 0)
                title = m.get("title", "")

                results.append(
                    MarketInfo(
                        slug=slug,
                        ticker=ticker.upper(),
                        expiration_ts=int(expiration),
                        title=title,
                    )
                )

            # If we got fewer than the page limit, we've reached the end
            if len(markets) < _PAGE_LIMIT:
                break
            offset += _PAGE_LIMIT

        logger.debug(
            "roster_fetched",
            total_matches=len(results),
            tickers=list(self._tickers),
        )
        return results
