"""Limitless exchange implementation of WalletDataFetcher."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import aiohttp

from alpha_pipeline.tracking.models import (
    PnlSummary,
    TradedVolume,
    WalletPosition,
    WalletSnapshot,
    raw_to_usd,
)
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)

_BASE_URL = "https://api.limitless.exchange"


class LimitlessWalletFetcher:
    """Fetch wallet portfolio data from the Limitless public API."""

    def __init__(self, base_url: str = _BASE_URL) -> None:
        self._base_url = base_url.rstrip("/")
        self._session: aiohttp.ClientSession | None = None

    @property
    def exchange_id(self) -> str:
        return "limitless"

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def fetch_positions(self, address: str) -> list[WalletPosition]:
        """GET /portfolio/{address}/positions — parse CLOB positions."""
        session = await self._ensure_session()
        url = f"{self._base_url}/portfolio/{address}/positions"
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return []
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError:
            logger.exception("limitless_positions_error", address=address)
            return []

        positions: list[WalletPosition] = []
        clob_items = data.get("clob", []) if isinstance(data, dict) else []
        for item in clob_items:
            market = item.get("market", {})
            slug = market.get("slug", "")
            title = market.get("title", "")
            status = market.get("status", "")
            pos_data = item.get("positions", {})
            balances = item.get("tokensBalance", {})

            for outcome_key in ("yes", "no"):
                side = pos_data.get(outcome_key)
                if side is None:
                    continue
                # Skip sides with zero cost (no position)
                cost = raw_to_usd(side.get("cost"))
                if cost == 0.0:
                    continue

                balance_raw = balances.get(outcome_key, "0")
                shares = raw_to_usd(balance_raw)

                positions.append(
                    WalletPosition(
                        market_slug=slug,
                        title=title,
                        status=status,
                        outcome=outcome_key.upper(),
                        shares=shares,
                        cost_usd=cost,
                        fill_price=raw_to_usd(side.get("fillPrice")),
                        market_value_usd=raw_to_usd(side.get("marketValue")),
                        realized_pnl_usd=raw_to_usd(
                            side.get("realisedPnl", side.get("realizedPnL")),
                        ),
                        unrealized_pnl_usd=raw_to_usd(side.get("unrealizedPnl")),
                    ),
                )
        return positions

    async def fetch_pnl_summary(
        self, address: str, timeframe: str = "7d",
    ) -> PnlSummary | None:
        """GET /portfolio/{address}/pnl-chart?timeframe=..."""
        session = await self._ensure_session()
        url = f"{self._base_url}/portfolio/{address}/pnl-chart"
        try:
            async with session.get(url, params={"timeframe": timeframe}) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError:
            logger.exception("limitless_pnl_error", address=address)
            return None

        realized = sum(
            raw_to_usd(item.get("realizedPnL"))
            for item in (data.get("positions", []) if isinstance(data, dict) else [])
        )

        return PnlSummary(
            timeframe=timeframe,
            current_value=float(data.get("currentValue", 0)),
            previous_value=float(data.get("previousValue", 0)),
            percent_change=float(data.get("percentChange", 0)),
            realized_pnl=realized,
        )

    async def fetch_traded_volume(self, address: str) -> TradedVolume | None:
        """GET /portfolio/{address}/traded-volume."""
        session = await self._ensure_session()
        url = f"{self._base_url}/portfolio/{address}/traded-volume"
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return None
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError:
            logger.exception("limitless_volume_error", address=address)
            return None

        total = raw_to_usd(
            data.get("data", data.get("totalVolume", data.get("total", 0))),
        )
        return TradedVolume(total_volume_usd=total, raw_fields=data)

    async def fetch_pnl_history(
        self, address: str, timeframe: str = "all",
    ) -> list[dict]:
        """GET /portfolio/{address}/pnl-chart?timeframe=... → historical data points.

        Returns a list of ``{"timestamp": datetime, "value": float}`` dicts
        from the ``data[]`` array in the API response.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/portfolio/{address}/pnl-chart"
        try:
            async with session.get(url, params={"timeframe": timeframe}) as resp:
                if resp.status == 404:
                    return []
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientError:
            logger.exception("limitless_pnl_history_error", address=address)
            return []

        points: list[dict] = []
        raw_data = data.get("data", []) if isinstance(data, dict) else []
        for item in raw_data:
            try:
                ts = item.get("timestamp", item.get("t"))
                val = item.get("value", item.get("v", 0))
                if ts is None:
                    continue
                # Handle epoch ms, epoch seconds, and ISO strings
                if isinstance(ts, (int, float)):
                    # Limitless API returns epoch milliseconds
                    epoch_s = ts / 1000 if ts > 1e12 else ts
                    dt = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
                elif isinstance(ts, str):
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                else:
                    continue
                points.append({"timestamp": dt, "value": float(val)})
            except (ValueError, TypeError):
                continue

        logger.info(
            "pnl_history_fetched",
            address=address,
            timeframe=timeframe,
            points=len(points),
        )
        return points

    async def fetch_snapshot(
        self, address: str, timeframe: str = "7d",
    ) -> WalletSnapshot:
        """Fetch all three endpoints concurrently for a single wallet."""
        positions, pnl, volume = await asyncio.gather(
            self.fetch_positions(address),
            self.fetch_pnl_summary(address, timeframe),
            self.fetch_traded_volume(address),
        )
        return WalletSnapshot(
            address=address,
            exchange=self.exchange_id,
            polled_at=datetime.now(timezone.utc),
            positions=positions,
            pnl_summary=pnl,
            traded_volume=volume,
        )

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
