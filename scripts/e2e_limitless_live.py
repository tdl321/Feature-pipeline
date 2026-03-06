"""E2E live validation for the Limitless adapter.

Connects to a real Limitless CLOB market, collects orderbook snapshots
for ~15 seconds, and computes TOB imbalance to confirm the full flow.

Usage:
    python scripts/e2e_limitless_live.py [market-slug]

Default slug: auto-detected from active markets API.
"""
from __future__ import annotations

import asyncio
import sys

# Use uvloop if available
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from alpha_pipeline.config import get_settings
from alpha_pipeline.data.adapters.limitless import LimitlessAdapter
from alpha_pipeline.utils.logging import setup_logging, get_logger

logger = get_logger(__name__)

_COLLECT_SECONDS = 15


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    slug = sys.argv[1] if len(sys.argv) > 1 else ""

    if not slug:
        # Try to fetch an active CLOB market slug via REST
        import aiohttp
        import orjson

        headers: dict[str, str] = {}
        if settings.limitless_api_key:
            headers["X-API-Key"] = settings.limitless_api_key

        async with aiohttp.ClientSession() as session:
            url = f"{settings.limitless_api_url}/v1/markets?page=1&limit=5&sortBy=volume"
            async with session.get(url, headers=headers) as resp:
                if resp.status != 200:
                    print(f"Failed to fetch markets: HTTP {resp.status}")
                    return
                markets = orjson.loads(await resp.read())

        # Pick first CLOB market with a slug
        for m in markets if isinstance(markets, list) else markets.get("data", []):
            candidate = m.get("slug", "")
            if candidate:
                slug = candidate
                break

        if not slug:
            print("No active market slugs found. Pass a slug as CLI arg.")
            return

    print(f"Testing with market slug: {slug}")
    print(f"Collecting data for {_COLLECT_SECONDS} seconds...\n")

    adapter = LimitlessAdapter(settings)
    await adapter.connect()
    await adapter.subscribe([slug])
    await adapter.start_listening()

    snapshots = []
    try:
        async for ob in adapter.stream_orderbooks():
            snapshots.append(ob)
            print(
                f"  [{len(snapshots):3d}] "
                f"bid={ob.best_bid}  ask={ob.best_ask}  "
                f"mid={ob.mid_price}  spread={ob.spread}  "
                f"levels={len(ob.bids)}x{len(ob.asks)}"
            )
            if len(snapshots) >= 1:
                # After first snapshot, set a deadline
                try:
                    await asyncio.wait_for(
                        _collect_remaining(adapter, snapshots),
                        timeout=_COLLECT_SECONDS,
                    )
                except asyncio.TimeoutError:
                    pass
                break
    except asyncio.TimeoutError:
        pass

    await adapter.disconnect()

    # --- Summary ---
    print(f"\nCollected {len(snapshots)} orderbook snapshots")
    if not snapshots:
        print("No data received — check API key and market slug.")
        return

    # Compute TOB imbalance from last snapshot
    last = snapshots[-1]
    if last.best_bid is not None and last.best_ask is not None:
        bid_size = last.bids[0].size if last.bids else 0.0
        ask_size = last.asks[0].size if last.asks else 0.0
        total = bid_size + ask_size
        imbalance = (bid_size - ask_size) / total if total > 0 else 0.0
        print(f"\nTOB Imbalance (last snapshot):")
        print(f"  Bid size: {bid_size:.2f}")
        print(f"  Ask size: {ask_size:.2f}")
        print(f"  Imbalance: {imbalance:+.4f}")
        print(f"  Mid price: {last.mid_price:.4f}")
        print(f"  Spread: {last.spread:.4f}")

    print("\nE2E validation PASSED — Limitless adapter is live.")


async def _collect_remaining(
    adapter: LimitlessAdapter,
    snapshots: list,
) -> None:
    """Collect remaining snapshots until timeout."""
    async for ob in adapter.stream_orderbooks():
        snapshots.append(ob)
        print(
            f"  [{len(snapshots):3d}] "
            f"bid={ob.best_bid}  ask={ob.best_ask}  "
            f"mid={ob.mid_price}  spread={ob.spread}"
        )


if __name__ == "__main__":
    asyncio.run(main())
