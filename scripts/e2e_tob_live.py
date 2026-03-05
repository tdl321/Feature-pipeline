"""Short E2E test: connect to live Polymarket, collect orderbook data, compute TOB imbalance.

Collects ~15 seconds of live data, then computes and prints the TOB imbalance feature.
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone

from alpha_pipeline.config import get_settings
from alpha_pipeline.data.adapters.polymarket import PolymarketAdapter
from alpha_pipeline.data.buffer import TimeSeriesBuffer
from alpha_pipeline.features.categories.order_flow.tob_imbalance import TobImbalance
from alpha_pipeline.schemas.feature import FeatureOutput
from alpha_pipeline.utils.logging import setup_logging, get_logger

logger = get_logger(__name__)

# Balanced market (~65% YES) with good volume
ASSET_ID = "17755827780507870103087739732569035075234386298574900130039408497123606676791"
COLLECT_SECONDS = 15


def _orderbook_to_record(ob) -> dict:
    return {
        "timestamp": time.time(),
        "exchange": ob.exchange.value,
        "market_id": ob.market_id,
        "asset_id": ob.asset_id,
        "outcome": ob.outcome.value,
        "best_bid": ob.best_bid,
        "best_ask": ob.best_ask,
        "mid_price": ob.mid_price,
        "spread": ob.spread,
        "bid_depth": sum(lv.size for lv in ob.bids),
        "ask_depth": sum(lv.size for lv in ob.asks),
        "n_bid_levels": len(ob.bids),
        "n_ask_levels": len(ob.asks),
        "best_bid_size": ob.bids[0].size if ob.bids else 0.0,
        "best_ask_size": ob.asks[0].size if ob.asks else 0.0,
    }


async def main() -> None:
    setup_logging("INFO")
    settings = get_settings()

    adapter = PolymarketAdapter(settings)
    buffer = TimeSeriesBuffer(max_rows=10_000)
    feature = TobImbalance()

    print(f"\n{'='*60}")
    print(f"  E2E Live Test: TOB Imbalance on Polymarket")
    print(f"  Asset: ...{ASSET_ID[-12:]}")
    print(f"  Collecting {COLLECT_SECONDS}s of live orderbook data...")
    print(f"{'='*60}\n")

    # --- Connect & subscribe ---
    await adapter.connect()
    await adapter.start_listening()
    await adapter.subscribe([ASSET_ID])

    # --- Collect orderbook snapshots ---
    start = time.time()
    snapshot_count = 0

    async for ob in adapter.stream_orderbooks():
        record = _orderbook_to_record(ob)
        buffer.append(record)
        snapshot_count += 1

        elapsed = time.time() - start
        if snapshot_count % 5 == 0 or snapshot_count == 1:
            bid_str = f"{ob.best_bid:.4f} ({ob.bids[0].size:,.0f})" if ob.bids else "None"
            ask_str = f"{ob.best_ask:.4f} ({ob.asks[0].size:,.0f})" if ob.asks else "None"
            spread_str = f"{ob.spread:.4f}" if ob.spread is not None else "N/A"
            print(
                f"  [{elapsed:5.1f}s] snapshot #{snapshot_count:>3d}  "
                f"bid={bid_str}  ask={ask_str}  spread={spread_str}"
            )

        if elapsed >= COLLECT_SECONDS:
            break

    await adapter.disconnect()

    # --- Compute TOB imbalance ---
    print(f"\n{'─'*60}")
    print(f"  Collected {buffer.size} snapshots in {time.time() - start:.1f}s")
    print(f"{'─'*60}\n")

    df = buffer.to_polars()
    if df.is_empty():
        print("  ERROR: No data collected — check network / asset ID")
        return

    result: FeatureOutput | None = feature.compute(
        orderbook_df=df,
        trades_df=None,
        market_id=ASSET_ID,
    )

    if result is None:
        print("  ERROR: Feature returned None — insufficient data")
        return

    print(f"  Feature: {result.feature_name}")
    print(f"  Time:    {result.timestamp.isoformat()}")
    print(f"  Market:  ...{result.market_id[-12:]}")
    print()
    for key, val in result.values.items():
        if isinstance(val, float):
            print(f"    {key:.<30s} {val:>10.6f}")
        else:
            print(f"    {key:.<30s} {val!s:>10}")

    # --- Interpretation ---
    imb = result.values["tob_imbalance"]
    z = result.values["tob_imbalance_zscore"]
    print()
    if imb > 0.55:
        bias = "BUY pressure"
    elif imb < 0.45:
        bias = "SELL pressure"
    else:
        bias = "NEUTRAL"

    sig = "normal"
    if abs(z) > 2:
        sig = "EXTREME"
    elif abs(z) > 1:
        sig = "elevated"

    print(f"  Interpretation: {bias} (z-score {sig})")
    print()


if __name__ == "__main__":
    asyncio.run(main())
