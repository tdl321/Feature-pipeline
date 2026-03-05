"""Main entry point for the alpha pipeline."""
from __future__ import annotations

import asyncio
import signal
import sys
from pathlib import Path

# Use uvloop for 2-2.5x speedup on Unix
try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from alpha_pipeline.config import get_settings
from alpha_pipeline.data.collector import FeatureCollector
from alpha_pipeline.data.manager import DataManager
from alpha_pipeline.data.adapters.polymarket import PolymarketAdapter
from alpha_pipeline.features.registry import FeatureRegistry
from alpha_pipeline.features.runner import FeatureRunner
from alpha_pipeline.schemas.feature import FeatureVector
from alpha_pipeline.utils.logging import setup_logging, get_logger

# Import feature modules to trigger @register_feature decorators
import alpha_pipeline.features.categories.cross_exchange.arb_spread  # noqa: F401
import alpha_pipeline.features.categories.order_flow.tob_imbalance  # noqa: F401
import alpha_pipeline.features.categories.order_flow.buy_sell_imbalance  # noqa: F401
import alpha_pipeline.features.categories.toxicity.markouts  # noqa: F401
import alpha_pipeline.features.categories.pricing.binary_implied_prob  # noqa: F401
import alpha_pipeline.features.categories.size_signals.avg_order_size  # noqa: F401
import alpha_pipeline.features.categories.spread_analysis.spread_dynamics  # noqa: F401


logger = get_logger(__name__)


async def log_output(
    output_queue: asyncio.Queue[FeatureVector],
    collector: FeatureCollector,
) -> None:
    """Log and persist feature vectors as they arrive."""
    while True:
        try:
            vector = await asyncio.wait_for(output_queue.get(), timeout=1.0)
            logger.info(
                "feature_vector",
                market_id=vector.market_id,
                exchange=vector.exchange,
                n_features=len(vector.features),
                features={f.feature_name: f.values for f in vector.features},
            )
            collector.write(vector)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info("pipeline_starting", log_level=settings.log_level)

    # --- Data layer ---
    data_manager = DataManager(settings)

    # Add adapters (start with Polymarket only for smoke testing)
    poly = PolymarketAdapter(settings)
    data_manager.add_adapter(poly)

    # Optionally add Opinion / Limitless adapters here

    # --- Feature engine ---
    registry = FeatureRegistry()
    registry.load_registered()
    logger.info("features_loaded", features=registry.all_names)

    runner = FeatureRunner(
        registry=registry,
        orderbook_buffers=data_manager.orderbook_buffers,
        trade_buffers=data_manager.trade_buffers,
        event_queue=data_manager.event_queue,
    )

    # --- Subscribe to markets ---
    # Pass market/asset IDs via CLI args or config
    market_ids = sys.argv[1:] if len(sys.argv) > 1 else []
    if not market_ids:
        logger.warning("no_market_ids", msg="Pass market IDs as CLI arguments")
        return

    await data_manager.start()
    for adapter in data_manager._adapters:
        await adapter.subscribe(market_ids)

    # --- Data persistence ---
    collector = FeatureCollector(settings.feature_output_dir)

    # --- Run ---
    output_task = asyncio.create_task(
        log_output(runner.output_queue, collector)
    )
    runner_task = asyncio.create_task(runner.run())

    # Graceful shutdown
    stop = asyncio.Event()

    def _signal_handler() -> None:
        logger.info("shutdown_signal_received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    await stop.wait()

    runner.stop()
    await data_manager.stop()
    runner_task.cancel()
    output_task.cancel()
    collector.close()
    logger.info("pipeline_stopped")


if __name__ == "__main__":
    asyncio.run(main())
