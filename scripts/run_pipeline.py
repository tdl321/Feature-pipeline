"""Main entry point for the alpha pipeline."""
from __future__ import annotations

import argparse
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

from alpha_pipeline.config import Settings, get_settings
from alpha_pipeline.data.collector import FeatureCollector
from alpha_pipeline.data.manager import DataManager
from alpha_pipeline.data.adapters.limitless import LimitlessAdapter
from alpha_pipeline.data.roster import MarketRoster
from alpha_pipeline.data.adapters.opinion import OpinionAdapter
from alpha_pipeline.data.adapters.polymarket import PolymarketAdapter
from alpha_pipeline.features.registry import FeatureRegistry
from alpha_pipeline.features.runner import FeatureRunner
from alpha_pipeline.metrics import MetricsCollector
from alpha_pipeline.schemas import OutcomeType
from alpha_pipeline.schemas.feature import FeatureVector
from alpha_pipeline.utils.logging import setup_logging, get_logger

# Import feature modules to trigger @register_feature decorators
import alpha_pipeline.features.categories.cross_exchange.arb_spread  # noqa: F401
import alpha_pipeline.features.categories.cross_exchange.cross_outcome_arb  # noqa: F401
import alpha_pipeline.features.categories.order_flow.tob_imbalance  # noqa: F401
import alpha_pipeline.features.categories.order_flow.buy_sell_imbalance  # noqa: F401
import alpha_pipeline.features.categories.order_flow.wash_detection  # noqa: F401
import alpha_pipeline.features.categories.toxicity.markouts  # noqa: F401
import alpha_pipeline.features.categories.pricing.binary_implied_prob  # noqa: F401
import alpha_pipeline.features.categories.size_signals.avg_order_size  # noqa: F401
import alpha_pipeline.features.categories.spread_analysis.spread_dynamics  # noqa: F401


logger = get_logger(__name__)


async def log_output(
    output_queue: asyncio.Queue[FeatureVector],
    collector: FeatureCollector,
    metrics: MetricsCollector | None = None,
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
            if metrics is not None:
                metrics.observe_vector(vector)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break


async def poll_infrastructure_metrics(
    metrics: MetricsCollector,
    data_manager: DataManager,
    output_queue: asyncio.Queue[FeatureVector],
) -> None:
    """Periodically push buffer/queue sizes into Prometheus gauges."""
    while True:
        try:
            metrics.update_buffers(
                data_manager.orderbook_buffers,
                data_manager.trade_buffers,
            )
            metrics.update_queues(
                data_manager.event_queue.qsize(),
                output_queue.qsize(),
            )
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments with per-exchange market IDs."""
    parser = argparse.ArgumentParser(
        description="Alpha pipeline — prediction market orderbook ingestion",
    )
    parser.add_argument(
        "--poly", type=str, default="",
        help="Comma-separated Polymarket asset IDs (hex) — all treated as YES",
    )
    parser.add_argument(
        "--poly-pairs", type=str, default="",
        help=(
            "Comma-separated YES:NO asset ID pairs for Polymarket. "
            "Format: 'yes_id:no_id,yes_id2:no_id2'. "
            "Both sides are subscribed with correct outcome tagging."
        ),
    )
    parser.add_argument(
        "--limitless", type=str, default="",
        help="Comma-separated Limitless market slugs (manual, bypasses roster)",
    )
    parser.add_argument(
        "--limitless-track", type=str, default="",
        help=(
            "Comma-separated tickers for auto-discovery (e.g. ETH,SOL). "
            "Roster polls for active markets and auto-subscribes on rollover."
        ),
    )
    parser.add_argument(
        "--opinion", type=str, default="",
        help="Comma-separated Opinion market IDs",
    )
    return parser.parse_args()


async def _roster_loop(
    roster: MarketRoster,
    adapter: LimitlessAdapter,
    data_manager: DataManager,
    settings: Settings,
) -> None:
    """Periodically refresh the roster and hot-swap adapter subscriptions."""
    while True:
        try:
            await asyncio.sleep(settings.limitless_roster_poll_seconds)
            delta = await roster.refresh()
            if delta.added or delta.removed:
                await adapter.resubscribe(roster.active_slugs)
                for slug in delta.removed:
                    data_manager.cleanup_market(slug)
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("roster_loop_error")


async def main() -> None:
    args = parse_args()
    settings = get_settings()
    setup_logging(settings.log_level)

    logger.info("pipeline_starting", log_level=settings.log_level)

    poly_ids = [m.strip() for m in args.poly.split(",") if m.strip()]
    limitless_slugs = [m.strip() for m in args.limitless.split(",") if m.strip()]
    limitless_tickers = [
        t.strip().upper()
        for t in args.limitless_track.split(",")
        if t.strip()
    ]
    opinion_ids = [m.strip() for m in args.opinion.split(",") if m.strip()]

    # Build asset_outcome_map from --poly-pairs (YES:NO pairs)
    poly_outcome_map: dict[str, OutcomeType] = {}
    if args.poly_pairs:
        for pair_str in args.poly_pairs.split(","):
            pair_str = pair_str.strip()
            if not pair_str:
                continue
            parts = pair_str.split(":")
            if len(parts) != 2:
                logger.error(
                    "invalid_poly_pair",
                    pair=pair_str,
                    msg="Expected format 'yes_asset_id:no_asset_id'",
                )
                return
            yes_id, no_id = parts[0].strip(), parts[1].strip()
            poly_outcome_map[yes_id] = OutcomeType.YES
            poly_outcome_map[no_id] = OutcomeType.NO
            # Add both IDs to subscription list
            if yes_id not in poly_ids:
                poly_ids.append(yes_id)
            if no_id not in poly_ids:
                poly_ids.append(no_id)

    if not poly_ids and not limitless_slugs and not limitless_tickers and not opinion_ids:
        logger.warning(
            "no_market_ids",
            msg="Pass market IDs via --poly, --limitless, --limitless-track, and/or --opinion",
        )
        return

    # --- Data layer ---
    data_manager = DataManager(settings)

    # Polymarket adapter (always available)
    if poly_ids:
        poly = PolymarketAdapter(settings)
        data_manager.add_adapter(poly)

    # Opinion adapter (WebSocket primary, REST fallback)
    if opinion_ids:
        opinion = OpinionAdapter(settings)
        data_manager.add_adapter(opinion)

    # Limitless adapter (manual slugs and/or auto-tracked tickers)
    limitless: LimitlessAdapter | None = None
    roster: MarketRoster | None = None
    if limitless_slugs or limitless_tickers:
        if not settings.limitless_api_key:
            logger.warning(
                "limitless_no_api_key",
                msg="Set LIMITLESS_API_KEY to enable Limitless adapter",
            )
        limitless = LimitlessAdapter(settings)
        data_manager.add_adapter(limitless)

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

    # --- Subscribe each adapter to its own markets ---
    await data_manager.start()
    for adapter in data_manager._adapters:
        if isinstance(adapter, PolymarketAdapter) and poly_ids:
            await adapter.subscribe(
                poly_ids,
                asset_outcome_map=poly_outcome_map if poly_outcome_map else None,
            )
        elif isinstance(adapter, OpinionAdapter) and opinion_ids:
            await adapter.subscribe(opinion_ids)
        elif isinstance(adapter, LimitlessAdapter):
            # Manual slugs get subscribed directly
            initial_slugs = list(limitless_slugs)

            # Auto-tracked tickers: create roster, do initial refresh
            if limitless_tickers and limitless is not None:
                # Reuse the adapter's session for roster API calls
                import aiohttp
                headers: dict[str, str] = {}
                if settings.limitless_api_key:
                    headers["X-API-Key"] = settings.limitless_api_key
                roster_session = aiohttp.ClientSession(
                    base_url=settings.limitless_api_url,
                    headers=headers,
                )
                roster = MarketRoster(
                    session=roster_session,
                    api_url=settings.limitless_api_url,
                    tickers=frozenset(limitless_tickers),
                    grace_seconds=settings.limitless_roster_expiry_grace_seconds,
                )
                delta = await roster.refresh()
                initial_slugs.extend(roster.active_slugs)
                logger.info(
                    "roster_initial_discovery",
                    tickers=limitless_tickers,
                    discovered=roster.active_slugs,
                )

            if initial_slugs:
                await adapter.subscribe(initial_slugs)

    # --- Metrics ---
    metrics: MetricsCollector | None = None
    if settings.metrics_enabled:
        metrics = MetricsCollector(port=settings.metrics_port)
        metrics.start()

    # --- Data persistence ---
    collector = FeatureCollector(settings.feature_output_dir)

    # --- Run ---
    output_task = asyncio.create_task(
        log_output(runner.output_queue, collector, metrics)
    )
    runner_task = asyncio.create_task(runner.run())

    infra_task: asyncio.Task[None] | None = None
    if metrics is not None:
        infra_task = asyncio.create_task(
            poll_infrastructure_metrics(metrics, data_manager, runner.output_queue)
        )

    # --- Roster loop (auto-discovery) ---
    roster_task: asyncio.Task[None] | None = None
    if roster is not None and limitless is not None:
        roster_task = asyncio.create_task(
            _roster_loop(roster, limitless, data_manager, settings)
        )

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
    if infra_task is not None:
        infra_task.cancel()
    if roster_task is not None:
        roster_task.cancel()
    collector.close()
    logger.info("pipeline_stopped")


if __name__ == "__main__":
    asyncio.run(main())
