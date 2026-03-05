from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import polars as pl
import structlog
import structlog.contextvars

from alpha_pipeline.data.buffer import TimeSeriesBuffer
from alpha_pipeline.features.registry import FeatureRegistry
from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector

logger = structlog.get_logger(__name__)


class FeatureRunner:
    """Consumes data events, reads buffers, runs features, emits vectors."""

    def __init__(
        self,
        registry: FeatureRegistry,
        orderbook_buffers: dict[str, TimeSeriesBuffer],
        trade_buffers: dict[str, TimeSeriesBuffer],
        event_queue: asyncio.Queue[dict[str, Any]],
        output_queue: asyncio.Queue[FeatureVector] | None = None,
    ) -> None:
        self._registry = registry
        self._ob_buffers = orderbook_buffers
        self._trade_buffers = trade_buffers
        self._event_queue = event_queue
        self._output_queue = output_queue or asyncio.Queue()
        self._running = False

    async def run(self) -> None:
        """Main event loop -- consume events and compute features."""
        self._running = True
        logger.info("feature_runner_started")

        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            market_id = event.get("market_id", "")
            exchange = event.get("exchange", "")
            trigger_event_id = event.get("event_id", "")
            correlation_id = str(uuid4())

            # Bind correlation_id to structlog context for this cycle
            structlog.contextvars.bind_contextvars(correlation_id=correlation_id)
            try:
                vector = self._compute_features(
                    market_id,
                    exchange,
                    correlation_id=correlation_id,
                    trigger_event_id=trigger_event_id,
                )
                if vector and vector.features:
                    await self._output_queue.put(vector)
                    logger.debug(
                        "feature_vector_emitted",
                        market_id=market_id,
                        count=len(vector.features),
                    )
            except Exception:
                logger.exception("feature_computation_error", market_id=market_id)
            finally:
                structlog.contextvars.unbind_contextvars("correlation_id")

    def stop(self) -> None:
        self._running = False

    def _compute_features(
        self,
        market_id: str,
        exchange: str,
        *,
        correlation_id: str = "",
        trigger_event_id: str = "",
    ) -> FeatureVector | None:
        """Run all enabled features for a given market."""
        ob_buf = self._ob_buffers.get(market_id)
        trade_buf = self._trade_buffers.get(market_id)

        ob_df = ob_buf.to_polars() if ob_buf and not ob_buf.is_empty else None
        trade_df = (
            trade_buf.to_polars() if trade_buf and not trade_buf.is_empty else None
        )

        trigger_ids = (trigger_event_id,) if trigger_event_id else ()
        outputs: list[FeatureOutput] = []

        for feature in self._registry.get_enabled():
            if not feature.validate_input(ob_df, trade_df):
                continue

            result = feature.compute(
                orderbook_df=ob_df,
                trades_df=trade_df,
                market_id=market_id,
            )
            if result is not None:
                # Attach traceability fields without mutating the original
                result = result.model_copy(update={
                    "correlation_id": correlation_id,
                    "trigger_event_ids": trigger_ids,
                })
                outputs.append(result)

        if not outputs:
            return None

        return FeatureVector(
            timestamp=outputs[0].timestamp,
            market_id=market_id,
            exchange=ExchangeId(exchange) if exchange else ExchangeId.POLYMARKET,
            features=tuple(outputs),
            correlation_id=correlation_id,
            trigger_event_ids=trigger_ids,
        )

    @property
    def output_queue(self) -> asyncio.Queue[FeatureVector]:
        return self._output_queue
