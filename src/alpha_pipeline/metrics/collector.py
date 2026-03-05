"""MetricsCollector — thin wrapper around Prometheus metric objects.

Provides ``start()`` to launch the HTTP server and ``observe_*`` /
``update_*`` helpers that the pipeline calls on every tick.
"""
from __future__ import annotations

from prometheus_client import CollectorRegistry, start_http_server

from alpha_pipeline.metrics.exporter import (
    make_buffer_size,
    make_connection_up,
    make_events_processed,
    make_feature_errors,
    make_feature_value,
    make_feature_vectors,
    make_features_computed,
    make_queue_size,
)
from alpha_pipeline.schemas.feature import FeatureVector
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


class MetricsCollector:
    """Collects and exposes Prometheus metrics for the pipeline."""

    def __init__(
        self,
        port: int = 8000,
        registry: CollectorRegistry | None = None,
    ) -> None:
        self._port = port
        self._registry = registry

        self.feature_value = make_feature_value(registry)
        self.events_processed = make_events_processed(registry)
        self.feature_vectors = make_feature_vectors(registry)
        self.features_computed = make_features_computed(registry)
        self.feature_errors = make_feature_errors(registry)
        self.buffer_size = make_buffer_size(registry)
        self.queue_size = make_queue_size(registry)
        self.connection_up = make_connection_up(registry)

    def start(self) -> None:
        """Start the Prometheus HTTP server on a daemon thread."""
        kw = {"registry": self._registry} if self._registry else {}
        start_http_server(self._port, **kw)
        logger.info("metrics_server_started", port=self._port)

    def observe_vector(self, vector: FeatureVector) -> None:
        """Record counters and gauges for a single FeatureVector."""
        self.feature_vectors.labels(
            market_id=vector.market_id,
            exchange=vector.exchange,
        ).inc()

        for output in vector.features:
            self.features_computed.labels(feature_name=output.feature_name).inc()
            for key, value in output.values.items():
                if isinstance(value, bool):
                    self.feature_value.labels(
                        feature_name=output.feature_name,
                        market_id=vector.market_id,
                        value_key=key,
                    ).set(int(value))
                elif isinstance(value, (int, float)):
                    self.feature_value.labels(
                        feature_name=output.feature_name,
                        market_id=vector.market_id,
                        value_key=key,
                    ).set(value)
                # strings and None are skipped

    def update_buffers(
        self,
        ob_buffers: dict[str, object],
        trade_buffers: dict[str, object],
    ) -> None:
        """Set buffer-size gauges from live buffer dicts."""
        for market_id, buf in ob_buffers.items():
            self.buffer_size.labels(
                buffer_type="orderbook", market_id=market_id
            ).set(buf.size)  # type: ignore[union-attr]
        for market_id, buf in trade_buffers.items():
            self.buffer_size.labels(
                buffer_type="trade", market_id=market_id
            ).set(buf.size)  # type: ignore[union-attr]

    def update_queues(self, event_size: int, output_size: int) -> None:
        """Set queue-depth gauges."""
        self.queue_size.labels(queue_name="event").set(event_size)
        self.queue_size.labels(queue_name="output").set(output_size)
