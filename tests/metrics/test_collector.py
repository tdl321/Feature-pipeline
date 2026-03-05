"""Unit tests for MetricsCollector using isolated CollectorRegistry."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest
from prometheus_client import CollectorRegistry

from alpha_pipeline.metrics.collector import MetricsCollector
from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector


@pytest.fixture()
def registry() -> CollectorRegistry:
    return CollectorRegistry()


@pytest.fixture()
def collector(registry: CollectorRegistry) -> MetricsCollector:
    return MetricsCollector(port=0, registry=registry)


def _make_vector(
    values: dict[str, float | int | bool | str | None] | None = None,
    feature_name: str = "order_flow.tob_imbalance",
    market_id: str = "0xabc",
) -> FeatureVector:
    if values is None:
        values = {"tob_imbalance": 0.65, "tob_imbalance_zscore": 1.2}
    return FeatureVector(
        timestamp=datetime.now(timezone.utc),
        market_id=market_id,
        exchange=ExchangeId.POLYMARKET,
        features=(
            FeatureOutput(
                feature_name=feature_name,
                timestamp=datetime.now(timezone.utc),
                market_id=market_id,
                values=values,
            ),
        ),
    )


class TestObserveVector:
    def test_increments_vector_counter(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        vector = _make_vector()
        collector.observe_vector(vector)

        val = registry.get_sample_value(
            "pipeline_feature_vectors_total",
            {"market_id": "0xabc", "exchange": "polymarket"},
        )
        assert val == 1.0

    def test_increments_features_computed(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        vector = _make_vector()
        collector.observe_vector(vector)

        val = registry.get_sample_value(
            "pipeline_features_computed_total",
            {"feature_name": "order_flow.tob_imbalance"},
        )
        assert val == 1.0

    def test_sets_gauge_for_numeric_values(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        vector = _make_vector({"tob_imbalance": 0.65, "tob_imbalance_zscore": 1.2})
        collector.observe_vector(vector)

        imb = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "tob_imbalance",
            },
        )
        assert imb == 0.65

        zscore = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "tob_imbalance_zscore",
            },
        )
        assert zscore == 1.2

    def test_booleans_exported_as_0_1(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        vector = _make_vector({"is_widening": True, "is_stable": False})
        collector.observe_vector(vector)

        widen = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "is_widening",
            },
        )
        assert widen == 1.0

        stable = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "is_stable",
            },
        )
        assert stable == 0.0

    def test_strings_and_none_skipped(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        vector = _make_vector({"label": "high", "empty": None, "score": 42.0})
        collector.observe_vector(vector)

        # Only score should produce a gauge
        score = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "score",
            },
        )
        assert score == 42.0

        # String and None should not appear
        label = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "label",
            },
        )
        assert label is None

        empty = registry.get_sample_value(
            "pipeline_feature_value",
            {
                "feature_name": "order_flow.tob_imbalance",
                "market_id": "0xabc",
                "value_key": "empty",
            },
        )
        assert empty is None


class TestUpdateBuffers:
    def test_sets_correct_values(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        class FakeBuf:
            def __init__(self, size: int) -> None:
                self.size = size

        collector.update_buffers(
            ob_buffers={"0xabc": FakeBuf(150)},
            trade_buffers={"0xabc": FakeBuf(75)},
        )

        ob = registry.get_sample_value(
            "pipeline_buffer_size",
            {"buffer_type": "orderbook", "market_id": "0xabc"},
        )
        assert ob == 150.0

        tr = registry.get_sample_value(
            "pipeline_buffer_size",
            {"buffer_type": "trade", "market_id": "0xabc"},
        )
        assert tr == 75.0


class TestUpdateQueues:
    def test_sets_correct_values(
        self, collector: MetricsCollector, registry: CollectorRegistry
    ) -> None:
        collector.update_queues(event_size=12, output_size=3)

        ev = registry.get_sample_value(
            "pipeline_queue_size", {"queue_name": "event"}
        )
        assert ev == 12.0

        out = registry.get_sample_value(
            "pipeline_queue_size", {"queue_name": "output"}
        )
        assert out == 3.0
