"""Prometheus metric definitions for the alpha pipeline.

All metrics are defined at module level for import convenience.
Each factory accepts an optional ``registry`` for test isolation;
when *None* the default global registry is used.
"""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge


def make_feature_value(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "pipeline_feature_value",
        "Current value of a computed feature",
        ["feature_name", "market_id", "value_key"],
        **kw,
    )


def make_events_processed(registry: CollectorRegistry | None = None) -> Counter:
    kw = {"registry": registry} if registry else {}
    return Counter(
        "pipeline_events_processed_total",
        "Total data events ingested by the pipeline",
        ["event_type", "exchange"],
        **kw,
    )


def make_feature_vectors(registry: CollectorRegistry | None = None) -> Counter:
    kw = {"registry": registry} if registry else {}
    return Counter(
        "pipeline_feature_vectors_total",
        "Total feature vectors emitted",
        ["market_id", "exchange"],
        **kw,
    )


def make_features_computed(registry: CollectorRegistry | None = None) -> Counter:
    kw = {"registry": registry} if registry else {}
    return Counter(
        "pipeline_features_computed_total",
        "Total individual features computed",
        ["feature_name"],
        **kw,
    )


def make_feature_errors(registry: CollectorRegistry | None = None) -> Counter:
    kw = {"registry": registry} if registry else {}
    return Counter(
        "pipeline_feature_errors_total",
        "Total feature computation errors",
        ["feature_name"],
        **kw,
    )


def make_buffer_size(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "pipeline_buffer_size",
        "Current number of rows in a time-series buffer",
        ["buffer_type", "market_id"],
        **kw,
    )


def make_queue_size(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "pipeline_queue_size",
        "Current depth of an asyncio queue",
        ["queue_name"],
        **kw,
    )


def make_connection_up(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "pipeline_connection_up",
        "Whether an exchange connection is alive (1=up, 0=down)",
        ["exchange"],
        **kw,
    )
