"""Prometheus metric factories for wallet tracking.

Same pattern as ``alpha_pipeline.metrics.exporter`` — each factory
accepts an optional ``registry`` for test isolation.
"""
from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge


def make_position_shares(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_shares",
        "Number of shares held in a position",
        ["wallet", "exchange", "market", "outcome"],
        **kw,
    )


def make_position_cost_usd(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_cost_usd",
        "Cost basis of a position in USD",
        ["wallet", "exchange", "market", "outcome"],
        **kw,
    )


def make_position_market_value_usd(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_market_value_usd",
        "Current market value of a position in USD",
        ["wallet", "exchange", "market", "outcome"],
        **kw,
    )


def make_position_realized_pnl_usd(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_realized_pnl_usd",
        "Realized PnL of a position in USD",
        ["wallet", "exchange", "market", "outcome"],
        **kw,
    )


def make_position_unrealized_pnl_usd(
    registry: CollectorRegistry | None = None,
) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_unrealized_pnl_usd",
        "Unrealized PnL of a position in USD",
        ["wallet", "exchange", "market", "outcome"],
        **kw,
    )


def make_position_fill_price(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_fill_price",
        "Average fill price of a position",
        ["wallet", "exchange", "market", "outcome"],
        **kw,
    )


def make_total_pnl_usd(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_total_pnl_usd",
        "Total portfolio PnL in USD (from pnl-chart currentValue)",
        ["wallet", "exchange"],
        **kw,
    )


def make_pnl_percent_change(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_pnl_percent_change",
        "Portfolio PnL percentage change over timeframe",
        ["wallet", "exchange"],
        **kw,
    )


def make_traded_volume_usd(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_traded_volume_usd",
        "Total traded volume in USD",
        ["wallet", "exchange"],
        **kw,
    )


def make_position_count(registry: CollectorRegistry | None = None) -> Gauge:
    kw = {"registry": registry} if registry else {}
    return Gauge(
        "wallet_position_count",
        "Number of open positions",
        ["wallet", "exchange"],
        **kw,
    )


def make_poll_total(registry: CollectorRegistry | None = None) -> Counter:
    kw = {"registry": registry} if registry else {}
    return Counter(
        "wallet_poll_total",
        "Total number of wallet poll cycles",
        ["wallet", "exchange"],
        **kw,
    )


def make_poll_errors_total(registry: CollectorRegistry | None = None) -> Counter:
    kw = {"registry": registry} if registry else {}
    return Counter(
        "wallet_poll_errors_total",
        "Total number of wallet poll errors",
        ["wallet", "exchange"],
        **kw,
    )
