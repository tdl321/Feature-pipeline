"""WalletMetricsCollector — Prometheus HTTP server + gauge helpers."""
from __future__ import annotations

from prometheus_client import CollectorRegistry, start_http_server

from alpha_pipeline.tracking.metrics import (
    make_pnl_percent_change,
    make_poll_errors_total,
    make_poll_total,
    make_position_cost_usd,
    make_position_count,
    make_position_fill_price,
    make_position_market_value_usd,
    make_position_realized_pnl_usd,
    make_position_shares,
    make_position_unrealized_pnl_usd,
    make_total_pnl_usd,
    make_traded_volume_usd,
)
from alpha_pipeline.tracking.models import WalletSnapshot
from alpha_pipeline.utils.logging import get_logger

logger = get_logger(__name__)


class WalletMetricsCollector:
    """Collects and exposes Prometheus metrics for wallet tracking."""

    def __init__(
        self,
        port: int = 8001,
        registry: CollectorRegistry | None = None,
    ) -> None:
        self._port = port
        self._registry = registry

        self.position_shares = make_position_shares(registry)
        self.position_cost_usd = make_position_cost_usd(registry)
        self.position_market_value_usd = make_position_market_value_usd(registry)
        self.position_realized_pnl_usd = make_position_realized_pnl_usd(registry)
        self.position_unrealized_pnl_usd = make_position_unrealized_pnl_usd(registry)
        self.position_fill_price = make_position_fill_price(registry)
        self.total_pnl_usd = make_total_pnl_usd(registry)
        self.pnl_percent_change = make_pnl_percent_change(registry)
        self.traded_volume_usd = make_traded_volume_usd(registry)
        self.position_count = make_position_count(registry)
        self.poll_total = make_poll_total(registry)
        self.poll_errors_total = make_poll_errors_total(registry)

    def start(self) -> None:
        """Start the Prometheus HTTP server on a daemon thread."""
        kw = {"registry": self._registry} if self._registry else {}
        start_http_server(self._port, **kw)
        logger.info("wallet_metrics_server_started", port=self._port)

    def observe_snapshot(self, snapshot: WalletSnapshot) -> None:
        """Set all gauges from a wallet snapshot."""
        addr = snapshot.address
        exch = snapshot.exchange

        self.poll_total.labels(wallet=addr, exchange=exch).inc()
        self.position_count.labels(wallet=addr, exchange=exch).set(
            len(snapshot.positions),
        )

        for pos in snapshot.positions:
            labels = {
                "wallet": addr,
                "exchange": exch,
                "market": pos.market_slug,
                "outcome": pos.outcome,
            }
            self.position_shares.labels(**labels).set(pos.shares)
            self.position_cost_usd.labels(**labels).set(pos.cost_usd)
            self.position_market_value_usd.labels(**labels).set(pos.market_value_usd)
            self.position_realized_pnl_usd.labels(**labels).set(pos.realized_pnl_usd)
            self.position_unrealized_pnl_usd.labels(**labels).set(
                pos.unrealized_pnl_usd,
            )
            self.position_fill_price.labels(**labels).set(pos.fill_price)

        if snapshot.pnl_summary is not None:
            pnl = snapshot.pnl_summary
            self.total_pnl_usd.labels(wallet=addr, exchange=exch).set(
                pnl.current_value,
            )
            self.pnl_percent_change.labels(wallet=addr, exchange=exch).set(
                pnl.percent_change,
            )

        if snapshot.traded_volume is not None:
            self.traded_volume_usd.labels(wallet=addr, exchange=exch).set(
                snapshot.traded_volume.total_volume_usd,
            )

    def observe_error(self, wallet: str, exchange: str) -> None:
        """Increment the poll-error counter for a wallet."""
        self.poll_errors_total.labels(wallet=wallet, exchange=exchange).inc()
