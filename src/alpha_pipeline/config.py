from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Polymarket
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Opinion
    opinion_api_url: str = "https://api.opinion.xyz"
    opinion_ws_url: str = "wss://ws.opinion.trade"
    opinion_api_key: str = ""
    opinion_poll_interval_seconds: float = 2.0

    # Limitless
    limitless_ws_url: str = "wss://ws.limitless.exchange"
    limitless_api_url: str = "https://api.limitless.exchange"
    limitless_api_key: str = ""

    # Limitless roster (auto-discovery)
    limitless_roster_poll_seconds: float = 30.0
    limitless_roster_expiry_grace_seconds: float = 300.0

    # Pipeline
    buffer_max_rows: int = 100_000
    buffer_ttl_seconds: int = 3600
    log_level: str = "INFO"
    feature_output_dir: str = "./output"

    # Orderbook
    orderbook_depth_levels: int = 10

    # Metrics
    metrics_enabled: bool = True
    metrics_port: int = 8000

    # Market matching
    market_matching_config: str = "config/markets.yaml"

    # Wallet tracking
    wallet_tracking_addresses: str = ""
    wallet_tracking_poll_seconds: float = 60.0
    wallet_tracking_port: int = 8001
    wallet_tracking_pnl_timeframe: str = "7d"
    wallet_tracking_output_dir: str = "./output/wallets"


def get_settings() -> Settings:
    return Settings()
