from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Polymarket
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

    # Opinion
    opinion_api_url: str = "https://api.opinion.xyz"
    opinion_api_key: str = ""
    opinion_poll_interval_seconds: float = 2.0

    # Limitless
    limitless_ws_url: str = "wss://api.limitless.exchange"
    limitless_jwt_token: str = ""

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


def get_settings() -> Settings:
    return Settings()
