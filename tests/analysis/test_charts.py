"""Tests for chart generation — verifies all 8 PNGs are produced."""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from alpha_pipeline.analysis.loader import load_feature_vectors, vectors_to_feature_frames
from alpha_pipeline.analysis.report import generate_report
from alpha_pipeline.schemas.enums import ExchangeId
from alpha_pipeline.schemas.feature import FeatureOutput, FeatureVector

# Number of synthetic data points
N = 60


def _ts(i: int) -> datetime:
    """Generate a timestamp offset by *i* minutes from a fixed base."""
    base = datetime(2026, 3, 5, 10, 0, tzinfo=timezone.utc)
    return base + timedelta(minutes=i)


def _make_arb(i: int, ts: datetime) -> FeatureOutput:
    spread = math.sin(i / 10) * 50
    return FeatureOutput(
        feature_name="cross_exchange.arb_spread",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "arb_spread": spread / 10000,
            "arb_spread_bps": spread,
            "bid_exchange": "polymarket",
            "ask_exchange": "opinion",
        },
    )


def _make_tob(i: int, ts: datetime) -> FeatureOutput:
    imb = 0.5 + 0.3 * math.sin(i / 8)
    return FeatureOutput(
        feature_name="order_flow.tob_imbalance",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "tob_imbalance": imb,
            "tob_imbalance_ma": 0.5 + 0.2 * math.sin(i / 12),
            "tob_imbalance_zscore": (imb - 0.5) / 0.1,
        },
    )


def _make_buy_sell(i: int, ts: datetime) -> FeatureOutput:
    buy = 500 + random.random() * 200
    sell = 400 + random.random() * 200
    return FeatureOutput(
        feature_name="order_flow.buy_sell_imbalance",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "buy_sell_ratio": buy / (buy + sell),
            "net_flow": buy - sell,
            "buy_volume": buy,
            "sell_volume": sell,
            "trade_count": random.randint(5, 30),
        },
    )


def _make_pricing(i: int, ts: datetime) -> FeatureOutput:
    prob = 0.5 + 0.2 * math.sin(i / 15)
    return FeatureOutput(
        feature_name="pricing.binary_implied_prob",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "implied_prob": prob,
            "edge_vs_50": prob - 0.5,
            "fair_spread": 0.02 + random.random() * 0.01,
            "complement_edge": 0.5 - prob,
        },
    )


def _make_size(i: int, ts: datetime) -> FeatureOutput:
    size = 80 + 40 * math.sin(i / 6)
    return FeatureOutput(
        feature_name="size_signals.avg_order_size",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "avg_trade_size": size,
            "size_zscore": (size - 100) / 20,
            "is_retail_signal": size < 60,
            "trade_count": random.randint(3, 20),
        },
    )


def _make_spread(i: int, ts: datetime) -> FeatureOutput:
    spread = 0.02 + 0.01 * math.sin(i / 7)
    avg = 0.025
    return FeatureOutput(
        feature_name="spread_analysis.spread_dynamics",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "current_spread": spread,
            "spread_pct": spread / 0.56 * 100,
            "spread_percentile": min(1.0, max(0.0, spread / 0.04)),
            "is_widening": spread > avg,
            "avg_spread": avg,
        },
    )


def _make_markout(i: int, ts: datetime) -> FeatureOutput:
    base = random.random() * 10 - 5
    return FeatureOutput(
        feature_name="toxicity.markouts",
        timestamp=ts,
        market_id="mkt_test",
        values={
            "markout_1s": base + random.random() * 2,
            "markout_5s": base * 1.5 + random.random() * 3,
            "markout_30s": base * 2 + random.random() * 5,
            "markout_60s": base * 2.5 + random.random() * 8,
            "trade_price": 0.55 + random.random() * 0.05,
            "trade_side": "buy" if i % 2 == 0 else "sell",
        },
    )


@pytest.fixture
def synthetic_jsonl(tmp_path) -> Path:
    """Generate a JSONL file with all 7 features across N data points."""
    random.seed(42)
    path = tmp_path / "features_2026-03-05.jsonl"
    with open(path, "wb") as f:
        for i in range(N):
            ts = _ts(i)
            vec = FeatureVector(
                timestamp=ts,
                market_id="mkt_test",
                exchange=ExchangeId.POLYMARKET,
                features=(
                    _make_arb(i, ts),
                    _make_tob(i, ts),
                    _make_buy_sell(i, ts),
                    _make_pricing(i, ts),
                    _make_size(i, ts),
                    _make_spread(i, ts),
                    _make_markout(i, ts),
                ),
            )
            f.write(vec.to_json_bytes() + b"\n")
    return path


def test_all_charts_generated(synthetic_jsonl, tmp_path):
    """generate_report produces all 8 PNGs (7 features, markouts = 2)."""
    chart_dir = tmp_path / "charts"
    saved = generate_report([synthetic_jsonl], chart_dir)

    assert len(saved) == 8

    names = {p.stem for p in saved}
    expected = {
        "arb_spread",
        "tob_imbalance",
        "buy_sell_imbalance",
        "binary_implied_prob",
        "avg_order_size",
        "spread_dynamics",
        "markouts_timeseries",
        "markouts_curve",
    }
    assert names == expected

    # All files exist and are non-empty
    for p in saved:
        assert p.exists()
        assert p.stat().st_size > 0


def test_filter_by_market(synthetic_jsonl, tmp_path):
    """Charts only include filtered market data."""
    chart_dir = tmp_path / "charts"
    saved = generate_report(
        [synthetic_jsonl], chart_dir, market_id="nonexistent"
    )
    assert saved == []


def test_single_data_point(tmp_path):
    """Charts handle a single data point without crashing."""
    ts = _ts(0)
    random.seed(42)
    vec = FeatureVector(
        timestamp=ts,
        market_id="mkt_test",
        exchange=ExchangeId.POLYMARKET,
        features=(
            _make_arb(0, ts),
            _make_tob(0, ts),
            _make_buy_sell(0, ts),
            _make_pricing(0, ts),
            _make_size(0, ts),
            _make_spread(0, ts),
            _make_markout(0, ts),
        ),
    )
    jsonl = tmp_path / "single.jsonl"
    with open(jsonl, "wb") as f:
        f.write(vec.to_json_bytes() + b"\n")

    chart_dir = tmp_path / "charts"
    saved = generate_report([jsonl], chart_dir)
    # Should produce charts even with 1 point (markout curve may skip if < 2 samples)
    assert len(saved) >= 6


def test_empty_input(tmp_path):
    """Empty file produces no charts."""
    empty = tmp_path / "empty.jsonl"
    empty.touch()
    chart_dir = tmp_path / "charts"
    saved = generate_report([empty], chart_dir)
    assert saved == []
