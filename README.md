# Alpha Pipeline

Prediction market alpha pipeline — real-time orderbook ingestion, feature computation, and monitoring for cross-exchange arbitrage detection.

## Architecture

```
                          Exchange WebSockets / REST APIs
                    ┌──────────┬──────────────┬──────────────┐
                    │Polymarket│   Opinion    │  Limitless   │
                    │  (WS)    │   (REST)     │ (Socket.IO)  │
                    └────┬─────┴──────┬───────┴──────┬───────┘
                         │            │              │
                    NormalizedOrderbook / NormalizedTrade
                    (event_id, sequence_num, timestamps)
                         │            │              │
                    ┌────▼────────────▼──────────────▼───────┐
                    │           DataManager                  │
                    │  • Sequence validation (per market)    │
                    │  • Timestamp preservation              │
                    │  • TTL-based eviction (every 60s)      │
                    └────┬────────────────────────┬──────────┘
                         │                        │
              ┌──────────▼──────────┐   ┌────────▼─────────┐
              │  Orderbook Buffers  │   │  Trade Buffers   │
              │  (per market, 100K) │   │  (per market)    │
              │  Ring buffer + TTL  │   │  Ring buffer     │
              └──────────┬──────────┘   └────────┬─────────┘
                         │                        │
                         └───────────┬────────────┘
                                     │
                              Event Queue (FIFO)
                           {type, market_id, event_id}
                                     │
                    ┌────────────────▼────────────────────┐
                    │          FeatureRunner              │
                    │  • Consumes events sequentially     │
                    │  • Generates correlation_id (UUID)  │
                    │  • Binds to structlog context       │
                    │  • Runs features in topo order      │
                    └────────────────┬────────────────────┘
                                     │
                              FeatureVector
                    (correlation_id, trigger_event_ids)
                                     │
                    ┌────────────────▼────────────────────┐
                    │                                      │
              ┌─────▼──────┐                    ┌─────────▼────────┐
              │  Parquet    │                    │  Prometheus      │
              │  Collector  │                    │  Metrics (:8000) │
              │  (chunked)  │                    │  → Grafana       │
              └─────────────┘                    └──────────────────┘
```

## Data Flow

### Ingestion

Three exchange adapters normalize raw market data into canonical schemas:

| Adapter | Protocol | Connection | Sequence Tracking |
|---------|----------|------------|-------------------|
| **Polymarket** | WebSocket CLOB | `wss://ws-subscriptions-clob.polymarket.com` | Per-asset monotonic counter |
| **Opinion** | REST polling | `https://api.opinion.xyz` | Default (0) |
| **Limitless** | Socket.IO | `wss://api.limitless.exchange` | Default (0) |

All adapters implement the `ExchangeAdapter` protocol:

```python
async def connect() -> None
async def subscribe(market_ids: list[str]) -> None
async def stream_orderbooks() -> AsyncIterator[NormalizedOrderbook]
async def stream_trades() -> AsyncIterator[NormalizedTrade]
async def disconnect() -> None
```

### Schemas

**NormalizedOrderbook** — Immutable, frozen Pydantic model:
- `event_id` (UUID4) — unique event identifier
- `sequence_num` (int) — per-asset monotonic counter for ordering validation
- `bids` / `asks` — sorted `OrderbookLevel` tuples (price, size)
- `local_timestamp` — when the pipeline received the event
- `exchange_timestamp` — when the exchange reports the event occurred (nullable)
- Computed: `best_bid`, `best_ask`, `mid_price`, `spread`

**NormalizedTrade** — Same traceability fields plus `price`, `size`, `side`, `usd_notional`.

### Buffering

**TimeSeriesBuffer** — Per-market ring buffer backed by `collections.deque`:
- Max capacity: 100,000 rows (configurable, auto-evicts oldest on overflow)
- TTL: 3,600s (manual eviction every 60s removes stale records)
- Export: `to_polars()` converts to DataFrame for feature computation

### Timestamp Model

Three-tier timestamps prevent information loss:

| Field | Set By | Purpose |
|-------|--------|---------|
| `exchange_timestamp` | Exchange | Original event time from source |
| `local_timestamp` | Adapter | When pipeline received the event |
| `ingestion_ts` | DataManager | When record entered the buffer |

## Features

Eight registered features across six categories, computed on every data event:

### Order Flow

| Feature | Output Keys | Description |
|---------|-------------|-------------|
| **TOB Imbalance** | `tob_imbalance`, `tob_imbalance_ma`, `tob_imbalance_zscore` | Top-of-book bid/ask size ratio with rolling mean and z-score. Strongest short-term alpha signal. |
| **Buy/Sell Imbalance** | `buy_sell_ratio`, `net_flow`, `buy_volume`, `sell_volume`, `trade_count` | Rolling taker buy/sell volume ratio. Captures retail directional bias. |
| **Wash Detection** | `wash_trade_ratio`, `inside_spread_count`, `total_trade_count`, `wash_volume_ratio` | Identifies trades printing inside the spread — impossible in legitimate order books. |

### Spread Analysis

| Feature | Output Keys | Description |
|---------|-------------|-------------|
| **Spread Dynamics** | `current_spread`, `spread_pct`, `spread_percentile`, `is_widening`, `avg_spread` | BBA spread analysis with percentile rank and widening detection. |

### Pricing

| Feature | Output Keys | Description |
|---------|-------------|-------------|
| **Binary Implied Prob** | `implied_prob`, `edge_vs_50`, `fair_spread`, `complement_edge` | Mid-price as probability, distance from max uncertainty, theoretical fair spread. |

### Toxicity

| Feature | Output Keys | Description |
|---------|-------------|-------------|
| **Markouts** | `markout_1s`, `markout_5s`, `markout_30s`, `markout_60s`, `trade_price`, `trade_side` | Post-fill price change at 1s/5s/30s/60s horizons. Positive = toxic (informed) flow. |

### Size Signals

| Feature | Output Keys | Description |
|---------|-------------|-------------|
| **Avg Order Size** | `avg_trade_size`, `size_zscore`, `is_retail_signal`, `trade_count` | Z-score normalized trade size flags unusual institutional or retail activity. |

### Cross-Exchange

| Feature | Output Keys | Description |
|---------|-------------|-------------|
| **Arb Spread** | `arb_spread`, `arb_spread_bps`, `bid_exchange`, `ask_exchange` | Core alpha: spread between best bid on one exchange and best ask on another. Positive = live arbitrage opportunity. |

### Feature Engine Internals

- **Registration**: `@register_feature` decorator auto-registers `Feature` subclasses
- **Registry**: `FeatureRegistry` manages enable/disable and topological sorting via `depends_on`
- **Runner**: `FeatureRunner` consumes events sequentially, generates a `correlation_id` per cycle, binds it to structlog context, and attaches it to all outputs
- **Computation**: Each feature receives Polars DataFrames from market-specific buffers and applies its own windowing (time-based or row-based)

## Traceability

End-to-end correlation from market tick to persisted output:

```
Market Tick
  └─ event_id (UUID4)           ← assigned at adapter
  └─ sequence_num (monotonic)   ← per-asset counter
       │
       ▼
Event Queue
  └─ event_id propagated
       │
       ▼
FeatureRunner
  └─ correlation_id (UUID4)     ← generated per computation cycle
  └─ trigger_event_ids          ← which event(s) triggered this
  └─ structlog.bind_contextvars(correlation_id=...)
       │
       ▼
Parquet Output
  └─ correlation_id             ← in every row
  └─ trigger_event_ids          ← JSON array linking to source events
```

**Sequence validation**: DataManager tracks `last_sequence` per market and logs warnings on gaps or reordering (never crashes the pipeline).

## Monitoring

### Prometheus Metrics

Exposed at `:8000/metrics`, scraped every 5s:

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pipeline_feature_value` | Gauge | feature_name, market_id, value_key | Current value of each computed feature |
| `pipeline_events_processed_total` | Counter | event_type, exchange | Total data events ingested |
| `pipeline_feature_vectors_total` | Counter | market_id, exchange | Total feature vectors emitted |
| `pipeline_features_computed_total` | Counter | feature_name | Total individual features computed |
| `pipeline_feature_errors_total` | Counter | feature_name | Total feature computation errors |
| `pipeline_buffer_size` | Gauge | buffer_type, market_id | Current rows in ring buffer |
| `pipeline_queue_size` | Gauge | queue_name | Event/output queue depth |
| `pipeline_connection_up` | Gauge | exchange | Connection status (1=UP, 0=DOWN) |

### Grafana Dashboard

Auto-provisioned at `localhost:3000` (anonymous admin access). Three rows:

**Row 1 — Market Microstructure** (3 panels):

| Panel | Metric | Unit | Axis | Description |
|-------|--------|------|------|-------------|
| **TOB Imbalance** | `tob_imbalance` | ratio [0, 1] | left | Bid size / (bid + ask size). 0.5 = neutral, >0.5 = buying pressure |
| | `tob_imbalance_ma` | ratio [0, 1] | left | Rolling mean over lookback window |
| | `tob_imbalance_zscore` | standard deviations | right (orange dashed) | How many stdev current imbalance deviates from rolling mean |
| **Spread** | `current_spread` | price units (e.g. 0.02 = 2 cents) | left | Absolute best_ask - best_bid |
| | `avg_spread` | price units | left | Mean spread over rolling window |
| | `spread_percentile` | percentile [0, 1] | right (purple dashed) | Rank of current spread vs history. 0.9 = wider than 90% of recent spreads |
| **Implied Probability** | `implied_prob` | probability [0, 1] | left | Mid-price interpreted as event probability |
| | `edge_vs_50` | probability delta | right (yellow dashed) | abs(implied_prob - 0.5). Distance from maximum uncertainty |

**Row 2 — Order Flow & Toxicity** (3 panels):

| Panel | Metric | Unit | Axis | Description |
|-------|--------|------|------|-------------|
| **Buy/Sell Flow** | `buy_volume` | contracts | left (stacked area) | Total buy-side volume in rolling window |
| | `sell_volume` | contracts | left (stacked area) | Total sell-side volume in rolling window |
| | `buy_sell_ratio` | ratio [0, 1] | right (white line) | buy_volume / (buy + sell). 0.5 = balanced |
| **Trade Size** | `avg_trade_size` | contracts | left | Mean trade size in rolling window |
| | `size_zscore` | standard deviations | right (orange) | Z-score of latest trade size vs window mean/stdev |
| | `is_retail_signal` | boolean (0/1) | left (cyan points) | 1 when abs(size_zscore) > threshold (default 2.0) |
| **Toxicity** | `markout_1s` | price delta | left | Price change 1s after trade, adjusted for side (+= toxic) |
| | `markout_5s` | price delta | left | Price change at 5s horizon |
| | `markout_30s` | price delta | left | Price change at 30s horizon |
| | `markout_60s` | price delta | left | Price change at 60s horizon |
| | `wash_trade_ratio` | ratio [0, 1] | right (red dashed) | Fraction of trades printing inside the spread |

**Row 3 — Pipeline Health** (collapsed by default, 4 panels):

| Panel | Metric | Unit | Description |
|-------|--------|------|-------------|
| **Throughput** | `rate(pipeline_features_computed_total[1m])` | ops/sec | Feature computations per second, broken down by feature_name |
| **Errors** | `rate(pipeline_feature_errors_total[1m])` | ops/sec | Error rate per feature (red bars) |
| **Buffers & Queues** | `pipeline_buffer_size` | rows | Current rows in orderbook/trade ring buffers per market |
| | `pipeline_queue_size` | items | Current depth of event and output asyncio queues |
| **Connection** | `pipeline_connection_up` | binary | 1 = UP (green), 0 = DOWN (red) per exchange |

### Infrastructure

```bash
make start      # docker compose up -d (Prometheus + Grafana)
make stop       # docker compose down
make dashboard  # open http://localhost:3000
make pipeline MARKETS="market_id_1 market_id_2"
```

Docker Compose runs Prometheus (`:9090`) and Grafana (`:3000`) with `host.docker.internal` bridging to reach the pipeline's metrics endpoint on the host.

## Persistence & Analysis

### Parquet Output

Feature vectors are persisted as date-partitioned Parquet files:

```
output/
├── features_2026-03-05_141030.parquet
├── features_2026-03-05_143512.parquet
└── ...
```

Schema: `timestamp`, `market_id`, `exchange`, `feature_name`, `values_json`, `correlation_id`, `trigger_event_ids`

Auto-flush every 100 vectors (configurable).

### Post-Hoc Analysis

```bash
# Generate charts from persisted data
python scripts/analyze_features.py output/ -m MARKET_ID -o charts/

# With time range
python scripts/analyze_features.py output/ --start 2026-03-05T10:00 --end 2026-03-05T18:00
```

Chart modules exist for each feature category (order flow, toxicity, pricing, spread, size signals, cross-exchange).

## Configuration

All settings via environment variables or `.env` file (Pydantic Settings):

```bash
# Exchange connections
POLYMARKET_WS_URL=wss://ws-subscriptions-clob.polymarket.com/ws/market
OPINION_API_URL=https://api.opinion.xyz
OPINION_API_KEY=
LIMITLESS_WS_URL=wss://api.limitless.exchange
LIMITLESS_JWT_TOKEN=

# Pipeline
BUFFER_MAX_ROWS=100000
BUFFER_TTL_SECONDS=3600
ORDERBOOK_DEPTH_LEVELS=10
FEATURE_OUTPUT_DIR=./output
LOG_LEVEL=INFO

# Metrics
METRICS_ENABLED=true
METRICS_PORT=8000
```

## Quick Start

```bash
# Install
pip install -e ".[analysis]"

# Start monitoring stack
make start

# Run pipeline
python scripts/run_pipeline.py <MARKET_ID_1> <MARKET_ID_2>

# Open dashboard
make dashboard

# Analyze persisted data
python scripts/analyze_features.py output/
```

## Project Structure

```
src/alpha_pipeline/
├── config.py                         # Pydantic settings
├── schemas/                          # Immutable data models
│   ├── enums.py                      # ExchangeId, Side, OutcomeType
│   ├── orderbook.py                  # NormalizedOrderbook, OrderbookLevel
│   ├── trade.py                      # NormalizedTrade
│   └── feature.py                    # FeatureSpec, FeatureOutput, FeatureVector
├── data/
│   ├── adapters/
│   │   ├── base.py                   # ExchangeAdapter protocol
│   │   ├── polymarket.py             # WebSocket CLOB adapter
│   │   ├── opinion.py                # REST polling adapter
│   │   └── limitless.py              # Socket.IO adapter
│   ├── manager.py                    # DataManager (orchestration, sequence validation)
│   ├── buffer.py                     # TimeSeriesBuffer (ring buffer + TTL)
│   └── collector.py                  # FeatureCollector (Parquet sink)
├── features/
│   ├── base.py                       # Feature ABC, @register_feature
│   ├── registry.py                   # FeatureRegistry (enable/disable, topo sort)
│   ├── runner.py                     # FeatureRunner (event loop, correlation binding)
│   └── categories/
│       ├── order_flow/               # tob_imbalance, buy_sell_imbalance, wash_detection
│       ├── spread_analysis/          # spread_dynamics
│       ├── pricing/                  # binary_implied_prob
│       ├── toxicity/                 # markouts
│       ├── size_signals/             # avg_order_size
│       └── cross_exchange/           # arb_spread
├── metrics/
│   ├── collector.py                  # MetricsCollector
│   └── exporter.py                   # Prometheus metric definitions
├── analysis/
│   ├── loader.py                     # Load Parquet → FeatureVectors
│   ├── report.py                     # Chart generation orchestrator
│   └── charts/                       # Per-feature visualization modules
└── utils/
    └── logging.py                    # structlog setup, correlation ID helpers
```

## Dependencies

- **Runtime**: websockets, python-socketio, aiohttp, polars, pydantic, structlog, prometheus_client, orjson, uvloop
- **Analysis**: matplotlib
- **Dev**: pytest, pytest-asyncio, hypothesis, pytest-cov
