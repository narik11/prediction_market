# Prediction Collector

High-performance prediction market data collector focused on Polymarket BTC 15-minute up/down markets. Collects 1-second aggregated orderbook and trade data with live oracle price feeds.

## Overview

This collector captures real-time market data from Polymarket's WebSocket API, aggregates it into 1-second buckets, and writes to daily-rotating Parquet files. Kalshi integration is stubbed but currently disabled in production.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Polymarket WS  │────▶│  1s Aggregation │────▶│  Buffer Manager │
│   (WebSocket)   │     │   (Schema)      │     │  (asyncio.Queue)│
└─────────────────┘     └─────────────────┘     └────────┬────────┘
         │                                               │
         │                                               │
         ▼                                               ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  RTDS Oracle    │────▶│   Parquet       │◄────│   Flush every   │
│  (BTC/USD Price)│     │   Storage       │     │   30-60s        │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Quick Start

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure:** Copy `config.json` and edit as needed:
   ```bash
   cp config.json.example config.json
   # Edit config.json with your settings
   ```

3. **Run:**
   ```bash
   python -m src.main
   ```

## Configuration (config.json)

### Data Sources

```json
{
  "sources": {
    "polymarket": {
      "enabled": true,
      "ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
      "rest_url": "https://clob.polymarket.com",
      "markets": {"btc_15min": "btc-15min-updown"}
    },
    "kalshi": {
      "enabled": false,
      ...
    }
  }
}
```

### Buffer Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_rows_per_source` | 10000 | Max buffered rows per source |
| `flush_threshold` | 1000 | Flush when buffer reaches this size |
| `flush_interval_sec` | 30 | Maximum time between flushes |

### Rotation Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `check_interval_sec` | 30 | Seconds between rotation checks |
| `fast_check_interval_sec` | 2 | Fast check interval (within 1min of expiry) |
| `buffer_min_sec` | 5 | Min seconds before expiry to rotate |
| `buffer_max_sec` | 15 | Max seconds before expiry to rotate |
| `min_rotation_interval_sec` | 600 | Minimum seconds between rotations |

### Storage Settings

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rotation_utc_hour` | 0 | UTC hour for daily file rotation (0=midnight UTC) |
| `compression` | "zstd" | Parquet compression codec |
| `row_group_size` | 10000 | Rows per Parquet row group |

## Data Schema

### 1-Second Aggregated Tick Data

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | datetime | 1-second bucket UTC timestamp |
| `received_at` | datetime | Local time when message was received |
| `market_question` | string | Full market question text |
| `price_to_beat` | float | BTC price at window start (from oracle) |
| `oracle_price` | float | Current BTC/USD oracle price |
| `oracle_price_timestamp` | datetime | Oracle price timestamp |
| `yes_best_bid` | float | YES token best bid price |
| `yes_best_bid_size` | float | YES token best bid size |
| `yes_best_ask` | float | YES token best ask price |
| `yes_best_ask_size` | float | YES token best ask size |
| `no_best_bid` | float | NO token best bid price |
| `no_best_bid_size` | float | NO token best bid size |
| `no_best_ask` | float | NO token best ask price |
| `no_best_ask_size` | float | NO token best ask size |
| `yes_last_trade_price` | float | YES last trade price (carried forward) |
| `yes_last_trade_size` | float | YES last trade size |
| `no_last_trade_price` | float | NO last trade price (carried forward) |
| `no_last_trade_size` | float | NO last trade size |

## Key Components

### RTDS Client (`src/polymarket/rtds.py`)

WebSocket client connecting to `wss://ws-live-data.polymarket.com` for live BTC/USD oracle prices from Chainlink.

**Features:**
- Binary search (`bisect`) for historical price lookups O(log n)
- 20-second oracle queue buffer for `price_to_beat` calculation
- Price history (1 hour) for retrospective queries

**Key Methods:**
- `get_first_price_after(timestamp)` - First price at or after timestamp (for market start)
- `get_price_at_time(timestamp)` - Closest price to timestamp
- `get_oracle_price()` - Latest BTC/USD price

### Buffer Manager (`src/buffer.py`)

Async batching with `asyncio.Queue` for zero-copy data handoff.

**Features:**
- Concurrent batch puts via `asyncio.gather()`
- Size-based and time-based flush triggers
- Per-source buffering with separate schemas

### Storage (`src/storage.py`)

Parquet output with fastparquet true-append support.

**Features:**
- Daily file rotation at UTC midnight
- Compound key deduplication (`timestamp|market_question`)
- Single-pass DataFrame building (~40% CPU reduction vs double-iteration)
- Null-safe record handling

### Polymarket Client (`src/polymarket/client.py`)

WebSocket client with automatic market rotation for 15-min BTC markets.

**Features:**
- Slug-based market discovery (`btc-updown-15m-{timestamp}`)
- Adaptive rotation checking (30s normal, 2s near expiry)
- Token-level message filtering (discards old market messages after rotation)
- Atomic token capture for race-free subscriptions

## Market Rotation Logic

1. **Discovery:** Queries Gamma API for markets by slug pattern
2. **Price Capture:** Gets `price_to_beat` from RTDS oracle at market start time
3. **Rotation Window:** Rotates 5-15 seconds before expiry or 0-30 seconds after
4. **Throttling:** Minimum 10 minutes between rotations (with exception for expired markets)
5. **Buffer Reset:** Clears aggregation buffer on rotation to prevent cross-contamination

## Logging

Log files rotate daily at UTC midnight with 3-day retention:

| Logger | File | Purpose |
|--------|------|---------|
| `collector` | `logs/collector.log` | Main operational logs |
| `polymarket.connection` | `logs/polymarket/connection.log` | WebSocket events (no propagation) |
| `collector_errors` | `logs/errors/collector_errors.log` | Errors only (10-day retention) |

## Known Limitations

### WebSocket Data Gaps

Polymarket WebSocket can experience 6-34 second gaps. **Safety rules for arbitrage:**

- **NO TRADE** if data is >3 seconds stale
- **NO TRADE** if gap occurred <10 seconds ago
- **USE REST API** fallback for critical decisions
- **DUAL-FEED** validation recommended

### UTC Midnight File Rotation

Data files rotate at UTC midnight (6:00 PM CST / 7:00 PM CDT), not local midnight.

### Kalshi Integration

Kalshi support is stubbed but currently disabled. Only Polymarket is actively collecting.

## Performance Optimizations

Recent improvements (2026-02-15 to 2026-02-17):

1. **RTDS:** Changed `_queue_max_age` from 10s → 20s to cover rotation gaps
2. **Storage:** Added null check guard for `write_records()`
3. **Storage:** Batch deduplication with compound key (`timestamp|market_question`)
4. **Buffer:** `asyncio.gather()` for concurrent batch puts (O(n) → O(1))
5. **Storage:** Single-pass DataFrame building (~40% CPU reduction)
6. **RTDS:** Binary search (`bisect`) for oracle price lookup O(n) → O(log n)

## Directory Structure

```
prediction_collector/
├── src/
│   ├── main.py                 # Entry point & orchestration
│   ├── config.py               # Configuration management
│   ├── buffer.py               # Async batching
│   ├── storage.py              # Parquet I/O
│   └── polymarket/
│       ├── client.py           # WebSocket & rotation
│       ├── rtds.py             # Oracle price feed
│       └── schema.py           # 1s aggregation & normalization
├── config.json                 # Main configuration
├── data/                       # Output Parquet files
│   └── polymarket_YYYY-MM-DD.parquet
└── logs/                       # Rotated log files
    ├── collector.log
    ├── polymarket/
    │   └── connection.log
    └── errors/
        └── collector_errors.log
```

## Shutdown Behavior

Graceful shutdown sequence:

1. Stop WebSocket clients (no new data)
2. Flush completed seconds from schema buffer
3. Stop BufferManager (flushes remaining)
4. Close storage writers

## Stats & Monitoring

Runtime statistics available via `PredictionCollector.get_stats()`:

```python
{
  "running": True,
  "uptime_sec": 3600,
  "records_written": 123456,
  "last_flush": "2026-02-17T21:00:00Z",
  "polymarket": {
    "messages_received": 5000000,
    "ticks_normalized": 4500000,
    "subscribed_tokens": 2,
    "connected": True,
    "current_market_ends_in_sec": 450
  },
  "storage": {
    "sources": ["polymarket"],
    "records_written": {"polymarket": 123456}
  }
}
```

## License

MIT
