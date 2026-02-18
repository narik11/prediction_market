# Real-Time Prediction Market Data Collector

## Overview
A Python real-time data collection system that captures raw 1-second data from Polymarket and Kalshi BTC Up/Down 15-minute markets and stores it in Parquet files. No processing, no arbitrage logic, no calculations - just capture and store.

---

## Data Sources (Native Only)

### Polymarket (Native APIs)

| Component | Endpoint |
|-----------|----------|
| Market Discovery | `https://gamma-api.polymarket.com/markets?active=true&tag=BTC` |
| WebSocket | `wss://ws-subscriptions-clob.polymarket.com/ws/market` |
| REST Order Book | `https://clob.polymarket.com/book?token_id={TOKEN_ID}` |
| Oracle Price | From WebSocket `oracle_price` or `reference_price` field |

### Kalshi (Native APIs)

| Component | Endpoint |
|-----------|----------|
| Market Discovery | `https://api.elections.kalshi.com/trade-api/v2/series/KXBTC15M/markets` |
| WebSocket | `wss://api.elections.kalshi.com/trade-api/ws/v2` |
| REST Order Book | `https://api.elections.kalshi.com/trade-api/v2/markets/{TICKER}/orderbook` |
| Oracle Price | From market metadata `price_to_beat` or WebSocket `last_price` |

---

## Raw Data Schema

### Polymarket Raw Schema

```json
{
  "timestamp": "2026-02-13T14:23:45.123Z",
  "unix_ms": 1739456625123,
  "source": "polymarket",
  "market_id": "0x...",
  "condition_id": "0x...",
  "window_start": "2026-02-13T14:15:00.000Z",
  "window_end": "2026-02-13T14:30:00.000Z",
  "price_to_beat": 96541.80,
  "oracle_price": 96542.50,
  "token_yes_id": "34097058...",
  "token_no_id": "34097058...",
  "yes_bids": [{"price": 0.62, "size": 1250.5}, ...],
  "yes_asks": [{"price": 0.63, "size": 980.0}, ...],
  "no_bids": [{"price": 0.37, "size": 850.0}, ...],
  "no_asks": [{"price": 0.38, "size": 1100.0}, ...],
  "yes_last_trade": {"price": 0.625, "size": 150.0, "time": "..."},
  "no_last_trade": {"price": 0.375, "size": 200.0, "time": "..."},
  "raw_message": "{...}"
}
```

### Kalshi Raw Schema

```json
{
  "timestamp": "2026-02-13T14:23:45.123Z",
  "unix_ms": 1739456625123,
  "source": "kalshi",
  "ticker": "KXBTC15M-26FEB131430",
  "series_ticker": "KXBTC15M",
  "window_start": "2026-02-13T14:15:00.000Z",
  "window_end": "2026-02-13T14:30:00.000Z",
  "price_to_beat": 96537.50,
  "oracle_price": 96538.25,
  "yes_bids": [{"price": 61, "contracts": 500}, ...],
  "no_bids": [{"price": 38, "contracts": 750}, ...],
  "yes_last_trade": {"price": 61.5, "contracts": 100, "time": "..."},
  "no_last_trade": {"price": 38.5, "contracts": 150, "time": "..."},
  "ticker_data": {"yes_ask": 62, "yes_bid": 61, "last_price": 61.5, ...},
  "raw_message": "{...}"
}
```

## Storage Format (Apache Parquet)

### File Structure

```
./data/
├── polymarket/
│   ├── raw_2026-02-13.parquet
│   └── raw_2026-02-14.parquet
└── kalshi/
    ├── raw_2026-02-13.parquet
    └── raw_2026-02-14.parquet
```

### Parquet Schema (Polymarket)

| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp[ns] | ISO8601 with nanoseconds |
| unix_ms | int64 | Epoch milliseconds |
| market_id | string | Polymarket condition ID |
| window_start | timestamp[ns] | 15m window start |
| window_end | timestamp[ns] | 15m window end |
| price_to_beat | float64 | Window start BTC price |
| oracle_price | float64 | Current BTC oracle price |
| yes_best_bid | float64 | Top YES bid price |
| yes_best_bid_size | float64 | Top YES bid size |
| yes_best_ask | float64 | Top YES ask price |
| yes_best_ask_size | float64 | Top YES ask size |
| no_best_bid | float64 | Top NO bid price |
| no_best_bid_size | float64 | Top NO bid size |
| no_best_ask | float64 | Top NO ask price |
| no_best_ask_size | float64 | Top NO ask size |
| yes_last_trade_price | float64 | Last YES trade |
| yes_last_trade_size | float64 | Last YES trade size |
| no_last_trade_price | float64 | Last NO trade |
| no_last_trade_size | float64 | Last NO trade size |
| full_orderbook_yes | string | JSON string of all YES bids/asks |
| full_orderbook_no | string | JSON string of all NO bids/asks |
| raw_ws_message | string | Full original WebSocket message |

### Parquet Schema (Kalshi)

| Column | Type | Description |
|--------|------|-------------|
| timestamp | timestamp[ns] | ISO8601 with nanoseconds |
| unix_ms | int64 | Epoch milliseconds |
| ticker | string | Full market ticker |
| window_start | timestamp[ns] | 15m window start |
| window_end | timestamp[ns] | 15m window end |
| price_to_beat | float64 | Window start BTC price |
| oracle_price | float64 | Current BTC oracle price |
| yes_best_bid | float64 | Top YES bid price (cents/100) |
| yes_best_bid_size | int64 | Top YES bid contracts |
| no_best_bid | float64 | Top NO bid price (cents/100) |
| no_best_bid_size | int64 | Top NO bid contracts |
| yes_last_trade_price | float64 | Last YES trade (cents/100) |
| yes_last_trade_size | int64 | Last YES trade contracts |
| no_last_trade_price | float64 | Last NO trade (cents/100) |
| no_last_trade_size | int64 | Last NO trade contracts |
| full_orderbook_yes | string | JSON string of all YES bids |
| full_orderbook_no | string | JSON string of all NO bids |
| ticker_channel_data | string | JSON of last ticker message |
| raw_ws_message | string | Full original WebSocket message |

---

## Implementation Requirements

### 1. Market Discovery (Run Once at Startup)

**Polymarket:**
- GET `https://gamma-api.polymarket.com/markets?active=true&tag=BTC&limit=100`
- Filter: slug contains "15m" OR description contains "15 minute"
- Extract: condition_id, token_ids (yes/no), start_date, end_date, price_to_beat

**Kalshi:**
- GET `https://api.elections.kalshi.com/trade-api/v2/series/KXBTC15M/markets?status=open`
- Extract: ticker, open_time, close_time, price_to_beat

### 2. WebSocket Connections (Asyncio)

**Polymarket WebSocket:**
- URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Subscribe: `{"type": "subscribe", "channel": "market", "condition_id": "{ID}"}`
- Messages: orderbook_update, trade, price_change, market_update
- Action: Parse and append to in-memory buffer

**Kalshi WebSocket:**
- URL: `wss://api.elections.kalshi.com/trade-api/ws/v2`
- Auth: HMAC SHA256 signature with API key + secret
- Subscribe to: ticker, orderbook_delta, trade channels
- Action: Parse and append to in-memory buffer

### 3. Data Buffering (In-Memory)

- Maintain two lists: `pm_buffer = []`, `kalshi_buffer = []`
- Each WebSocket message → create dict with timestamp → append to buffer
- Buffer size limit: 10,000 rows per source (flush when exceeded)

### 4. Parquet Writing (PyArrow)

- Use pyarrow + pandas or fastparquet
- Write mode: Append to existing file or create new
- Compression: SNAPPY or ZSTD
- Row group size: 10,000 rows
- Flush trigger: Every 60 seconds OR buffer size > 5,000 rows

### 5. File Rotation

- New file every day: `raw_YYYY-MM-DD.parquet`
- Check at midnight UTC: close current file, open new file

### 6. Graceful Shutdown

- Handle SIGINT/SIGTERM
- On shutdown: flush remaining buffer to Parquet, close files, exit

---

## Configuration (config.json)

```json
{
  "polymarket": {
    "gamma_url": "https://gamma-api.polymarket.com",
    "ws_url": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "market_tag": "BTC",
    "filter_15m": true
  },
  "kalshi": {
    "rest_url": "https://api.elections.kalshi.com/trade-api/v2",
    "ws_url": "wss://api.elections.kalshi.com/trade-api/ws/v2",
    "api_key": "YOUR_API_KEY",
    "api_secret": "YOUR_SECRET",
    "series_ticker": "KXBTC15M"
  },
  "storage": {
    "base_path": "./data",
    "format": "parquet",
    "compression": "snappy",
    "flush_interval_seconds": 60,
    "buffer_size_limit": 5000,
    "rotate_daily": true
  }
}
```

---

## Key Requirements

- **No Processing Logic**: Do NOT calculate spreads, arbitrage, or derived fields
- **Store Raw Messages**: Save full WebSocket JSON as string in raw_ws_message column
- **Store Full Orderbook**: Serialize entire bid/ask array to JSON string
- **Timestamps**: Use nanosecond precision (pandas timestamp[ns])
- **File Per Day**: Create new Parquet file at midnight UTC
- **Append Mode**: Add rows to existing Parquet file (not overwrite)
- **Compression**: Use SNAPPY for speed or ZSTD for size
- **Graceful Shutdown**: Flush buffer on SIGINT before exit

---

## Output Files

Data will be saved to:
- `./data/polymarket/raw_YYYY-MM-DD.parquet`
- `./data/kalshi/raw_YYYY-MM-DD.parquet`

---

## Notes

- Polymarket read-only access is public (no API key required)
- Kalshi requires API key + secret for WebSocket authentication
- Files rotate daily at midnight UTC
- Press Ctrl+C to stop gracefully (flushes remaining buffer)
