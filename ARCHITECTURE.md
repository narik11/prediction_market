# Prediction Collector Architecture

System architecture for the production prediction market data collector.

## System Overview

A high-throughput, low-latency data collection system focused on Polymarket BTC 15-minute prediction markets. Implements reliable 1-second aggregation with automatic market rotation and persistent Parquet output.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Prediction Collector                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌──────────────┐    ┌──────────────┐                       ┌──────────┐  │
│   │   RTDS       │    │  Polymarket  │                       │ Watchdog │  │
│   │   Oracle     │    │   WebSocket  │                       │(external)│  │
│   └──────┬───────┘    └──────┬───────┘                       └────┬─────┘  │
│          │                   │                                      │        │
│          │ BTC/USD           │ Market Data                         │        │
│          │ Price Feed        │ (books, trades)                     │        │
│          │                   │                                     │        │
│          ▼                   ▼                                     │        │
│   ┌─────────────────────────────────────────┐                      │        │
│   │    Schema Module (1s Aggregation)       │                      │        │
│   │  ┌─────────────────────────────────┐   │                      │        │
│   │  │ Token Metadata Mapping          │   │                      │        │
│   │  │ ┌─────────────────────────────┐ │   │                      │        │
│   │  │ │ Second Buffer (ts → data)   │ │   │                      │        │
│   │  │ └─────────────────────────────┘ │   │                      │        │
│   │  │ Last-Known Value Carry-Forward│   │                      │        │
│   │  └─────────────────────────────────┘   │                      │        │
│   └──────────────┬──────────────────────────┘                      │        │
│                  │ Completed Seconds                               │        │
│                  │ (every 30s)                                    │        │
│                  ▼                                                │        │
│   ┌────────────────────────┐                                      │        │
│   │   BufferManager        │◄─────────────────────────────────────┘        │
│   │   (asyncio.Queue)      │         Heartbeat File                        │
│   │  ┌────────────────┐  │         (30s updates)                         │
│   │  │ Inbound Queue   │  │                                              │
│   │  │ Size Triggers   │  │                                              │
│   │  │ Time Triggers   │  │                                              │
│   │  └────────────────┘   │                                              │
│   └──────────┬────────────┘                                              │
│              │ Flushed Records                                           │
│              │ (batch puts)                                                │
│              ▼                                                           │
│   ┌────────────────────────┐                                             │
│   │   StorageManager       │                                             │
│   │   (Parquet I/O)        │                                             │
│   │  ┌────────────────┐   │                                             │
│   │  │ Deduplication  │   │                                             │
│   │  │ (ts|market)    │   │                                             │
│   │  └────────────────┘   │                                             │
│   │  ┌────────────────┐   │                                             │
│   │  │ fastparquet    │   │                                             │
│   │  │ Append Writes  │   │                                             │
│   │  └────────────────┘   │                                             │
│   └──────────┬────────────┘                                             │
│              │                                                           │
│              ▼                                                           │
│   ┌──────────────────────────────────────────────────────────┐         │
│   │                  Data Files (UTC Daily)                   │         │
│   │  data/polymarket_YYYY-MM-DD.parquet                      │         │
│   └──────────────────────────────────────────────────────────┘         │
│                                                                           │
└─────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Ingestion Layer

#### RTDS Client (`polymarket/rtds.py`)
```
WebSocket (wss://ws-live-data.polymarket.com)
    │
    ├─► Chainlink BTC/USD price updates (every ~2s)
    │
    ├─► Binary search index for historical lookups
    │   - `_price_history`: 1-hour rolling window
    │   - `_oracle_queue`: 20-second buffer (for rotation)
    │
    └─► Schema.set_oracle_price(price, timestamp)
```

#### Polymarket Client (`polymarket/client.py`)
```
WebSocket (wss://ws-subscriptions-clob.polymarket.com/ws/market)
    │
    ├─► Orderbook updates (event_type: "book")
    │   ├─ bids[]: [price, size] pairs (best = last)
    │   └─ asks[]: [price, size] pairs (best = first for NO)
    │
    ├─► Trade updates (event_type: "last_trade_price", "trade")
    │   ├─ price: trade price
    │   └─ size: trade size
    │
    └─► Schema.normalize_ws_message(msg) ──► 1s buffer
```

### 2. Aggregation Layer

#### Schema 1-Second Buffer (`polymarket/schema.py`)

```python
# Per-second entry structure
_second_buffer: Dict[str, Dict] = {
    "2026-02-17T21:15:00": {
        "timestamp": datetime(2026, 2, 17, 21, 15, 0, tzinfo=UTC),
        "received_at": datetime(...),           # First message arrival time
        "market_question": "Bitcoin... 15min 21:15?",
        
        # Oracle data
        "price_to_beat": 96542.38,              # Window start price
        "oracle_price": 96587.21,               # Current BTC/USD
        "oracle_price_timestamp": datetime(...),
        
        # YES token (UP) orderbook
        "yes_best_bid": 0.52, "yes_best_bid_size": 1250.5,
        "yes_best_ask": 0.54, "yes_best_ask_size": 892.3,
        
        # NO token (DOWN) orderbook
        "no_best_bid": 0.46, "no_best_bid_size": 2100.0,
        "no_best_ask": 0.48, "no_best_ask_size": 1500.2,
        
        # Trades (last known carries forward)
        "yes_last_trade_price": 0.53, "yes_last_trade_size": 100.0,
        "no_last_trade_price": 0.47, "no_last_trade_size": 250.0,
        
        "_updated": True  # Flag for internal tracking
    }
}
```

#### Aggregation Rules

1. **Timestamp Bucketing**: Messages grouped by second (UTC)
2. **Carry-Forward**: Last known values preserved if no update in second
3. **Update Coalescing**: Multiple updates in same second = last wins per field
4. **Token Filter**: Messages for old markets discarded after rotation

### 3. Buffering Layer

#### BufferManager (`buffer.py`)

```
┌────────────────────────────────────────────────────────────┐
│                     BufferManager                          │
├────────────────────────────────────────────────────────────┤
│                                                            │
│  Inbound Queue (asyncio.Queue, max=10000)                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │  put_batch() ──► [asyncio.gather() concurrent puts]│  │
│  └────────────────────────────────────────────────────┘  │
│                         │                                   │
│                         ▼                                   │
│  Per-Source Buffers       │                                   │
│  ┌─────────────┐        │                                   │
│  │ polymarket  │◄───────┘                                   │
│  │ ┌─────────┐│                                            │
│  │ │ Record 1││  Flush Triggers:                           │
│  │ │ Record 2││   - Size: flush_threshold (1000)          │
│  │ │ Record 3││   - Time: flush_interval_sec (30s)         │
│  │ │   ...   ││                                            │
│  │ └─────────┘│                                            │
│  └──────┬─────┘                                            │
│         │                                                    │
│         └───────────► flush_callback(source, records)        │
│                                                            │
└────────────────────────────────────────────────────────────┘
```

### 4. Storage Layer

#### StorageManager (`storage.py`)

```
Incoming Batch (List[Dict])
    │
    ▼
┌────────────────────────────────────────────┐
│  write_records()                            │
│                                              │
│  1. NULL Check Guard ──► Skip if None       │
│                                              │
│  2. _filter_duplicates()                    │
│     ├─ Build compound key: timestamp|market │
│     ├─ Deduplicate within batch (keep last) │
│     └─ Filter against _written_timestamps   │
│                                              │
│  3. _records_to_pandas()                    │
│     └─ Single-pass DataFrame construction   │
│         ~40% CPU reduction vs double-loop   │
│                                              │
│  4. Write via fastparquet                   │
│     ├─ File exists? ──► append=True       │
│     └─ New file? ──► create                 │
└────────────────────────────────────────────┘
    │
    ▼
./data/polymarket_2026-02-17.parquet
```

#### Deduplication Logic

```python
# Compound key prevents duplicates across markets
key = f"{timestamp}|{market_question}"

# Within-batch dedup: keep last occurrence
seen_indices[key] = index  # Overwrite → last wins

# Cross-batch dedup: skip if in _written_timestamps
if ts_str in self._written_timestamps:
    skip record
```

## Market Rotation Flow

### Rotation Timeline (15-minute market)

```
    T-15min          T-60s           T-15s            T+0           T+30s
      │               │               │               │               │
      ▼               ▼               ▼               ▼               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ MARKET ACTIVE │  Fast Check  │  ROTATION   │   EXPIRY   │  NEW MARKET   │
│   (Collect)   │   Window     │   WINDOW    │            │   (Collect)   │
├─────────────────────────────────────────────────────────────────────────┤
│ • WebSocket   │ • 2s checks │ • Discover  │ • Tokens   │ • New tokens  │
│   connected   │ • Warm next │ • Capture   │   valid    │ • Reset       │
│ • Normal      │   market    │   price     │ • Flush    │   buffer      │
│   rate (30s)  │             │ • Pre-sub   │ • Rotate   │ • Continue    │
│               │             │             │            │               │
│ Price: from   │             │ Price: from │            │ Price: from   │
│ RTDS history  │             │ RTDS queue  │            │ RTDS history  │
│ (first after) │             │ buffer (20s)│            │ (first after) │
└─────────────────────────────────────────────────────────────────────────┘
```

### Rotation State Machine

```
┌─────────────┐
│   STARTUP   │
└──────┬──────┘
       │ discover_markets(discover_next=False)
       ▼
┌─────────────┐     WebSocket      ┌─────────────┐
│   ACTIVE    │◄────connect───────►│  CONNECTING │
│             │                    └─────────────┘
│  ┌───────┐  │
│  │ 15min │  │                             ┌─────────┐
│  │ timer │  │◄────────────────────────────┤  ERROR  │
│  └───┬───┘  │  (discover_next=True)       └────┬────┘
│      │      │                                   │
│      │ T-60s                                   │ retry
│      ▼      │                                   │ loop
│  ┌───────┐  │                                   │
│  │ FAST  │  │                                   │
│  │ CHECK │  │                                   │
│  └───┬───┘  │                                   │
│      │      │                                   │
│      │ T-15s                    ┌────────────────┘
│      ▼      │                    │
│  ┌─────────┐│                    │
│  │ROTATION ││────────────────────┘
│  │WINDOW   ││  (failure)
│  └───┬─────┘│
│      │      │
│      │ T+0  │
│      ▼      │
│  ┌─────────┐│
│  │ FLUSH   ││
│  │ BUFFER  ││
│  └───┬─────┘│
│      │      │
│      ▼      │
┌─────────────┐
│   NEW       │
│   MARKET    │
└─────────────┘
```

## Performance Characteristics

### Throughput

| Component | Rate | Notes |
|-----------|------|-------|
| WebSocket Ingest | ~50,000 msg/min | Varies by market activity |
| Normalized Ticks | ~3,000 ticks/min | 1s aggregation reduces volume |
| Flush to Disk | ~1,000 records/flush | Configurable threshold |
| Writes/minute | 1-2 | 30s interval + edge triggers |

### Latency Budget

```
WebSocket Message ──► [0-2ms] ──► Schema normalization
                                     │
                                     ▼
                              1s Buffer (varies)
                                     │
                                     ▼
Buffer Flush ───────► [1-5ms] ──► Deduplication
                                     │
                                     ▼
                              DataFrame Build ──► [2-10ms]
                                     │
                                     ▼
                              fastparquet Append ──► [5-50ms]
                                     │
                                     ▼
                              Total: ~10-70ms + buffer wait
```

### Memory Usage

```
┌─────────────────────────────────────────────────┐
│ Component           │ Typical │     Peak      │
├─────────────────────────────────────────────────┤
│ Schema buffer       │  2 MB   │    10 MB      │
│ BufferManager queue  │  1 MB   │     5 MB      │
│ Storage dedup set    │ ~500 KB │    ~2 MB      │
│ DataFrame (flush)    │ ~2 MB   │    ~5 MB      │
├─────────────────────────────────────────────────┤
│ Total               │ ~5 MB   │   ~20 MB      │
└─────────────────────────────────────────────────┘
```

## Reliability Mechanisms

### Connection Resilience

```
WebSocket Error
       │
       ▼
┌───────────────┐
│ Log error     │
└───────┬───────┘
        │
        ▼
┌─────────────────┐     ┌───────────────┐
│ Backoff Sleep  │     │   Success     │
│ (Exp 1s-60s)   │─────────► Reconnect │
└───────┬─────────┘     └───────────────┘
        │
        │ Failures
        ▼
┌─────────────────┐
│ Continue retry   │
│ indefinitely     │
└─────────────────┘
```

### Data Duplication Prevention

Three layers of protection:

1. **Within-second**: Last-write-wins per field
2. **Within-batch**: Deduplication with compound key (timestamp|market)
3. **Across-files**: Tracked in `_written_timestamps` set

### Graceful Degradation

| Failure Mode | Behavior |
|--------------|----------|
| WebSocket drop | Auto-reconnect with backoff |
| RTDS drop | Price_to_beat may fallback to current price |
| Disk full | Log error, data lost (no backpressure currently) |
| Market discovery fail | Retry with backoff (max 10 attempts) |
| Rotation fail | Continue on current market, retry in next window |

## Security Considerations

### Credentials
- No API credentials required for Polymarket (public endpoints)
- Kalshi credentials via config.json (currently unused)

### Input Validation
- Token metadata checked against current market condition_id
- Old market messages filtered after rotation
- JSON parsing with exception handling

### Logging
- Connection logs isolated from operational logs
- No request/response bodies logged (only metadata)
- Error logs separate with longer retention

## Monitoring & Observability

### Health Checks

| Check | Frequency | Failure Action |
|-------|-----------|----------------|
| WebSocket connected | Continuous | Reconnect |
| Heartbeat file update | 30s | Watchdog restart |
| RTDS price freshness | Each rotation | Fallback |
| Market rotation | Per-market | Retry |

### Key Metrics

```python
{
    "messages_received": 5000000,    # Total WebSocket messages
    "ticks_normalized": 4500000,      # After token filtering
    "subscribed_tokens": 2,            # YES + NO per market
    "current_market_ends_in_sec": 450,
    "records_written": 123456,
    "last_flush": "2026-02-17T21:00:00Z",
    "connected": True
}
```

## Future Considerations

### Scalability Limits
- Current design: single market focus (BTC 15min)
- Single-worker architecture
- Could extend to multi-market with:
  - Per-market rotation threads
  - Partitioned storage writers
  - Separate heartbeat metrics per market

### Kalshi Integration
- Stub exists in codebase
- Would need market discovery implementation
- Separate rotation logic (different expiry times)
- Dual-feed validation recommended

### WebSocket Data Gaps
- Known 6-34 second gaps from Polymarket
- No automatic fill via REST API (trading decision)
- Could add gap detection and alerting
