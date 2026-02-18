"""PyArrow schema definitions for Kalshi data with 1-second aggregation.

Kalshi prices are in cents (0-10000) which get normalized to 0.0-1.0 floats
to match Polymarket's price format.
"""

import pyarrow as pa
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
import time
import logging


logger = logging.getLogger("kalshi")


class KalshiSchema:
    """Schema for Kalshi orderbook and trade data.
    
    Designed to match PolymarketSchema structure for unified data processing.
    """
    
    # Market metadata
    _current_market_ticker: Optional[str] = None
    _current_market_title: Optional[str] = None
    _current_market_subtitle: Optional[str] = None
    
    # 1-second aggregation buffer: timestamp -> aggregated data
    _second_buffer: Dict[str, Dict[str, Any]] = {}
    
    # Last known values for carry-forward
    _last_known: Dict[str, Any] = {
        'price_to_beat': 0.0,
        'oracle_price': 0.0,
        'yes_best_bid': 0.0, 'yes_best_bid_size': 0.0,
        'yes_best_ask': 0.0, 'yes_best_ask_size': 0.0,
        'no_best_bid': 0.0, 'no_best_bid_size': 0.0,
        'no_best_ask': 0.0, 'no_best_ask_size': 0.0,
        'yes_last_trade_price': 0.0, 'yes_last_trade_size': 0.0,
        'no_last_trade_price': 0.0, 'no_last_trade_size': 0.0,
    }
    
    @classmethod
    def set_market_info(cls, ticker: str, title: str, subtitle: str, yes_price: int, no_price: int):
        """Set current market info - call this when rotating to a new market.
        
        Args:
            ticker: Market ticker (e.g., KXBTC15M-20260218-0800)
            title: Market title/question
            subtitle: Market subtitle/details
            yes_price: Initial YES price in cents (0-10000)
            no_price: Initial NO price in cents (0-10000)
        """
        cls._current_market_ticker = ticker
        cls._current_market_title = title
        cls._current_market_subtitle = subtitle
        
        # Set initial oracle price from yes_price (price at market open)
        initial_oracle = yes_price / 10000.0 if yes_price else 0.0
        cls._last_known['oracle_price'] = initial_oracle
        
        logger.info(f"KalshiSchema market info updated: {ticker} - {title[:50]}...")
    
    @classmethod
    def reset_buffer(cls):
        """Clear the 1-second aggregation buffer.
        
        Also clears last_known orderbook values to prevent old market data 
        from carrying into new market.
        """
        cls._second_buffer.clear()
        # Clear ALL orderbook/trade last_known values
        # Only preserve price_to_beat and oracle_price which are market-level
        price_to_beat = cls._last_known.get('price_to_beat', 0.0)
        oracle_price = cls._last_known.get('oracle_price', 0.0)
        cls._last_known = {
            'price_to_beat': price_to_beat,
            'oracle_price': oracle_price,
            'yes_best_bid': 0.0, 'yes_best_bid_size': 0.0,
            'yes_best_ask': 0.0, 'yes_best_ask_size': 0.0,
            'no_best_bid': 0.0, 'no_best_bid_size': 0.0,
            'no_best_ask': 0.0, 'no_best_ask_size': 0.0,
            'yes_last_trade_price': 0.0, 'yes_last_trade_size': 0.0,
            'no_last_trade_price': 0.0, 'no_last_trade_size': 0.0,
        }
        logger.debug(f"KalshiSchema buffer reset. Preserved price_to_beat={price_to_beat}, oracle_price={oracle_price}")
    
    @classmethod
    def set_price_to_beat(cls, price: float):
        """Set the price to beat (window start BTC price) from market discovery."""
        cls._last_known['price_to_beat'] = float(price) if price else 0.0
        logger.debug(f"KalshiSchema price_to_beat set to: {price}")
    
    @classmethod
    def set_oracle_price(cls, price: float, timestamp: Optional[datetime] = None):
        """Set the current oracle price (live BTC/USD from Kalshi ticker channel)."""
        cls._last_known['oracle_price'] = float(price) if price else 0.0
        cls._last_known['oracle_price_timestamp'] = timestamp or datetime.now(timezone.utc)
    
    @classmethod
    def get_second_key(cls, timestamp: datetime) -> str:
        """Get 1-second bucket key from timestamp."""
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    
    @classmethod
    def _ensure_second_entry(cls, second_key: str) -> Dict[str, Any]:
        """Get or create entry for a specific second."""
        if second_key not in cls._second_buffer:
            cls._second_buffer[second_key] = {
                'timestamp': datetime.fromisoformat(second_key).replace(tzinfo=timezone.utc),
                'received_at': datetime.now(timezone.utc),
                'market_question': f"{cls._current_market_title or ''} - {cls._current_market_subtitle or ''}".strip() or "",
                # Price fields (carry forward from last known)
                'price_to_beat': cls._last_known['price_to_beat'],
                'oracle_price': cls._last_known['oracle_price'],
                'oracle_price_timestamp': cls._last_known.get('oracle_price_timestamp', None),
                # Start with last known values for orderbook
                'yes_best_bid': cls._last_known['yes_best_bid'],
                'yes_best_bid_size': cls._last_known['yes_best_bid_size'],
                'yes_best_ask': cls._last_known['yes_best_ask'],
                'yes_best_ask_size': cls._last_known['yes_best_ask_size'],
                'no_best_bid': cls._last_known['no_best_bid'],
                'no_best_bid_size': cls._last_known['no_best_bid_size'],
                'no_best_ask': cls._last_known['no_best_ask'],
                'no_best_ask_size': cls._last_known['no_best_ask_size'],
                'yes_last_trade_price': cls._last_known['yes_last_trade_price'],
                'yes_last_trade_size': cls._last_known['yes_last_trade_size'],
                'no_last_trade_price': cls._last_known['no_last_trade_price'],
                'no_last_trade_size': cls._last_known['no_last_trade_size'],
                # Track if we got any update this second
                '_updated': False,
            }
        return cls._second_buffer[second_key]
    
    @classmethod
    def _update_last_known(cls, entry: Dict[str, Any]):
        """Update last known values from a completed second entry."""
        for key in cls._last_known:
            if key in entry:
                cls._last_known[key] = entry[key]
    
    # Core schema for 1-second aggregated data - MATCHES PolymarketSchema.TICK_SCHEMA
    TICK_SCHEMA = pa.schema([
        ("timestamp", pa.timestamp("us")),           # 1-second bucket timestamp (server time)
        ("received_at", pa.timestamp("us")),         # Local time when message was received
        ("market_question", pa.string()),             # Full market question
        ("price_to_beat", pa.float64()),              # Window start BTC price
        ("oracle_price", pa.float64()),               # Current BTC oracle price
        ("oracle_price_timestamp", pa.timestamp("us")),  # Oracle price timestamp
        ("yes_best_bid", pa.float64()),               # YES best bid price (0.0-1.0)
        ("yes_best_bid_size", pa.float64()),          # YES best bid size
        ("yes_best_ask", pa.float64()),               # YES best ask price (0.0-1.0)
        ("yes_best_ask_size", pa.float64()),          # YES best ask size
        ("no_best_bid", pa.float64()),                # NO best bid price (0.0-1.0)
        ("no_best_bid_size", pa.float64()),           # NO best bid size
        ("no_best_ask", pa.float64()),                # NO best ask price (0.0-1.0)
        ("no_best_ask_size", pa.float64()),           # NO best ask size
        ("yes_last_trade_price", pa.float64()),       # YES last trade price (0.0-1.0)
        ("yes_last_trade_size", pa.float64()),        # YES last trade size
        ("no_last_trade_price", pa.float64()),        # NO last trade price (0.0-1.0)
        ("no_last_trade_size", pa.float64()),         # NO last trade size
    ])
    
    @classmethod
    def _normalize_price(cls, price_cents: int) -> float:
        """Normalize Kalshi price from cents (0-10000) to float (0.0-1.0).
        
        Args:
            price_cents: Price in Kalshi cents (0-10000)
            
        Returns:
            Normalized price as float (0.0-1.0)
        """
        if price_cents is None:
            return 0.0
        return float(price_cents) / 10000.0
    
    @classmethod
    def _parse_kalshi_timestamp(cls, ts: Any) -> datetime:
        """Parse Kalshi timestamp to datetime.
        
        Kalshi timestamps are typically milliseconds since epoch.
        """
        if ts is None:
            return datetime.now(timezone.utc)
        
        if isinstance(ts, str):
            # ISO format
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except:
                return datetime.now(timezone.utc)
        
        if isinstance(ts, (int, float)):
            # Kalshi uses milliseconds since epoch
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        
        return ts if hasattr(ts, 'tzinfo') else datetime.now(timezone.utc)
    
    @classmethod
    def normalize_ws_message(cls, msg: Dict[str, Any]) -> Optional[str]:
        """Normalize WebSocket message and add to 1-second aggregation buffer.
        
        Handles Kalshi WebSocket message types:
        - ticker: Market summary with yes_price/no_price (oracle prices)
        - orderbook_delta: Orderbook updates
        - trade: Trade execution updates
        
        Returns:
            The second_key if data was added, None otherwise.
        """
        # Capture local received time at message arrival
        received_at = datetime.now(timezone.utc)
        
        # Get message type
        msg_type = msg.get("type", "").lower()
        
        # Get timestamp (server time)
        ts_ms = msg.get("ts", time.time() * 1000)
        if isinstance(ts_ms, str):
            try:
                ts_ms = int(ts_ms)
            except:
                ts_ms = time.time() * 1000
        timestamp = cls._parse_kalshi_timestamp(ts_ms)
        second_key = cls.get_second_key(timestamp)
        
        # Get market ticker
        ticker = msg.get("ticker", "")
        
        # Skip if not for current market (after rotation, old messages may arrive)
        if cls._current_market_ticker and ticker and ticker != cls._current_market_ticker:
            return None
        
        # Get or create entry for this second
        entry = cls._ensure_second_entry(second_key)
        entry['received_at'] = received_at
        entry['_updated'] = True
        
        if msg_type == "ticker":
            # Ticker provides market summary with yes_price and no_price (oracle)
            yes_price_cents = msg.get("yes_price", 0)
            no_price_cents = msg.get("no_price", 0)
            
            # Normalize prices
            yes_price = cls._normalize_price(yes_price_cents)
            no_price = cls._normalize_price(no_price_cents)
            
            # Update oracle price from yes_price (this is the market's current price)
            cls.set_oracle_price(yes_price, timestamp)
            entry['oracle_price'] = yes_price
            entry['oracle_price_timestamp'] = timestamp
            
            # Also update best bid/ask from market prices
            entry['yes_best_bid'] = yes_price
            entry['yes_best_ask'] = yes_price
            entry['no_best_bid'] = no_price
            entry['no_best_ask'] = no_price
            
            # Update last_known
            cls._last_known['oracle_price'] = yes_price
            cls._last_known['oracle_price_timestamp'] = timestamp
            cls._last_known['yes_best_bid'] = yes_price
            cls._last_known['yes_best_ask'] = yes_price
            cls._last_known['no_best_bid'] = no_price
            cls._last_known['no_best_ask'] = no_price
            
        elif msg_type == "orderbook_delta":
            # Orderbook delta provides bid/ask updates
            market_ticker = msg.get("market_ticker", "")
            side = msg.get("side", "").lower()  # "yes" or "no"
            price_cents = msg.get("price", 0)
            size = msg.get("size", 0)
            is_buy = msg.get("is_buy", True)  # True = bid, False = ask
            
            price = cls._normalize_price(price_cents)
            
            if side == "yes":
                if is_buy:
                    entry['yes_best_bid'] = price
                    entry['yes_best_bid_size'] = float(size)
                    cls._last_known['yes_best_bid'] = price
                    cls._last_known['yes_best_bid_size'] = float(size)
                else:
                    entry['yes_best_ask'] = price
                    entry['yes_best_ask_size'] = float(size)
                    cls._last_known['yes_best_ask'] = price
                    cls._last_known['yes_best_ask_size'] = float(size)
            elif side == "no":
                if is_buy:
                    entry['no_best_bid'] = price
                    entry['no_best_bid_size'] = float(size)
                    cls._last_known['no_best_bid'] = price
                    cls._last_known['no_best_bid_size'] = float(size)
                else:
                    entry['no_best_ask'] = price
                    entry['no_best_ask_size'] = float(size)
                    cls._last_known['no_best_ask'] = price
                    cls._last_known['no_best_ask_size'] = float(size)
                    
        elif msg_type == "trade":
            # Trade provides execution updates
            side = msg.get("side", "").lower()  # "yes" or "no"
            price_cents = msg.get("price", 0)
            count = msg.get("count", 0)  # Trade size
            
            price = cls._normalize_price(price_cents)
            
            if side == "yes":
                entry['yes_last_trade_price'] = price
                entry['yes_last_trade_size'] = float(count)
                cls._last_known['yes_last_trade_price'] = price
                cls._last_known['yes_last_trade_size'] = float(count)
            elif side == "no":
                entry['no_last_trade_price'] = price
                entry['no_last_trade_size'] = float(count)
                cls._last_known['no_last_trade_price'] = price
                cls._last_known['no_last_trade_size'] = float(count)
        else:
            # Unknown message type - skip
            return None
        
        return second_key
    
    @classmethod
    def flush_completed_seconds(cls, current_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Flush all completed seconds (older than current second) and return records.
        
        Args:
            current_time: Current timestamp. If None, uses now.
            
        Returns:
            List of completed second records ready for writing.
        """
        if current_time is None:
            current_time = datetime.now(timezone.utc)
        
        current_second_key = cls.get_second_key(current_time)
        completed_records = []
        keys_to_remove = []
        
        for second_key, entry in cls._second_buffer.items():
            # Skip current second (still receiving data)
            if second_key >= current_second_key:
                continue
            
            # Create clean record without internal fields
            record = {k: v for k, v in entry.items() if not k.startswith('_')}
            completed_records.append(record)
            keys_to_remove.append(second_key)
            
            # Update last known values for carry-forward
            cls._update_last_known(entry)
        
        # Remove flushed entries
        for key in keys_to_remove:
            del cls._second_buffer[key]
        
        return completed_records
    
    @classmethod
    def flush_all(cls) -> List[Dict[str, Any]]:
        """Flush all buffered seconds (for shutdown)."""
        all_records = []
        
        for second_key, entry in sorted(cls._second_buffer.items()):
            record = {k: v for k, v in entry.items() if not k.startswith('_')}
            all_records.append(record)
            cls._update_last_known(entry)
        
        cls._second_buffer.clear()
        return all_records
    
    @classmethod
    def create_record_batch(cls, data: List[Dict[str, Any]], schema: pa.Schema = None) -> pa.RecordBatch:
        """Create a PyArrow RecordBatch from dict data."""
        if schema is None:
            schema = cls.TICK_SCHEMA
        
        # Ensure all required columns exist
        columns = {field.name: [] for field in schema}
        
        for row in data:
            for field_name in columns:
                value = row.get(field_name)
                # Handle timestamps and None values
                if value is None:
                    if isinstance(schema.field(field_name).type, pa.TimestampType):
                        columns[field_name].append(None)
                    elif field_name == 'market_question':
                        columns[field_name].append("")
                    else:
                        columns[field_name].append(0.0)
                else:
                    columns[field_name].append(value)
        
        arrays = []
        for field in schema:
            try:
                arr = pa.array(columns[field.name], type=field.type)
                arrays.append(arr)
            except Exception as e:
                logger.error(f"Error creating array for field {field.name}: {e}")
                # Create empty array of correct type
                arr = pa.array([None] * len(columns[field.name]), type=field.type)
                arrays.append(arr)
        
        return pa.RecordBatch.from_arrays(arrays, schema.names)
