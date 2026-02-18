"""PyArrow schema definitions for Polymarket data."""

import pyarrow as pa
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from collections import defaultdict
import logging
import time


class PolymarketSchema:
    """Schema for Polymarket orderbook and trade data."""

    # Token metadata mapping: token_id -> {outcome, market_question, condition_id}
    _token_metadata: Dict[str, Dict[str, Any]] = {}

    # Track YES/NO token IDs for the current market
    _yes_token_id: Optional[str] = None
    _no_token_id: Optional[str] = None
    _condition_id: Optional[str] = None

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

    # Market-level metadata
    _price_to_beat: float = 0.0
    _current_market_question: Optional[str] = None
    
    # Token-to-condition reverse mapping for fast lookup
    _token_to_condition: Dict[str, str] = {}

    @classmethod
    def set_market_info(cls, condition_id: str, question: str):
        """Set current market info - call this when rotating to a new market.

        Args:
            condition_id: Market condition ID
            question: Market question/description
        """
        cls._condition_id = condition_id
        cls._current_market_question = question
        logger = logging.getLogger("polymarket")
        logger.info(f"Schema market info updated: {condition_id[:20]}... - {question[:50]}...")

    @classmethod
    def set_tokens(cls, tokens: List[Dict[str, Any]], condition_id: str):
        """Set token metadata for schema enrichment.
        
        Args:
            tokens: List of token dicts with token_id, outcome, etc.
            condition_id: The market condition_id these tokens belong to
        """
        cls._condition_id = condition_id
        cls._token_metadata = {}
        
        # Also update market question from first token if available
        if tokens and not cls._current_market_question:
            cls._current_market_question = tokens[0].get("question", "")

        # Build token-to-condition mapping
        for token in tokens:
            token_id = str(token.get("token_id", ""))
            outcome = token.get("outcome", token.get("name", "")).upper()
            if token_id:
                cls._token_to_condition[token_id] = condition_id
                cls._token_metadata[token_id] = {
                    "outcome": outcome,
                    "market_question": token.get("question", ""),
                    "condition_id": condition_id,
                }
                if "YES" in outcome or outcome == "UP":
                    cls._yes_token_id = token_id
                elif "NO" in outcome or outcome == "DOWN":
                    cls._no_token_id = token_id

    @classmethod
    def get_condition_id_for_token(cls, token_id: str) -> Optional[str]:
        """Get condition_id for a given token_id.
        
        Returns the condition_id if found, None otherwise.
        """
        # Check in _token_metadata mapping
        token_data = cls._token_metadata.get(token_id, {})
        condition_id = token_data.get("condition_id")
        if condition_id:
            return condition_id
        
        # Check in token_to_condition mapping
        if token_id in cls._token_to_condition:
            return cls._token_to_condition[token_id]
        
        return None

    @classmethod
    def reset_buffer(cls):
        """Clear the 1-second aggregation buffer.
        
        Also clears last_known orderbook values to prevent old market data 
        from carrying into new market.
        """
        cls._second_buffer.clear()
        # Clear ALL orderbook/trade last_known values
        # Only preserve price_to_beat which is market-level
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
        logger = logging.getLogger("polymarket")
        logger.debug(f"Buffer reset. Preserved price_to_beat={price_to_beat}, cleared orderbook.")

    @classmethod
    def set_price_to_beat(cls, price: float):
        """Set the price to beat (window start BTC price) from market discovery."""
        cls._price_to_beat = float(price) if price else 0.0
        cls._last_known['price_to_beat'] = cls._price_to_beat
    
    @classmethod
    def set_oracle_price(cls, price: float, timestamp: Optional[datetime] = None):
        """Set the current oracle price (live BTC/USD from RTDS)."""
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
                'received_at': datetime.now(timezone.utc),  # Will be overwritten with actual received time
                'market_question': cls._current_market_question or "",
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

    # Core schema for 1-second aggregated data
    TICK_SCHEMA = pa.schema([
        ("timestamp", pa.timestamp("us")),           # 1-second bucket timestamp (server time)
        ("received_at", pa.timestamp("us")),         # Local time when message was received
        ("market_question", pa.string()),             # Full market question
        ("price_to_beat", pa.float64()),              # Window start BTC price
        ("oracle_price", pa.float64()),               # Current BTC oracle price
        ("oracle_price_timestamp", pa.timestamp("us")),  # Oracle price timestamp
        ("yes_best_bid", pa.float64()),               # YES best bid price
        ("yes_best_bid_size", pa.float64()),          # YES best bid size
        ("yes_best_ask", pa.float64()),               # YES best ask price
        ("yes_best_ask_size", pa.float64()),          # YES best ask size
        ("no_best_bid", pa.float64()),                # NO best bid price
        ("no_best_bid_size", pa.float64()),           # NO best bid size
        ("no_best_ask", pa.float64()),                # NO best ask price
        ("no_best_ask_size", pa.float64()),           # NO best ask size
        ("yes_last_trade_price", pa.float64()),       # YES last trade price
        ("yes_last_trade_size", pa.float64()),        # YES last trade size
        ("no_last_trade_price", pa.float64()),        # NO last trade price
        ("no_last_trade_size", pa.float64()),         # NO last trade size
    ])

    @classmethod
    def normalize_ws_message(cls, msg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize WebSocket message and add to 1-second aggregation buffer.

        Returns the second_key if data was added, None otherwise.
        """
        # Capture local received time at message arrival
        received_at = datetime.now(timezone.utc)
        
        event_type = msg.get("event_type", "")

        # Get timestamp (server time)
        ts_ms = msg.get("timestamp", time.time() * 1000)
        if isinstance(ts_ms, str):
            ts_ms = int(ts_ms)
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        second_key = cls.get_second_key(timestamp)

        # Get token info
        asset_id = msg.get("asset_id", "")
        token_info = cls._token_metadata.get(asset_id, {})
        
        # CRITICAL FIX: Check if this token belongs to the CURRENT market
        # After rotation, old token messages might still arrive from WebSocket lag
        # These should be ignored to prevent old market data in new timestamps
        token_condition_id = token_info.get("condition_id", "")
        if token_condition_id and token_condition_id != cls._condition_id:
            # This message is for a different market (old market after rotation)
            return None
        
        outcome = token_info.get("outcome", "").upper()

        # Determine if YES or NO
        is_yes = "YES" in outcome or asset_id == cls._yes_token_id
        is_no = "NO" in outcome or asset_id == cls._no_token_id

        if not is_yes and not is_no:
            return None  # Unknown token

        # Get or create entry for this second
        entry = cls._ensure_second_entry(second_key)
        entry['received_at'] = received_at
        entry['_updated'] = True

        # Update market question if available
        market_q = token_info.get("market_question", "")
        if market_q and not entry['market_question']:
            entry['market_question'] = market_q

        # Note: oracle_price is set exclusively by RTDS client via set_oracle_price()
        # WS messages do not carry reliable oracle price data

        if event_type == "book":
            # Orderbook update with bids/asks
            bids = msg.get("bids", [])
            asks = msg.get("asks", [])

            # Best bid = highest price (last in array since ascending)
            best_bid = float(bids[-1]["price"]) if bids else 0.0
            best_bid_size = float(bids[-1]["size"]) if bids else 0.0
            # Best ask = lowest price (last in array since descending)
            best_ask = float(asks[-1]["price"]) if asks else 0.0
            best_ask_size = float(asks[-1]["size"]) if asks else 0.0

            if is_yes:
                entry['yes_best_bid'] = best_bid
                entry['yes_best_bid_size'] = best_bid_size
                entry['yes_best_ask'] = best_ask
                entry['yes_best_ask_size'] = best_ask_size
                # Update last_known for carry-forward
                cls._last_known['yes_best_bid'] = best_bid
                cls._last_known['yes_best_bid_size'] = best_bid_size
                cls._last_known['yes_best_ask'] = best_ask
                cls._last_known['yes_best_ask_size'] = best_ask_size
            else:
                entry['no_best_bid'] = best_bid
                entry['no_best_bid_size'] = best_bid_size
                entry['no_best_ask'] = best_ask
                entry['no_best_ask_size'] = best_ask_size
                # Update last_known for carry-forward
                cls._last_known['no_best_bid'] = best_bid
                cls._last_known['no_best_bid_size'] = best_bid_size
                cls._last_known['no_best_ask'] = best_ask
                cls._last_known['no_best_ask_size'] = best_ask_size

        elif event_type in ("last_trade_price", "trade"):
            # Trade update - update both current entry AND last_known for carry-forward
            price = float(msg.get("price", 0))
            size = float(msg.get("size", 0))

            if is_yes:
                entry['yes_last_trade_price'] = price
                entry['yes_last_trade_size'] = size
                # Update last_known so future seconds carry this trade forward
                cls._last_known['yes_last_trade_price'] = price
                cls._last_known['yes_last_trade_size'] = size
            else:
                entry['no_last_trade_price'] = price
                entry['no_last_trade_size'] = size
                # Update last_known so future seconds carry this trade forward
                cls._last_known['no_last_trade_price'] = price
                cls._last_known['no_last_trade_size'] = size

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
                columns[field_name].append(row.get(field_name, 0.0 if field_name != 'market_question' else ""))

        arrays = []
        for field in schema:
            arr = pa.array(columns[field.name], type=field.type)
            arrays.append(arr)

        return pa.RecordBatch.from_arrays(arrays, schema.names)

    @staticmethod
    def _parse_timestamp(ts) -> datetime:
        """Parse various timestamp formats to datetime."""

        if ts is None:
            return datetime.now(timezone.utc)

        if isinstance(ts, str):
            # ISO format
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00"))
            except:
                return datetime.now(timezone.utc)

        if isinstance(ts, (int, float)):
            # Assume milliseconds or seconds since epoch
            if ts > 1e12:  # Milliseconds
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:  # Seconds
                return datetime.fromtimestamp(ts, tz=timezone.utc)

        return ts if hasattr(ts, 'tzinfo') else datetime.now(timezone.utc)
