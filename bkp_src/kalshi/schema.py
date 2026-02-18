"""PyArrow schema definitions for Kalshi data."""

import pyarrow as pa
from datetime import datetime, timezone
from typing import List, Dict, Any


class KalshiSchema:
    """Schema for Kalshi orderbook and trade data."""
    
    # Core schema for tick data - optimized for Parquet
    TICK_SCHEMA = pa.schema([
        ("timestamp", pa.timestamp("us")),        # Microsecond precision
        ("received_at", pa.timestamp("us")),     # Local receive time
        ("ticker", pa.string()),                  # Market ticker
        ("event_ticker", pa.string()),            # Event ticker
        ("price", pa.int16()),                    # Price (0-10000 for cents)
        ("size", pa.int32()),                     # Size/contracts
        ("side", pa.dictionary(pa.int8(), pa.string())),  # yes/no
        ("order_id", pa.string()),                # Order ID
        ("tick_type", pa.dictionary(pa.int8(), pa.string())),  # trade/order/book
        ("seq_num", pa.int64()),                  # Sequence number
        ("yes_price", pa.int16()),               # Current yes price
        ("no_price", pa.int16()),                # Current no price
        ("volume", pa.int64()),                   # Cumulative volume
    ])
    
    # Ticker stats schema
    STATS_SCHEMA = pa.schema([
        ("timestamp", pa.timestamp("us")),
        ("received_at", pa.timestamp("us")),
        ("ticker", pa.string()),
        ("open_price", pa.int16()),
        ("high_price", pa.int16()),
        ("low_price", pa.int16()),
        ("close_price", pa.int16()),
        ("volume", pa.int64()),
        ("open_interest", pa.int64()),
    ])
    
    @classmethod
    def create_record_batch(cls, data: List[Dict[str, Any]], schema: pa.Schema = None) -> pa.RecordBatch:
        """Create a PyArrow RecordBatch from dict data."""
        if schema is None:
            schema = cls.TICK_SCHEMA
        
        # Ensure all required columns exist
        columns = {field.name: [] for field in schema}
        
        for row in data:
            for field_name in columns:
                columns[field_name].append(row.get(field_name))
        
        arrays = []
        for field in schema:
            arr = pa.array(columns[field.name], type=field.type)
            arrays.append(arr)
        
        return pa.RecordBatch.from_arrays(arrays, schema.names)
    
    @classmethod
    def normalize_ws_message(cls, msg: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize WebSocket message to tick format."""
        import time
        from datetime import datetime, timezone
        
        now = datetime.now(timezone.utc)
        
        # Kalshi message format
        msg_type = msg.get("type", "")
        
        if msg_type == "trade":
            return {
                "timestamp": cls._parse_timestamp(msg.get("ts")),
                "received_at": now,
                "ticker": msg.get("ticker", ""),
                "event_ticker": msg.get("event_ticker", ""),
                "price": int(msg.get("yes_price", 0)),
                "size": int(msg.get("count", msg.get("size", 0))),
                "side": "yes" if msg.get("side") == "yes" else "no",
                "order_id": msg.get("trade_id", ""),
                "tick_type": "trade",
                "seq_num": msg.get("seq", 0),
                "yes_price": int(msg.get("yes_price", 0)),
                "no_price": int(msg.get("no_price", 0)),
                "volume": int(msg.get("volume", 0)),
            }
        elif msg_type == "order_book":
            return {
                "timestamp": cls._parse_timestamp(msg.get("ts")),
                "received_at": now,
                "ticker": msg.get("ticker", ""),
                "event_ticker": msg.get("event_ticker", ""),
                "price": int(msg.get("price", 0)),
                "size": int(msg.get("size", 0)),
                "side": "yes" if msg.get("side") == "yes" else "no",
                "order_id": msg.get("order_id", ""),
                "tick_type": "book",
                "seq_num": msg.get("seq", 0),
                "yes_price": int(msg.get("yes_price", 0)),
                "no_price": int(msg.get("no_price", 0)),
                "volume": int(msg.get("cumulative_volume", 0)),
            }
        elif msg_type == "ticker":
            return {
                "timestamp": cls._parse_timestamp(msg.get("ts")),
                "received_at": now,
                "ticker": msg.get("ticker", ""),
                "event_ticker": msg.get("event_ticker", ""),
                "price": int(msg.get("last_price", 0)),
                "size": 0,
                "side": "yes",
                "order_id": "",
                "tick_type": "ticker",
                "seq_num": 0,
                "yes_price": int(msg.get("yes_price", 0)),
                "no_price": int(msg.get("no_price", 0)),
                "volume": int(msg.get("volume", 0)),
            }
        else:
            # Unknown message type
            return {
                "timestamp": now,
                "received_at": now,
                "ticker": msg.get("ticker", "unknown"),
                "event_ticker": msg.get("event_ticker", ""),
                "price": 0,
                "size": 0,
                "side": "yes",
                "order_id": "",
                "tick_type": "unknown",
                "seq_num": 0,
                "yes_price": 0,
                "no_price": 0,
                "volume": 0,
            }
    
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
            # Kalshi uses milliseconds since epoch
            if ts > 1e12:
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        
        return ts if hasattr(ts, 'tzinfo') else datetime.now(timezone.utc)
