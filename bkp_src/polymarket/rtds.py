"""Polymarket RTDS (Real-Time Data Service) WebSocket client for oracle prices.

Provides live BTC/USD price feed from Chainlink via Polymarket's RTDS endpoint.
"""

import asyncio
import json
import logging
import bisect
from typing import Optional, Dict, Any
from datetime import datetime, timezone, timedelta
from collections import deque

import websockets

logger = logging.getLogger(__name__)


class RTDSClient:
    """Client for Polymarket Real-Time Data Service (RTDS).
    
    Connects to wss://ws-live-data.polymarket.com for live crypto price feeds.
    """
    
    WS_URL = "wss://ws-live-data.polymarket.com"
    
    def __init__(self):
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self._ws_task: Optional[asyncio.Task] = None
        
        # Latest BTC/USD price
        self.btc_usd_price: float = 0.0
        self.last_update: Optional[datetime] = None
        
        # Price history for historical lookups (keep last hour of price points)
        self._price_history: list[tuple[datetime, float]] = []
        self._max_history_age_seconds = 3600  # 1 hour
        
        # Oracle queue for "first price after expiry" logic
        self._oracle_queue: deque[tuple[datetime, float]] = deque()  # (oracle_timestamp, price)
        self._queue_max_age = 20  # seconds - covers rotation window plus buffer
        
    async def start(self) -> None:
        """Start the RTDS WebSocket connection."""
        self.running = True
        self._ws_task = asyncio.create_task(self._ws_loop())
        logger.info("RTDS client started")
    
    async def stop(self) -> None:
        """Stop the RTDS client. Idempotent - safe to call multiple times."""
        # Idempotency check: prevent double-close
        if not self.running:
            logger.debug("RTDS client already stopped")
            return
        
        self.running = False
        
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        
        if self.ws:
            await self.ws.close()
            self.ws = None
            
        logger.info("RTDS client stopped")
    
    async def _ws_loop(self) -> None:
        """WebSocket connection loop with auto-reconnect."""
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while self.running:
            try:
                logger.info(f"Connecting to RTDS: {self.WS_URL}")
                
                async with websockets.connect(
                    self.WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 1
                    
                    # Subscribe to BTC/USD Chainlink feed
                    await self._subscribe()
                    
                    # Process messages
                    async for message in ws:
                        if not self.running:
                            break
                        
                        try:
                            await self._handle_message(message)
                        except Exception as e:
                            logger.error(f"RTDS message handling error: {e}")
                
            except websockets.exceptions.ConnectionClosed:
                logger.warning("RTDS WebSocket closed, reconnecting...")
            except Exception as e:
                logger.error(f"RTDS WebSocket error: {e}")
            
            if self.running:
                logger.info(f"RTDS reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def _subscribe(self) -> None:
        """Subscribe to BTC/USD Chainlink price feed."""
        if not self.ws:
            return
        
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": '{"symbol":"btc/usd"}'
                }
            ]
        }
        
        await self.ws.send(json.dumps(sub_msg))
        logger.info("Subscribed to RTDS BTC/USD Chainlink feed")
    
    async def _handle_message(self, message: str) -> None:
        """Handle incoming RTDS message."""
        if not message.strip():
            return  # Skip empty messages
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            logger.warning(f"RTDS invalid JSON: {message[:200]}")
            return
        
        # Check for crypto_prices_chainlink updates
        topic = data.get("topic", "")
        msg_type = data.get("type", "")
        
        # Handle chainlink price updates (type: "update")
        if topic == "crypto_prices_chainlink" and msg_type == "update":
            payload = data.get("payload", {})
            symbol = payload.get("symbol", "")
            value = payload.get("value", 0.0)
            
            if symbol == "btc/usd" and value > 0:
                self.btc_usd_price = float(value)
                self.last_update = datetime.now(timezone.utc)
                self._add_price_to_history(self.last_update, self.btc_usd_price)
                logger.debug(f"BTC/USD updated: {self.btc_usd_price}")
        
        # Handle bulk price data on subscription confirmation
        elif topic == "crypto_prices" and msg_type == "subscribe":
            payload = data.get("payload", {})
            symbol = payload.get("symbol", "")
            data_points = payload.get("data", [])
            
            if symbol == "btc/usd" and data_points:
                # Process all data points into history
                for point in data_points:
                    value = point.get("value", 0.0)
                    timestamp = point.get("timestamp")
                    if value > 0 and timestamp:
                        try:
                            # Parse timestamp (usually Unix timestamp or ISO format)
                            if isinstance(timestamp, (int, float)):
                                point_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                            else:
                                point_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                            self._add_price_to_history(point_dt, float(value))
                        except Exception:
                            pass  # Skip malformed timestamps
                
                # Use latest value for current price
                latest = data_points[-1]
                value = latest.get("value", 0.0)
                if value > 0:
                    self.btc_usd_price = float(value)
                    self.last_update = datetime.now(timezone.utc)
                    logger.info(f"BTC/USD initialized from bulk data: {self.btc_usd_price}")
    
    def get_first_price_after(self, cutoff_timestamp: datetime) -> float:
        """Return first price with oracle_timestamp >= cutoff using binary search.
        
        Used for Polymarket price_to_beat calculation where we need the
        first price at or after market expiry, not the closest price.
        
        Args:
            cutoff_timestamp: The UTC datetime to find first price at or after
            
        Returns:
            BTC/USD price at or after cutoff, or 0.0 if not available
        """
        # Convert deque to list for bisect (since queue is maintained in time order)
        queue_list = list(self._oracle_queue)
        if not queue_list:
            return 0.0
        
        # Extract timestamps for binary search
        timestamps = [ts for ts, _ in queue_list]
        
        # Find insertion point for cutoff_timestamp
        idx = bisect.bisect_left(timestamps, cutoff_timestamp)
        
        if idx < len(queue_list):
            return queue_list[idx][1]
        return 0.0

    def get_oracle_price(self) -> float:
        """Get latest BTC/USD oracle price."""
        return self.btc_usd_price
    
    def _add_price_to_history(self, dt: datetime, price: float) -> None:
        """Add price point to history and oracle queue, pruning old entries."""
        # Add to price history (long-term, 1 hour)
        self._price_history.append((dt, price))
        
        # Add to oracle queue (short-term, for expiry-time queries)
        self._oracle_queue.append((dt, price))
        
        # Prune old entries from price history
        history_cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._max_history_age_seconds)
        self._price_history = [(t, p) for t, p in self._price_history if t >= history_cutoff]
        
        # Prune old entries from oracle queue
        queue_cutoff = datetime.now(timezone.utc) - timedelta(seconds=self._queue_max_age)
        self._oracle_queue = deque([(t, p) for t, p in self._oracle_queue if t >= queue_cutoff])
    
    def get_price_at_time(self, target_time: datetime) -> float:
        """Get BTC price at or closest to target_time from history.
        
        Args:
            target_time: The UTC datetime to get price for
            
        Returns:
            BTC/USD price at or nearest to target_time, or 0 if not available
        """
        if not self._price_history:
            return 0.0
        
        # Find closest price to target_time
        closest_price = 0.0
        min_diff = 999999.0  # float seconds, not timedelta
        
        for dt, price in self._price_history:
            diff = abs((dt - target_time).total_seconds())
            if diff < min_diff:
                min_diff = diff
                closest_price = price
        
        # Only use if within 60 seconds of target
        if min_diff <= 60:
            logger.debug(f"Found price {closest_price} at {min_diff:.0f}s from target")
            return closest_price
        else:
            logger.debug(f"No price within 60s of target (closest: {min_diff:.0f}s)")
            return 0.0
    
    def capture_price_to_beat(self) -> float:
        """Capture current BTC price as 'price to beat' for window start.
        
        Returns:
            Current BTC/USD price for use as price_to_beat baseline.
        """
        captured = self.btc_usd_price
        logger.info(f"Captured price_to_beat: {captured}")
        return captured
