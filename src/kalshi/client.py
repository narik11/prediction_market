"""Kalshi WebSocket and REST client for BTC 15-min markets."""

import asyncio
import json
import logging
import aiohttp
import websockets
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from pathlib import Path


try:
    from .schema import KalshiSchema
except ImportError:
    from kalshi.schema import KalshiSchema


# Two separate loggers for proper log routing:
# - ops_logger: rotation, discovery, decisions → collector.log (via root logger)
# - conn_logger: WebSocket events → connection.log (propagate=False in main.py)
ops_logger = logging.getLogger("kalshi")
conn_logger = logging.getLogger("kalshi.connection")


@dataclass
class RotationConfig:
    """Rotation timing configuration loaded from root config.json."""
    check_interval_sec: int = 30
    fast_check_interval_sec: int = 2
    buffer_min_sec: int = 5
    buffer_max_sec: int = 15
    min_rotation_interval_sec: int = 600
    max_expired_age_sec: int = 300
    max_retries: int = 10
    retry_delay_sec: float = 5.0
    
    @classmethod
    def from_file(cls, config_path: Optional[str] = None) -> "RotationConfig":
        """Load rotation config from root config.json."""
        if config_path is None:
            # Try root config.json first
            possible_paths = [
                Path("config.json"),
                Path("../config.json"),
                Path("../../config.json"),
            ]
            for path in possible_paths:
                if path.exists():
                    config_path = str(path)
                    break
        
        config = cls()
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    data = json.load(f)
                rotation = data.get("rotation", {})
                config.check_interval_sec = rotation.get("check_interval_sec", config.check_interval_sec)
                config.fast_check_interval_sec = rotation.get("fast_check_interval_sec", config.fast_check_interval_sec)
                config.buffer_min_sec = rotation.get("buffer_min_sec", config.buffer_min_sec)
                config.buffer_max_sec = rotation.get("buffer_max_sec", config.buffer_max_sec)
                config.min_rotation_interval_sec = rotation.get("min_rotation_interval_sec", config.min_rotation_interval_sec)
                config.max_expired_age_sec = rotation.get("max_expired_age_sec", config.max_expired_age_sec)
                discovery = rotation.get("discovery", {})
                config.max_retries = discovery.get("max_retries", config.max_retries)
                config.retry_delay_sec = discovery.get("retry_delay_sec", config.retry_delay_sec)
                ops_logger.info(f"Loaded rotation config from {config_path}")
            except Exception as e:
                ops_logger.warning(f"Failed to load config: {e}. Using defaults.")
        else:
            ops_logger.info("No rotation config found. Using defaults.")
        return config
    
    def log_values(self) -> None:
        """Log current configuration values."""
        ops_logger.info("Kalshi Rotation Configuration:")
        ops_logger.info(f"  check_interval_sec: {self.check_interval_sec}")
        ops_logger.info(f"  fast_check_interval_sec: {self.fast_check_interval_sec}")
        ops_logger.info(f"  buffer_min_sec: {self.buffer_min_sec}")
        ops_logger.info(f"  buffer_max_sec: {self.buffer_max_sec}")
        ops_logger.info(f"  min_rotation_interval_sec: {self.min_rotation_interval_sec}")
        ops_logger.info(f"  max_retries: {self.max_retries}")
        ops_logger.info(f"  retry_delay_sec: {self.retry_delay_sec}")


class KalshiClient:
    """Kalshi WebSocket client for BTC 15-min markets.
    
    Uses public API endpoints (no authentication required):
    - REST: https://api.elections.kalshi.com/trade-api/v2
    - WebSocket: wss://api.elections.kalshi.com/trade-api/ws/v2
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        on_second_data: Callable[[str, List[Dict]], Any],  # Callback for completed seconds
        rotation_config: Optional[RotationConfig] = None,  # Optional rotation timing config
    ):
        self.ws_url = config.get("ws_url", "wss://api.elections.kalshi.com/trade-api/ws/v2")
        self.rest_url = config.get("rest_url", "https://api.elections.kalshi.com/trade-api/v2")
        self.markets = config.get("markets", {})
        
        # Callback for completed 1-second aggregated data
        self.on_second_data = on_second_data
        
        # WebSocket state
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.subscribed_market: Optional[str] = None
        
        # Tasks
        self._ws_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._rotation_task: Optional[asyncio.Task] = None
        
        # Stats
        self._messages_received = 0
        self._ticks_normalized = 0
        
        # Rate tracking for per-minute logging
        self._rate_track_start = datetime.now(timezone.utc)
        self._rate_ticks_at_start = 0
        self._rate_task: Optional[asyncio.Task] = None
        
        # REST session
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Rotation loop generation - prevents zombie loops from running
        self._rotation_generation = 0
        
        # Market discovery
        self._discovered_market: Optional[Dict] = None
        self._current_market_end: Optional[datetime] = None
        self._series_ticker: Optional[str] = None
        
        # Rotation throttling - PERSISTS across rotation loops (class-level)
        self._last_rotation_time: Optional[datetime] = None
        
        # Market metadata cache
        self._market_metadata: Dict[str, Any] = {}
        
        # Rotation timing config (from config.json)
        self._rotation_config = rotation_config or RotationConfig.from_file()
        self._rotation_config.log_values()
        
        # Kalshi-specific: Use series_ticker from config (e.g., KXBTC15M)
        btc_market_config = self.markets.get("btc_15min", "")
        if isinstance(btc_market_config, str):
            self._series_ticker = btc_market_config.replace("-", "")
        elif isinstance(btc_market_config, dict):
            self._series_ticker = btc_market_config.get("series_ticker", "KXBTC15M")
        else:
            self._series_ticker = "KXBTC15M"
        
        ops_logger.info(f"Kalshi series ticker: {self._series_ticker}")
    
    async def start(self) -> None:
        """Start the Kalshi client."""
        self.running = True
        
        # Create REST session
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )
        
        # Discover markets
        await self._discover_markets()
        
        # Start WebSocket connection
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        
        # Start rate logging task (logs messages/minute every 30 seconds)
        self._rate_track_start = datetime.now(timezone.utc)
        self._rate_ticks_at_start = self._ticks_normalized
        self._rate_task = asyncio.create_task(self._rate_log_loop())
        
        ops_logger.info("Kalshi client started")
    
    async def _rate_log_loop(self) -> None:
        """Log message processing rate every 30 seconds."""
        # Initial log after 5 seconds
        await asyncio.sleep(5)
        
        while self.running:
            try:
                if not self.running:
                    break
                
                now = datetime.now(timezone.utc)
                elapsed = (now - self._rate_track_start).total_seconds()
                ticks_delta = self._ticks_normalized - self._rate_ticks_at_start
                
                if elapsed > 0:
                    rate_per_minute = ticks_delta * 60 / elapsed
                    ops_logger.info(f"Kalshi processing rate: {rate_per_minute:.0f} ticks/minute (total: {self._ticks_normalized})")
                
                # Reset tracking window
                self._rate_track_start = now
                self._rate_ticks_at_start = self._ticks_normalized
                
                await asyncio.sleep(30)  # Log every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.error(f"Kalshi rate log loop error: {e}")
    
    async def stop(self) -> None:
        """Stop the Kalshi client."""
        self.running = False
        
        # Cancel tasks
        for task in [self._ws_task, self._ping_task, self._rotation_task, self._rate_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
        # Close WebSocket
        if self.ws:
            await self.ws.close()
            self.ws = None
        
        # Close session
        if self._session:
            await self._session.close()
            self._session = None
        
        ops_logger.info(f"Kalshi client stopped. Messages: {self._messages_received}, Normalized: {self._ticks_normalized}")
    
    def _calculate_15m_window_timestamp(self, dt: Optional[datetime] = None) -> int:
        """Calculate Unix timestamp for current 15-min window start."""
        if dt is None:
            dt = datetime.now(timezone.utc)
        
        minute = dt.minute
        if minute < 15:
            window_min = 0
        elif minute < 30:
            window_min = 15
        elif minute < 45:
            window_min = 30
        else:
            window_min = 45
        
        window_start = dt.replace(minute=window_min, second=0, microsecond=0)
        return int(window_start.timestamp())
    
    def _format_kalshi_ticker(self, window_ts: int) -> str:
        """Format Kalshi market ticker from window timestamp.
        
        Format: KXBTC15M-YYYYMMDD-hhmm (e.g., KXBTC15M-20260218-0800)
        """
        dt = datetime.fromtimestamp(window_ts, tz=timezone.utc)
        return f"{self._series_ticker}-{dt.strftime('%Y%m%d-%H%M')}"
    
    async def _discover_markets(self, discover_next: bool = False, max_retries: int = 10, retry_delay: float = 5.0) -> None:
        """Discover BTC 15-min markets via Kalshi public REST API.
        
        Uses GET /markets?series_ticker=KXBTC15M&status=open to find active markets.
        
        Args:
            discover_next: If True, discover the NEXT 15-min window (for rotation).
                          If False, discover current window (for startup).
            max_retries: Maximum number of retry attempts (default: 10).
            retry_delay: Seconds to wait between retries (default: 5.0).
        """
        for attempt in range(max_retries):
            try:
                await self._discover_markets_single_attempt(discover_next)
                # If successful, return
                return
            except Exception as e:
                ops_logger.warning(f"Kalshi discovery attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    ops_logger.info(f"Retrying Kalshi discovery in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    ops_logger.error(f"Kalshi discovery: all {max_retries} attempts failed")
                    raise
    
    async def _discover_markets_single_attempt(self, discover_next: bool = False) -> None:
        """Internal: Single discovery attempt for Kalshi markets via public REST API.
        
        Args:
            discover_next: If True, discover the NEXT 15-min window (for rotation).
                          If False, discover current window (for startup).
        Raises:
            Exception: If market discovery fails or no accepting markets found.
        """
        now = datetime.now(timezone.utc)
        
        if discover_next:
            # For rotation: discover NEXT window (current + 15 min)
            next_window = now + timedelta(minutes=15)
            window_ts = self._calculate_15m_window_timestamp(next_window)
            ops_logger.info(f"Kalshi discovering NEXT market (for rotation, next_window={next_window.strftime('%H:%M:%S')})")
        else:
            # For startup: discover current window
            window_ts = self._calculate_15m_window_timestamp(now)
            ops_logger.info(f"Kalshi discovering CURRENT market (for startup)")
        
        expected_ticker = self._format_kalshi_ticker(window_ts)
        ops_logger.info(f"Kalshi looking for market ticker: {expected_ticker}")
        
        try:
            # Build discovery query for the series
            # Query with min/max close_ts to find markets in the current/next window
            end_time = datetime.fromtimestamp(window_ts, tz=timezone.utc) + timedelta(minutes=15)
            
            # Use series ticker filter
            url = f"{self.rest_url}/markets"
            params = {
                "series_ticker": self._series_ticker,
                "status": "open",
            }
            
            ops_logger.debug(f"Kalshi REST request: {url} with params {params}")
            
            async with self._session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    markets = data.get("markets", [])
                    
                    ops_logger.debug(f"Kalshi returned {len(markets)} markets")
                    
                    # Find the specific market for our target window
                    matching_market = None
                    for market in markets:
                        ticker = market.get("ticker", "")
                        ops_logger.debug(f"Checking market: {ticker}")
                        
                        # Check if this matches our expected ticker pattern
                        if expected_ticker in ticker or ticker.startswith(f"{self._series_ticker}-"):
                            # Parse market details
                            close_time_str = market.get("close_time", "")
                            open_time_str = market.get("open_time", "")
                            status = market.get("status", "")
                            
                            if close_time_str:
                                try:
                                    close_dt = datetime.fromtimestamp(int(close_time_str), tz=timezone.utc)
                                except (ValueError, TypeError):
                                    close_dt = None
                            else:
                                close_dt = None
                            
                            # Check if this is for our target window
                            if status == "active" and close_dt:
                                # For startup: market closes in the future
                                # For rotation: market closes next window
                                if (not discover_next and close_dt > now) or \
                                   (discover_next and close_dt > now + timedelta(minutes=10)):
                                    matching_market = market
                                    ops_logger.info(f"Found matching market: {ticker}")
                                    break
                    
                    if matching_market:
                        # Store market info
                        self._discovered_market = matching_market
                        ticker = matching_market.get("ticker", "")
                        title = matching_market.get("title", "")
                        subtitle = matching_market.get("subtitle", "")
                        yes_price = matching_market.get("yes_price", 0)  # 0-10000 cents
                        no_price = matching_market.get("no_price", 0)
                        
                        # Parse close time
                        close_time_str = matching_market.get("close_time", "")
                        if close_time_str:
                            try:
                                self._current_market_end = datetime.fromtimestamp(int(close_time_str), tz=timezone.utc)
                            except (ValueError, TypeError):
                                self._current_market_end = None
                        
                        # Cache metadata
                        self._market_metadata[ticker] = {
                            "title": title,
                            "subtitle": subtitle,
                            "yes_price": yes_price,
                            "no_price": no_price,
                            "close_time": self._current_market_end,
                        }
                        
                        # Update schema with market info
                        KalshiSchema.set_market_info(ticker, title, subtitle, yes_price, no_price)
                        
                        self.subscribed_market = ticker
                        
                        # Set price_to_beat from market's yes_price (price at open)
                        # This is the oracle price at window start
                        price_to_beat = yes_price / 10000.0 if yes_price else 0.0
                        KalshiSchema.set_price_to_beat(price_to_beat)
                        ops_logger.info(f"Kalshi price_to_beat: {price_to_beat} (from yes_price {yes_price})")
                        
                        ops_logger.info(f"Kalshi discovered market: {ticker} - {title[:50]}...")
                        ops_logger.info(f"Kalshi market ends at: {self._current_market_end.isoformat() if self._current_market_end else 'unknown'}")
                        
                        # Reset buffer for new market
                        KalshiSchema.reset_buffer()
                        
                        # Capture market_end BEFORE updating instance variable
                        market_end_for_rotation = self._current_market_end
                        
                        # Cancel any existing rotation task before starting new one
                        if self._rotation_task:
                            self._rotation_task.cancel()
                            try:
                                await self._rotation_task
                            except asyncio.CancelledError:
                                pass
                        
                        # Increment generation to invalidate old rotation loops
                        self._rotation_generation += 1
                        ops_logger.info(f"Kalshi starting rotation loop generation {self._rotation_generation}")
                        
                        # Start new rotation watcher with CAPTURED market_end
                        self._rotation_task = asyncio.create_task(
                            self._rotation_loop(market_end_for_rotation)
                        )
                        return
                    else:
                        ops_logger.warning(f"No matching market found for ticker: {expected_ticker}")
                else:
                    text = await resp.text()
                    ops_logger.warning(f"Kalshi REST query failed: {resp.status} - {text[:100]}")
                        
        except Exception as e:
            ops_logger.error(f"Kalshi discovery error: {e}")
    
    async def _rotation_loop(self, market_end: datetime) -> None:
        """Monitor for market rotation (new 15-min window).
        
        Uses adaptive checking: checks every 30s normally, but switches to
        frequent checks when within ROTATION_WINDOW of market expiration.
        
        Rotation should ONLY happen within 60-120 seconds before market expiration
        to avoid premature or late rotations.
        
        Args:
            market_end: The expiration time for THIS market (captured at loop start).
        """
        # Use config values
        CHECK_INTERVAL = self._rotation_config.check_interval_sec
        FAST_CHECK_INTERVAL = self._rotation_config.fast_check_interval_sec
        ROTATION_BUFFER_MIN = self._rotation_config.buffer_min_sec
        ROTATION_BUFFER_MAX = self._rotation_config.buffer_max_sec
        MIN_ROTATION_INTERVAL = self._rotation_config.min_rotation_interval_sec
        MAX_EXPIRED_AGE = self._rotation_config.max_expired_age_sec
        
        # Capture generation AND market_end at start - if generation changes, this loop is stale
        my_generation = self._rotation_generation
        my_market_end = market_end  # CAPTURED value - never changes for this loop
        
        ops_logger.info(f"Kalshi rotation loop started (generation {my_generation}, market_end={my_market_end.isoformat() if my_market_end else 'None'})")
        
        while self.running:
            try:
                # Check if this loop is stale (newer generation exists)
                if my_generation != self._rotation_generation:
                    ops_logger.info(f"Kalshi rotation loop {my_generation} is stale (current={self._rotation_generation}), exiting")
                    break
                
                if not my_market_end:
                    ops_logger.debug("Kalshi: No market end time set, waiting...")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue
                
                now = datetime.now(timezone.utc)
                # CRITICAL: Use CAPTURED my_market_end
                seconds_until_expiry = (my_market_end - now).total_seconds()
                
                # Detailed timing logging for debugging
                ops_logger.info(f"[KALSHI ROTATION CHECK] Generation {my_generation} | "
                           f"Current UTC: {now.isoformat()} | "
                           f"Market expires: {my_market_end.isoformat()} | "
                           f"Seconds until expiry: {seconds_until_expiry:.1f}s | "
                           f"Target window: {ROTATION_BUFFER_MIN}-{ROTATION_BUFFER_MAX}s before expiry")
                
                # Prevent multiple rotations for the same market window using CLASS-LEVEL timestamp
                if self._last_rotation_time and seconds_until_expiry > 0:
                    time_since_last_rotation = (now - self._last_rotation_time).total_seconds()
                    if time_since_last_rotation < MIN_ROTATION_INTERVAL:
                        # Market not expired yet AND too soon since last rotation - throttle
                        ops_logger.debug(f"Kalshi rotation throttled: {time_since_last_rotation:.0f}s since last rotation (need {MIN_ROTATION_INTERVAL}s)")
                        await asyncio.sleep(CHECK_INTERVAL)
                        continue
                
                # Check if it's time to rotate
                should_rotate = False
                
                # Case 1: Just expired (0 to 30s after expiry) - OPTIMAL
                if 0 <= -seconds_until_expiry <= 30:
                    should_rotate = True
                    ops_logger.info(f"[KALSHI ROTATION DECISION] AT EXPIRATION: {-seconds_until_expiry:.1f}s after expiry. Rotating now...")
                
                # Case 2: About to expire (5-15s before) - prepare by discovering next market
                elif 5 <= seconds_until_expiry <= 15:
                    should_rotate = False
                    ops_logger.debug(f"Kalshi within discovery window: {seconds_until_expiry:.1f}s until expiry. Will rotate at expiry.")
                
                # Case 3: Expired too long ago (>30s) - emergency rotation  
                elif seconds_until_expiry < -30:
                    should_rotate = True
                    ops_logger.warning(f"[KALSHI ROTATION DECISION] EXPIRED {-seconds_until_expiry:.1f}s ago. Emergency rotation...")
                
                if should_rotate:
                    # Update timestamp
                    self._last_rotation_time = now
                    
                    # Flush remaining data
                    remaining = KalshiSchema.flush_all()
                    if remaining and self.on_second_data:
                        await self.on_second_data("kalshi", remaining)
                    
                    ops_logger.info(f"Kalshi rotating to new market. Current market: {self.subscribed_market}")
                    
                    # Discover CURRENT market (not next) since we're at expiry
                    try:
                        await self._discover_markets(discover_next=False)
                        ops_logger.info(f"Kalshi discovery complete. New market: {self.subscribed_market}")
                        
                        # Force WebSocket reconnect for clean subscription state
                        if self.ws:
                            ops_logger.info("Kalshi closing WebSocket to force reconnect with new market...")
                            try:
                                await self.ws.close()
                            except Exception as e:
                                conn_logger.debug(f"Kalshi WebSocket close error (expected): {e}")
                        # WebSocket loop will auto-reconnect and subscribe to new market
                    except Exception as e:
                        ops_logger.error(f"Kalshi rotation failed: {e}")
                    
                    # Exit old loop - generation already incremented in _discover_markets
                    ops_logger.info(f"[KALSHI ROTATION COMPLETE] Generation {my_generation} finished. Gen {self._rotation_generation} started.")
                    break
                
                # Adaptive sleep: check more frequently when within 1 minute of expiration
                if seconds_until_expiry <= 60:
                    await asyncio.sleep(FAST_CHECK_INTERVAL)
                else:
                    await asyncio.sleep(CHECK_INTERVAL)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.error(f"Kalshi rotation loop error: {e}")
                await asyncio.sleep(CHECK_INTERVAL)
    
    async def _ws_loop(self) -> None:
        """Main WebSocket connection loop with auto-reconnect."""
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while self.running:
            try:
                conn_logger.info(f"Kalshi connecting to WS: {self.ws_url}")
                
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 1
                    
                    # Subscribe to channels for current market
                    await self._subscribe()
                    
                    # Process messages
                    async for message in ws:
                        if not self.running:
                            break
                        
                        try:
                            await self._handle_message(message)
                        except Exception as e:
                            ops_logger.error(f"Kalshi message handling error: {e}")
                
            except websockets.exceptions.ConnectionClosed:
                conn_logger.warning("Kalshi WebSocket closed, reconnecting...")
            except Exception as e:
                conn_logger.error(f"Kalshi WebSocket error: {e}")
            
            if self.running:
                conn_logger.info(f"Kalshi reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def _subscribe(self) -> None:
        """Subscribe to Kalshi WebSocket channels for current market."""
        if not self.ws or not self.subscribed_market:
            ops_logger.warning("Kalshi: No market available - deferring subscription")
            return
        
        # Subscribe to channels for this market
        # Kalshi WebSocket uses subscription messages with market ticker
        sub_msg = {
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["ticker", "orderbook_delta", "trade"],
                "market_tickers": [self.subscribed_market]
            }
        }
        
        await self.ws.send(json.dumps(sub_msg))
        ops_logger.info(f"Kalshi subscribed to {self.subscribed_market} on channels: ticker, orderbook_delta, trade")
    
    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message."""
        self._messages_received += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            conn_logger.warning(f"Kalshi invalid JSON: {message[:200]}")
            return
        
        # Handle different message types
        msg_type = data.get("type", "").lower()
        
        # Check for subscription confirmation
        if msg_type == "subscribed":
            conn_logger.info(f"Kalshi subscription confirmed: {data}")
            return
        
        # Check for subscription error
        if msg_type == "error":
            ops_logger.error(f"Kalshi subscription error: {data}")
            return
        
        # Process data messages
        try:
            second_key = KalshiSchema.normalize_ws_message(data)
            if second_key:
                self._ticks_normalized += 1
                
                # Log periodically
                if self._ticks_normalized % 1000 == 0:
                    ops_logger.debug(f"Kalshi normalized {self._ticks_normalized} ticks")
        except Exception as e:
            ops_logger.error(f"Kalshi normalization error: {e}")
    
    async def flush_completed_seconds(self) -> List[Dict[str, Any]]:
        """Flush completed seconds from buffer and return records for storage.
        
        Called periodically (e.g., every minute) by the main orchestrator.
        """
        records = KalshiSchema.flush_completed_seconds()
        if records:
            ops_logger.debug(f"Kalshi flushing {len(records)} completed seconds")
        return records
    
    async def flush_all(self) -> List[Dict[str, Any]]:
        """Flush all remaining data (for shutdown)."""
        return KalshiSchema.flush_all()
    
    async def _ping_loop(self) -> None:
        """Keep connection alive with periodic heartbeats."""
        while self.running:
            try:
                await asyncio.sleep(20)
                if self.ws and self.ws.open:
                    # Kalshi uses ping/pong, websockets library handles this automatically
                    pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.debug(f"Kalshi ping error: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        now = datetime.now(timezone.utc)
        time_to_end = None
        if self._current_market_end:
            time_to_end = max(0, (self._current_market_end - now).total_seconds())
        
        is_connected = False
        if self.ws:
            try:
                is_connected = getattr(self.ws, 'open', False) or getattr(self.ws, 'state', 0) == 1
            except Exception:
                is_connected = False
        
        return {
            "messages_received": self._messages_received,
            "ticks_normalized": self._ticks_normalized,
            "markets_discovered": 1 if self._discovered_market else 0,
            "subscribed_market": self.subscribed_market,
            "connected": is_connected,
            "current_market_ends_in_sec": time_to_end,
            "current_market_end_iso": self._current_market_end.isoformat() if self._current_market_end else None,
        }
