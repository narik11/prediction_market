"""Polymarket WebSocket and REST client."""

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
    from .schema import PolymarketSchema
    from .rtds import RTDSClient
except ImportError:
    from polymarket.schema import PolymarketSchema
    from polymarket.rtds import RTDSClient


# Two separate loggers for proper log routing:
# - ops_logger: rotation, discovery, decisions → collector.log (via root logger)
# - conn_logger: WebSocket events → connection.log (propagate=False in main.py)
ops_logger = logging.getLogger("polymarket")
conn_logger = logging.getLogger("polymarket.connection")


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
        ops_logger.info("Rotation Configuration:")
        ops_logger.info(f"  check_interval_sec: {self.check_interval_sec}")
        ops_logger.info(f"  fast_check_interval_sec: {self.fast_check_interval_sec}")
        ops_logger.info(f"  buffer_min_sec: {self.buffer_min_sec}")
        ops_logger.info(f"  buffer_max_sec: {self.buffer_max_sec}")
        ops_logger.info(f"  min_rotation_interval_sec: {self.min_rotation_interval_sec}")
        ops_logger.info(f"  max_retries: {self.max_retries}")
        ops_logger.info(f"  retry_delay_sec: {self.retry_delay_sec}")


class PolymarketClient:
    """Polymarket WebSocket client for 15-min BTC markets."""
    
    def __init__(
        self,
        config: Dict[str, Any],
        on_second_data: Callable[[str, List[Dict]], Any],  # Callback for completed seconds
        rotation_config: Optional[RotationConfig] = None,  # Optional rotation timing config
    ):
        self.ws_url = config.get("ws_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
        self.rest_url = config.get("rest_url", "https://clob.polymarket.com")
        self.markets = config.get("markets", {})
        
        # Callback for completed 1-second aggregated data
        self.on_second_data = on_second_data
        
        # WebSocket state
        self.ws: Optional[websockets.WebSocketClientProtocol] = None
        self.running = False
        self.subscribed_tokens: set = set()
        
        # Tasks
        self._ws_task: Optional[asyncio.Task] = None
        self._ping_task: Optional[asyncio.Task] = None
        self._rotation_task: Optional[asyncio.Task] = None
        self._oracle_task: Optional[asyncio.Task] = None  # RTDS oracle price updater
        
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
        self._discovered_markets: List[Dict] = []
        self._current_market_end: Optional[datetime] = None
        self._condition_id: Optional[str] = None
        
        # Rotation throttling - PERSISTS across rotation loops (class-level)
        self._last_rotation_time: Optional[datetime] = None
        
        # Discovery flag - blocks WebSocket subscription during market discovery
        self._discovery_in_progress = False
        
        # RTDS client for oracle prices
        self._rtds: Optional[RTDSClient] = None
        self._price_to_beat: float = 0.0
        
        # Market metadata cache - stores acceptingOrders per condition_id
        self._market_metadata: Dict[str, bool] = {}
        
        # Rotation timing config (from config.json)
        self._rotation_config = rotation_config or RotationConfig.from_file()
        self._rotation_config.log_values()
    
    async def start(self) -> None:
        """Start the Polymarket client."""
        self.running = True
        
        # Create REST session
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )
        
        # Start RTDS client for oracle prices (before discovery for early price capture)
        self._rtds = RTDSClient()
        await self._rtds.start()
        ops_logger.info("RTDS oracle client started")
        
        # Brief wait for initial price
        await asyncio.sleep(1)
        
        # Discover markets and set token metadata
        await self._discover_markets()
        
        # Start WebSocket connection
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._ping_task = asyncio.create_task(self._ping_loop())
        
        # Start oracle price updater (updates schema with live RTDS price every second)
        self._oracle_task = asyncio.create_task(self._update_oracle_price_loop())
        
        # Start rate logging task (logs messages/minute every minute)
        self._rate_track_start = datetime.now(timezone.utc)
        self._rate_ticks_at_start = self._ticks_normalized
        self._rate_task = asyncio.create_task(self._rate_log_loop())
        
        ops_logger.info("Polymarket client started")
    
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
                    ops_logger.info(f"Processing rate: {rate_per_minute:.0f} ticks/minute (total: {self._ticks_normalized})")
                
                # Reset tracking window
                self._rate_track_start = now
                self._rate_ticks_at_start = self._ticks_normalized
                
                await asyncio.sleep(30)  # Log every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.error(f"Rate log loop error: {e}")
    
    async def _update_oracle_price_loop(self) -> None:
        """Background task to update oracle_price in schema from RTDS."""
        while self.running:
            try:
                if self._rtds:
                    oracle_price = self._rtds.get_oracle_price()
                    if oracle_price > 0:
                        # Pass RTDS timestamp when setting oracle price
                        oracle_ts = self._rtds.last_update if self._rtds else None
                        PolymarketSchema.set_oracle_price(oracle_price, oracle_ts)
                await asyncio.sleep(1)  # Update every second
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.error(f"Oracle price update error: {e}")
                await asyncio.sleep(1)
    
    async def stop(self) -> None:
        """Stop the Polymarket client."""
        self.running = False
        
        # Cancel tasks
        for task in [self._ws_task, self._ping_task, self._rotation_task, self._oracle_task, self._rate_task]:
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
        
        # Stop RTDS client
        if self._rtds:
            await self._rtds.stop()
            self._rtds = None
        
        ops_logger.info(f"Polymarket client stopped. Messages: {self._messages_received}, Normalized: {self._ticks_normalized}")
    
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
    
    async def _discover_markets(self, discover_next: bool = False, max_retries: int = 10, retry_delay: float = 5.0) -> None:
        """Discover BTC 15-min markets via slug-based API.
        
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
                ops_logger.warning(f"Discovery attempt {attempt + 1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    ops_logger.info(f"Retrying discovery in {retry_delay} seconds...")
                    await asyncio.sleep(retry_delay)
                else:
                    ops_logger.error(f"All {max_retries} discovery attempts failed")
                    raise
        
    async def _discover_markets_single_attempt(self, discover_next: bool = False) -> None:
        """Internal: Single discovery attempt for BTC 15-min markets via slug-based API.
        
        Args:
            discover_next: If True, discover the NEXT 15-min window (for rotation).
                          If False, discover current window (for startup).
        Raises:
            Exception: If market discovery fails or no accepting markets found.
        """
        gamma_url = "https://gamma-api.polymarket.com"
        
        # Calculate 15-min window
        now = datetime.now(timezone.utc)
        
        if discover_next:
            # For rotation: discover NEXT window (current + 15 min)
            # BUT if we're already past the current window's end, just use current time
            next_window = now + timedelta(minutes=15)
            window_ts = self._calculate_15m_window_timestamp(next_window)
            slug = f"btc-updown-15m-{window_ts}"
            ops_logger.info(f"Discovering NEXT market via slug: {slug} (for rotation, next_window={next_window.strftime('%H:%M:%S')})")
        else:
            # For startup: discover current window
            window_ts = self._calculate_15m_window_timestamp(now)
            slug = f"btc-updown-15m-{window_ts}"
            ops_logger.info(f"Discovering CURRENT market via slug: {slug} (for startup)")
        
        try:
            url = f"{gamma_url}/markets/slug/{slug}"
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    market = await resp.json()
                    
                    # Extract market details
                    condition_id = market.get("conditionId")
                    question = market.get("question", "")
                    active = market.get("active", False)
                    accepting_orders = market.get("acceptingOrders", False)
                    
                    # Cache market metadata for filtering
                    if condition_id:
                        self._market_metadata[condition_id] = accepting_orders
                    
                    end_date_str = market.get("endDate")
                    token_ids_str = market.get("clobTokenIds", "[]")
                    
                    # Parse token IDs from JSON string
                    try:
                        token_ids = json.loads(token_ids_str) if token_ids_str else []
                    except json.JSONDecodeError:
                        token_ids = []
                    
                    if condition_id and token_ids:
                        # Get outcome texts from market response
                        outcomes_str = market.get("outcomes", '["YES", "NO"]')
                        try:
                            outcomes = json.loads(outcomes_str) if isinstance(outcomes_str, str) else outcomes_str
                        except json.JSONDecodeError:
                            outcomes = ["YES", "NO"]
                        
                        if len(outcomes) != len(token_ids):
                            outcomes = ["YES", "NO"][:len(token_ids)]
                        
                        # Build enriched token list
                        tokens = []
                        for i, tid in enumerate(token_ids):
                            outcome = outcomes[i] if i < len(outcomes) else f"OUTCOME_{i}"
                            tokens.append({
                                "token_id": str(tid),
                                "outcome": outcome,
                                "name": outcome,
                                "question": question,
                            })
                        
                        # Store market info
                        market_info = {
                            "market_slug": market.get("slug"),
                            "condition_id": condition_id,
                            "question": question,
                            "tokens": tokens,
                        }
                        self._discovered_markets = [market_info]
                        self._condition_id = condition_id
                        
                        # Set token metadata for schema enrichment
                        PolymarketSchema.set_tokens(tokens, condition_id)
                        
                        # Set market info explicitly to ensure market_id and market_question are updated
                        PolymarketSchema.set_market_info(condition_id, question)
                        
                        # Capture price_to_beat from RTDS oracle (BTC price at window start)
                        # IMPORTANT: Must use historical price at window_ts, not current price!
                        if self._rtds:
                            # Calculate exact market start time from window_ts
                            market_start_time = datetime.fromtimestamp(window_ts, tz=timezone.utc)
                            price_to_beat = self._rtds.get_first_price_after(market_start_time)
                            
                            if price_to_beat > 0:
                                self._price_to_beat = price_to_beat
                                PolymarketSchema.set_price_to_beat(price_to_beat)
                                ops_logger.info(f"Price to beat captured: {price_to_beat} (at or after {market_start_time.isoformat()})")
                            else:
                                # Fallback: try to get current price if historical not available
                                price_to_beat = self._rtds.capture_price_to_beat()
                                if price_to_beat > 0:
                                    self._price_to_beat = price_to_beat
                                    PolymarketSchema.set_price_to_beat(price_to_beat)
                                    ops_logger.warning(f"Price to beat captured from current price: {price_to_beat} (historical unavailable)")
                                else:
                                    ops_logger.warning("RTDS price not available yet for price_to_beat")
                        
                        # Track token IDs and end time
                        self.subscribed_tokens = set(token_ids)
                        
                        # Clear discovery flag - WebSocket can now subscribe with new tokens
                        self._discovery_in_progress = False
                        ops_logger.info(f"Discovery complete - subscribed_tokens updated with {len(token_ids)} new tokens")
                        
                        if end_date_str:
                            try:
                                self._current_market_end = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                                ops_logger.info(f"Market ends at: {self._current_market_end.isoformat()}")
                            except Exception:
                                self._current_market_end = None
                        
                        ops_logger.info(f"Discovered active market: {question[:50]}... ({len(token_ids)} tokens)")
                        
                        # Reset buffer for new market
                        PolymarketSchema.reset_buffer()
                        
                        # Capture market_end BEFORE updating instance variable
                        # This prevents phantom rotations in old loops
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
                        ops_logger.info(f"Starting rotation loop generation {self._rotation_generation}")
                        
                        # Start new rotation watcher with CAPTURED market_end
                        # Critical: pass market_end as parameter so old loops don't see new values
                        self._rotation_task = asyncio.create_task(
                            self._rotation_loop(market_end_for_rotation)
                        )
                        return
                    else:
                        ops_logger.warning(f"Market not active/accepting orders: active={active}, acceptingOrders={accepting_orders}")
                else:
                    text = await resp.text()
                    ops_logger.warning(f"Slug query failed: {resp.status} - {text[:100]}")
                        
        except Exception as e:
            ops_logger.error(f"Slug-based discovery error: {e}")
        
        # Fallback: try next window
        ops_logger.warning("Current window not found, trying next 15-min window...")
        await self._try_next_window()
        
        # If we reach here, discovery failed - raise exception
        if not self.subscribed_tokens:
            raise Exception(f"No markets found via discovery after trying current and next windows")
    
    async def _try_next_window(self) -> None:
        """Try to discover the next 15-min window market."""
        gamma_url = "https://gamma-api.polymarket.com"
        
        now = datetime.now(timezone.utc)
        next_window = now + timedelta(minutes=15)
        window_ts = self._calculate_15m_window_timestamp(next_window)
        slug = f"btc-updown-15m-{window_ts}"
        
        ops_logger.info(f"Trying next window slug: {slug}")
        
        try:
            url = f"{gamma_url}/markets/slug/{slug}"
            async with self._session.get(url) as resp:
                if resp.status == 200:
                    market = await resp.json()
                    token_ids_str = market.get("clobTokenIds", "[]")
                    
                    try:
                        token_ids = json.loads(token_ids_str) if token_ids_str else []
                    except json.JSONDecodeError:
                        token_ids = []
                    
                    if token_ids and market.get("acceptingOrders"):
                        self.subscribed_tokens = set(token_ids)
                        ops_logger.info(f"Found next window market: {market.get('question', '')[:50]}...")
                        return
        except Exception as e:
            ops_logger.error(f"Next window discovery error: {e}")
        
        # No hardcoded fallback - keep retrying via caller
        ops_logger.error("No markets found via discovery after all retries")
        self._discovery_in_progress = False  # Clear flag so caller can retry
        # Don't set subscribed_tokens - leave empty so caller knows discovery failed
    
    async def _rotation_loop(self, market_end: datetime) -> None:
        """Monitor for market rotation (new 15-min window).
        
        Uses adaptive checking: checks every 30s normally, but switches to
        frequent checks when within ROTATION_WINDOW of market expiration.
        
        Rotation should ONLY happen within 60-120 seconds before market expiration
        to avoid premature or late rotations.
        
        Args:
            market_end: The expiration time for THIS market (captured at loop start).
                       CRITICAL: Must NEVER use self._current_market_end to avoid
                       phantom rotations when new markets are discovered.
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
        
        ops_logger.info(f"Rotation loop started (generation {my_generation}, market_end={my_market_end.isoformat()})")
        
        while self.running:
            try:
                # Check if this loop is stale (newer generation exists)
                if my_generation != self._rotation_generation:
                    ops_logger.info(f"Rotation loop {my_generation} is stale (current={self._rotation_generation}), exiting")
                    break
                
                if not my_market_end:
                    ops_logger.debug("No market end time set, waiting...")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue
                
                now = datetime.now(timezone.utc)
                # CRITICAL: Use CAPTURED my_market_end, not self._current_market_end
                seconds_until_expiry = (my_market_end - now).total_seconds()
                
                # Detailed timing logging for debugging
                ops_logger.info(f"[ROTATION CHECK] Generation {my_generation} | "
                           f"Current UTC: {now.isoformat()} | "
                           f"Market expires: {my_market_end.isoformat()} | "
                           f"Seconds until expiry: {seconds_until_expiry:.1f}s | "
                           f"Target window: {ROTATION_BUFFER_MIN}-{ROTATION_BUFFER_MAX}s before expiry")
                
                # Prevent multiple rotations for the same market window using CLASS-LEVEL timestamp
                # BUT allow rotation if market has already expired (force rotation)
                if self._last_rotation_time and seconds_until_expiry > 0:
                    time_since_last_rotation = (now - self._last_rotation_time).total_seconds()
                    if time_since_last_rotation < MIN_ROTATION_INTERVAL:
                        # Market not expired yet AND too soon since last rotation - throttle
                        ops_logger.debug(f"Rotation throttled: {time_since_last_rotation:.0f}s since last rotation (need {MIN_ROTATION_INTERVAL}s)")
                        await asyncio.sleep(CHECK_INTERVAL)
                        continue
                    # else: we're past MIN_ROTATION_INTERVAL, allow rotation check to proceed
                # Note: If seconds_until_expiry <= 0 (market expired), throttling is bypassed
                
                # Check if it's time to rotate - rotate AT or just AFTER expiry when tokens are valid
                should_rotate = False
                
                # Case 1: Just expired (0 to 30s after expiry) - OPTIMAL, tokens are now valid
                if 0 <= -seconds_until_expiry <= 30:
                    should_rotate = True
                    ops_logger.info(f"[ROTATION DECISION] AT EXPIRATION: {-seconds_until_expiry:.1f}s after expiry. Rotating now...")
                
                # Case 2: About to expire (5-15s before) - prepare by discovering next market
                elif 5 <= seconds_until_expiry <= 15:
                    # Only discover, don't switch yet
                    should_rotate = False
                    ops_logger.debug(f"Within discovery window: {seconds_until_expiry:.1f}s until expiry. Will rotate at expiry.")
                
                # Case 3: Expired too long ago (>30s) - emergency rotation  
                elif seconds_until_expiry < -30:
                    should_rotate = True
                    ops_logger.warning(f"[ROTATION DECISION] EXPIRED {-seconds_until_expiry:.1f}s ago. Emergency rotation...")
                
                # Case 4: Too early - wait
                else:
                    pass  # Just continue waiting
                
                if should_rotate:
                    # Update timestamp
                    self._last_rotation_time = now
                    
                    # Flush remaining data
                    remaining = PolymarketSchema.flush_all()
                    if remaining and self.on_second_data:
                        await self.on_second_data("polymarket", remaining)
                    
                    ops_logger.info(f"Rotating to new market. Current tokens: {len(self.subscribed_tokens)}")
                    
                    # Discover CURRENT market (not next) since we're at expiry
                    try:
                        await self._discover_markets(discover_next=False)
                        ops_logger.info(f"Discovery complete. New tokens: {len(self.subscribed_tokens)}")
                        
                        # Force WebSocket reconnect for clean subscription state
                        if self.ws:
                            ops_logger.info("Closing WebSocket to force reconnect with new tokens...")
                            try:
                                await self.ws.close()
                            except Exception as e:
                                conn_logger.debug(f"WebSocket close error (expected): {e}")
                        # WebSocket loop will auto-reconnect and subscribe to new tokens
                    except Exception as e:
                        ops_logger.error(f"Rotation failed: {e}")
                    
                    # Exit old loop - generation already incremented in _discover_markets
                    ops_logger.info(f"[ROTATION COMPLETE] Generation {my_generation} finished. Gen {self._rotation_generation} started.")
                    break
                
                # Adaptive sleep: check more frequently when within 5 minutes of expiration
                if seconds_until_expiry <= 60:  # Within 1 minute
                    await asyncio.sleep(FAST_CHECK_INTERVAL)
                else:
                    await asyncio.sleep(CHECK_INTERVAL)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.error(f"Rotation loop error: {e}")
                await asyncio.sleep(CHECK_INTERVAL)
    
    async def _ws_loop(self) -> None:
        """Main WebSocket connection loop with auto-reconnect."""
        reconnect_delay = 1
        max_reconnect_delay = 60
        
        while self.running:
            try:
                conn_logger.info(f"Connecting to Polymarket WS: {self.ws_url}")
                
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                ) as ws:
                    self.ws = ws
                    reconnect_delay = 1
                    
                    # Subscribe
                    await self._subscribe()
                    
                    # Process messages
                    async for message in ws:
                        if not self.running:
                            break
                        
                        try:
                            await self._handle_message(message)
                        except Exception as e:
                            ops_logger.error(f"Message handling error: {e}")
                
            except websockets.exceptions.ConnectionClosed:
                conn_logger.warning("Polymarket WebSocket closed, reconnecting...")
            except Exception as e:
                conn_logger.error(f"WebSocket error: {e}")
            
            if self.running:
                conn_logger.info(f"Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
    
    async def _subscribe(self) -> None:
        """Subscribe to market data streams with atomic token capture.
        
        Takes a snapshot of tokens to prevent race conditions with
        concurrent market rotation.
        """
        if not self.ws:
            return
        
        # Atomic capture: snapshot tokens to prevent races
        # If tokens change between check and subscription, we use the snapshot
        token_snapshot = self.subscribed_tokens.copy()
        
        if not token_snapshot:
            ops_logger.warning("No tokens available - deferring subscription")
            return
        
        token_list = list(token_snapshot)
        if token_list:
            sub_msg = {
                "assets_ids": token_list,
                "type": "market"
            }
            await self.ws.send(json.dumps(sub_msg))
            # Log first few token IDs for debugging
            token_preview = ", ".join([t[:20] + "..." for t in token_list[:2]])
            ops_logger.info(f"Subscribed to {len(token_list)} Polymarket tokens: {token_preview}")
        else:
            # CRITICAL: Don't subscribe with empty tokens - this kills the WebSocket feed
            # This can happen if _ws_loop reconnects during market discovery
            ops_logger.warning("No tokens to subscribe to - skipping subscribe to prevent empty feed. Will retry on next reconnect.")
    
    async def _handle_message(self, message: str) -> None:
        """Handle incoming WebSocket message.
        
        Processes batches concurrently for efficiency while limiting
        max concurrency to prevent overwhelming the event loop.
        """
        self._messages_received += 1
        
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            ops_logger.warning(f"Invalid JSON: {message[:200]}")
            return
        
        # Handle list of messages with concurrency limit
        if isinstance(data, list):
            if not data:
                return
            
            # Process large batches with semaphore to prevent blocking
            MAX_CONCURRENT = 100
            if len(data) > MAX_CONCURRENT:
                # Process in chunks to limit concurrency
                for i in range(0, len(data), MAX_CONCURRENT):
                    chunk = data[i:i + MAX_CONCURRENT]
                    await asyncio.gather(
                        *[self._process_single_message(item) for item in chunk],
                        return_exceptions=True
                    )
            else:
                # Small batches: process concurrently
                await asyncio.gather(
                    *[self._process_single_message(item) for item in data],
                    return_exceptions=True
                )
        else:
            await self._process_single_message(data)
    
    async def _process_single_message(self, data: dict) -> None:
        """Process a single message dict - add to 1-second aggregation."""
        msg_type = data.get("type", "").lower()
        
        # Skip non-data messages
        if msg_type in ("connection", "success", "error"):
            if msg_type == "error":
                ops_logger.error(f"Polymarket error: {data}")
            return
        
        # Skip PRICE_CHANGE events
        event_type = data.get("event_type", "").lower()
        if event_type == "price_change":
            return
        
        # Add to 1-second aggregation buffer
        try:
            second_key = PolymarketSchema.normalize_ws_message(data)
            if second_key:
                self._ticks_normalized += 1
                
                # Log periodically (DEBUG level to avoid flooding)
                if self._ticks_normalized % 1000 == 0:
                    ops_logger.debug(f"Normalized {self._ticks_normalized} ticks")
                    
        except Exception as e:
            ops_logger.error(f"Normalization error: {e}")
    
    async def flush_completed_seconds(self) -> List[Dict[str, Any]]:
        """Flush completed seconds from buffer and return records for storage.
        
        Called periodically (e.g., every minute) by the main orchestrator.
        """
        records = PolymarketSchema.flush_completed_seconds()
        if records:
            ops_logger.debug(f"Flushing {len(records)} completed seconds")
        return records
    
    async def flush_all(self) -> List[Dict[str, Any]]:
        """Flush all remaining data (for shutdown)."""
        return PolymarketSchema.flush_all()
    
    async def _ping_loop(self) -> None:
        """Keep connection alive."""
        while self.running:
            try:
                await asyncio.sleep(20)
                if self.ws and self.ws.open:
                    await self.ws.send(json.dumps({"type": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception as e:
                ops_logger.debug(f"Ping error: {e}")
    
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
            "markets_discovered": len(self._discovered_markets),
            "subscribed_tokens": len(self.subscribed_tokens),
            "connected": is_connected,
            "current_market_ends_in_sec": time_to_end,
            "current_market_end_iso": self._current_market_end.isoformat() if self._current_market_end else None,
        }
