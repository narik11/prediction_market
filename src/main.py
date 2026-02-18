"""High-performance prediction market data collector main orchestration.

1-second aggregated data with minute-level Parquet writes.
"""

import asyncio
import signal
import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# Configure uvloop if available
try:
    import uvloop
    uvloop.install()
    logging.info("Using uvloop for asyncio event loop")
except ImportError:
    pass

try:
    from .config import Config
    from .storage import StorageManager
    from .buffer import BufferManager
    from .polymarket.client import PolymarketClient
    from .polymarket.schema import PolymarketSchema
    from .kalshi.client import KalshiClient
    from .kalshi.schema import KalshiSchema
except ImportError:
    from config import Config
    from storage import StorageManager
    from buffer import BufferManager
    from polymarket.client import PolymarketClient
    from polymarket.schema import PolymarketSchema
    from kalshi.client import KalshiClient
    from kalshi.schema import KalshiSchema


def setup_logging(config: Config) -> None:
    """Configure logging with file output and rotation."""
    log_dir = Path(config.data_dir).parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create subdirectories for different log types
    (log_dir / "polymarket").mkdir(exist_ok=True)
    (log_dir / "kalshi").mkdir(exist_ok=True)
    (log_dir / "errors").mkdir(exist_ok=True)
    
    # Main log file (rotates daily, keeps 3 days)
    from logging.handlers import TimedRotatingFileHandler
    main_handler = TimedRotatingFileHandler(
        log_dir / "collector.log",
        when="midnight",
        interval=1,
        backupCount=3,
        encoding="utf-8"
    )
    main_handler.setFormatter(logging.Formatter(config.logging.format))
    main_handler.setLevel(getattr(logging, config.logging.level))
    
    # Error-only log file (rotates daily, keeps 10 days)
    error_handler = TimedRotatingFileHandler(
        log_dir / "errors" / "collector_errors.log",
        when="midnight",
        interval=1,
        backupCount=10,
        encoding="utf-8"
    )
    error_handler.setFormatter(logging.Formatter(config.logging.format))
    error_handler.setLevel(logging.ERROR)  # Only errors and above
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(config.logging.format))
    console_handler.setLevel(getattr(logging, config.logging.level))
    
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, config.logging.level),
        handlers=[main_handler, error_handler, console_handler],
        force=True  # Override any existing config
    )
    
    # Create connection-specific loggers (3 days retention)
    # These loggers only receive connection events and don't propagate to collector.log
    
    # Polymarket connection logger - only connection events, no propagation
    conn_handler_pm = TimedRotatingFileHandler(
        log_dir / "polymarket" / "connection.log",
        when="midnight",
        interval=1,
        backupCount=3,
        encoding="utf-8"
    )
    conn_logger_pm = logging.getLogger("polymarket.connection")
    conn_logger_pm.addHandler(conn_handler_pm)
    conn_logger_pm.propagate = False  # Don't bubble to root/collector.log
    
    # Kalshi connection logger - only connection events, no propagation
    conn_handler_kal = TimedRotatingFileHandler(
        log_dir / "kalshi" / "connection.log",
        when="midnight",
        interval=1,
        backupCount=3,
        encoding="utf-8"
    )
    conn_logger_kal = logging.getLogger("kalshi.connection")
    conn_logger_kal.addHandler(conn_handler_kal)
    conn_logger_kal.propagate = False  # Don't bubble to root/collector.log
    
    # Ops loggers (polymarket, kalshi) go to collector.log via root logger - no extra handlers needed
    
    logging.info(f"Logging configured: {log_dir}/collector.log")


class PredictionCollector:
    """Main orchestrator for prediction market data collection."""
    
    def __init__(self, config: Optional[Config] = None):
        self.config = config or Config.load()
        setup_logging(self.config)
        self.logger = logging.getLogger("main")
        
        # Components
        self.storage: Optional[StorageManager] = None
        self.buffer_manager: Optional[BufferManager] = None
        self.polymarket: Optional[PolymarketClient] = None
        self.kalshi: Optional[KalshiClient] = None
        
        # State
        self.running = False
        self._shutdown_event = asyncio.Event()
        
        # Tasks
        self._flush_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        
        # Heartbeat
        self._heartbeat_file: Optional[Path] = None
        
        # Stats
        self._start_time: Optional[float] = None
        self._records_written = 0
        self._last_flush_time: Optional[datetime] = None
    
    async def initialize(self) -> None:
        """Initialize all components."""
        self.logger.info("Initializing Prediction Collector...")
        
        # Ensure data directory exists
        self.config.ensure_data_dir()
        
        # Initialize storage
        self.storage = StorageManager(
            data_dir=self.config.data_dir,
            compression=self.config.storage.compression,
            row_group_size=self.config.storage.row_group_size,
        )
        
        # Setup heartbeat file (for watchdog monitoring)
        monitor_dir = Path(self.config.data_dir).resolve() / "monitor"
        monitor_dir.mkdir(exist_ok=True)
        self._heartbeat_file = monitor_dir / "collector_heartbeat.txt"
        self.logger.info(f"Heartbeat file: {self._heartbeat_file}")
        
        # Register schemas
        self.storage.register_schema("polymarket", PolymarketSchema.TICK_SCHEMA)
        self.storage.register_schema("kalshi", KalshiSchema.TICK_SCHEMA)
        
        # Initialize BufferManager
        self.buffer_manager = BufferManager(
            max_rows_per_source=self.config.buffer.max_rows_per_source,
            flush_threshold=self.config.buffer.flush_threshold,
            flush_interval_sec=self.config.buffer.flush_interval_sec,
        )
        
        # Connect buffer to storage
        self.buffer_manager.set_flush_callback(self._on_buffer_flush)
        await self.buffer_manager.start()
        self.logger.info("BufferManager started")
        
        # Initialize Polymarket client with buffer callback
        if self.config.polymarket.enabled:
            try:
                from polymarket.client import RotationConfig
            except ImportError:
                from .polymarket.client import RotationConfig
            rotation_config = RotationConfig.from_file()
            self.polymarket = PolymarketClient(
                config={
                    "ws_url": self.config.polymarket.ws_url,
                    "rest_url": self.config.polymarket.rest_url,
                    "markets": self.config.polymarket.markets,
                },
                on_second_data=self.buffer_manager.put_batch,  # Send to BufferManager
                rotation_config=rotation_config,
            )
        
        # Initialize Kalshi client if enabled
        if self.config.kalshi.enabled:
            try:
                from kalshi.client import RotationConfig as KalshiRotationConfig
            except ImportError:
                from .kalshi.client import RotationConfig as KalshiRotationConfig
            
            kalshi_rotation_config = KalshiRotationConfig.from_file()
            self.kalshi = KalshiClient(
                config={
                    "ws_url": self.config.kalshi.ws_url,
                    "rest_url": self.config.kalshi.rest_url,
                    "markets": self.config.kalshi.markets,
                },
                on_second_data=self.buffer_manager.put_batch,  # Send to BufferManager
                rotation_config=kalshi_rotation_config,
            )
        
        self.logger.info("Initialization complete")
    
    async def _on_buffer_flush(self, source: str, records: List[Dict]) -> None:
        """Callback for BufferManager flush events."""
        if not records or not self.storage:
            return
        
        try:
            count = await self.storage.write_batch(source, records)
            self._records_written += count
            self._last_flush_time = datetime.now(timezone.utc)
            
            if count > 0:
                self.logger.info(f"Flushed {count} records from {source} to storage (total: {self._records_written})")
        except Exception as e:
            self.logger.error(f"Buffer flush error for {source}: {e}")
    
    async def start(self) -> None:
        """Start all components."""
        self.running = True
        self._start_time = asyncio.get_event_loop().time()
        
        self.logger.info("Starting Prediction Collector...")
        
        # Start clients
        if self.polymarket:
            await self.polymarket.start()
        
        if self.kalshi:
            await self.kalshi.start()
        
        # Start flush loop to move data from Schema 1s buffer to BufferManager
        self._flush_task = asyncio.create_task(self._schema_flush_loop())
        
        # Start heartbeat for watchdog monitoring
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        self.logger.info("All components started. Press Ctrl+C to exit.")
        
        # Wait for shutdown
        await self._shutdown_event.wait()
    
    async def _schema_flush_loop(self) -> None:
        """Flush completed seconds from clients to BufferManager every 30 seconds."""
        FLUSH_INTERVAL = 30  # seconds
        
        # Wait a few seconds for initial data collection
        await asyncio.sleep(5)
        
        self.logger.info("Schema flush loop started (60s interval)")
        
        while self.running:
            try:
                await asyncio.sleep(FLUSH_INTERVAL)
                
                if not self.running:
                    break
                
                # Flush completed seconds from Polymarket to BufferManager
                if self.polymarket and self.buffer_manager:
                    self.logger.debug("Calling polymarket.flush_completed_seconds()")
                    records = await self.polymarket.flush_completed_seconds()
                    if records:
                        count = await self.buffer_manager.put_batch("polymarket", records)
                        self.logger.info(f"Queued {count} records from Polymarket to BufferManager")
                    else:
                        self.logger.debug("No records from Polymarket")
                
                # Flush from Kalshi (if implemented)
                if self.kalshi and self.buffer_manager and hasattr(self.kalshi, 'flush_completed_seconds'):
                    self.logger.debug("Calling kalshi.flush_completed_seconds()")
                    records = await self.kalshi.flush_completed_seconds()
                    if records:
                        count = await self.buffer_manager.put_batch("kalshi", records)
                        self.logger.info(f"Queued {count} Kalshi records to BufferManager")
                    else:
                        self.logger.debug("No records from Kalshi")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Schema flush loop error: {e}", exc_info=True)
    
    async def _heartbeat_loop(self) -> None:
        """Write heartbeat timestamp for watchdog monitoring."""
        HEARTBEAT_INTERVAL = 30  # seconds
        
        self.logger.info(f"Heartbeat loop started ({HEARTBEAT_INTERVAL}s interval)")
        
        # Write initial heartbeat immediately
        if self._heartbeat_file:
            try:
                self._heartbeat_file.write_text(
                    datetime.now(timezone.utc).isoformat()
                )
                self.logger.info("Initial heartbeat written")
            except Exception as e:
                self.logger.error(f"Failed to write initial heartbeat: {e}")
        
        while self.running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)
                
                if not self.running:
                    break
                
                # Write current timestamp to heartbeat file
                if self._heartbeat_file:
                    try:
                        self._heartbeat_file.write_text(
                            datetime.now(timezone.utc).isoformat()
                        )
                        self.logger.debug("Heartbeat written")
                    except Exception as e:
                        self.logger.error(f"Failed to write heartbeat: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Heartbeat loop error: {e}")
    
    async def stop(self) -> None:
        """Stop all components gracefully."""
        self.logger.info("Shutting down...")
        self.running = False
        
        # Cancel schema flush loop
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Cancel heartbeat loop
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        # Stop clients first (no more incoming data)
        if self.polymarket:
            try:
                await asyncio.wait_for(self.polymarket.stop(), timeout=10.0)
                self.logger.info("Polymarket client stopped")
            except asyncio.TimeoutError:
                self.logger.warning("Polymarket client stop timeout")
        
        if self.kalshi:
            try:
                await asyncio.wait_for(self.kalshi.stop(), timeout=10.0)
                self.logger.info("Kalshi client stopped")
            except asyncio.TimeoutError:
                self.logger.warning("Kalshi client stop timeout")
        
        # Flush remaining data from clients through BufferManager
        self.logger.info("Flushing remaining client data...")
        
        if self.polymarket:
            try:
                remaining = await self.polymarket.flush_all()
                if remaining and self.buffer_manager:
                    await self.buffer_manager.put_batch("polymarket", remaining)
                    self.logger.info(f"Queued {len(remaining)} final records from Polymarket")
            except Exception as e:
                self.logger.error(f"Polymarket final flush error: {e}")
        
        # Flush remaining Kalshi data
        if self.kalshi:
            try:
                remaining = await self.kalshi.flush_all()
                if remaining and self.buffer_manager:
                    await self.buffer_manager.put_batch("kalshi", remaining)
                    self.logger.info(f"Queued {len(remaining)} final records from Kalshi")
            except Exception as e:
                self.logger.error(f"Kalshi final flush error: {e}")
        
        # Stop BufferManager (flushes all pending data to storage)
        if self.buffer_manager:
            try:
                await self.buffer_manager.stop()
                self.logger.info("BufferManager stopped")
            except Exception as e:
                self.logger.error(f"BufferManager stop error: {e}")
        
        # Close storage
        if self.storage:
            await self.storage.close_all()
            self.logger.info("Storage closed")
        
        self.logger.info(f"Shutdown complete. Total records written: {self._records_written}")
    
    def trigger_shutdown(self) -> None:
        """Signal shutdown."""
        self._shutdown_event.set()
    
    def get_stats(self) -> dict:
        """Get runtime statistics."""
        stats = {
            "running": self.running,
            "uptime_sec": 0,
            "records_written": self._records_written,
            "last_flush": self._last_flush_time.isoformat() if self._last_flush_time else None,
        }
        
        if self._start_time:
            stats["uptime_sec"] = asyncio.get_event_loop().time() - self._start_time
        
        if self.storage:
            stats["storage"] = self.storage.get_stats()
        
        if self.polymarket:
            stats["polymarket"] = self.polymarket.get_stats()
        
        if self.kalshi:
            stats["kalshi"] = self.kalshi.get_stats()
        
        return stats


def setup_signal_handlers(collector: PredictionCollector) -> None:
    """Setup signal handlers for graceful shutdown."""
    
    def signal_handler(sig, frame):
        logging.info(f"Received signal {sig}, initiating shutdown...")
        collector.trigger_shutdown()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        signal.signal(signal.SIGBREAK, signal_handler)
    except AttributeError:
        pass


async def main():
    """Main entry point."""
    config_path = os.getenv("CONFIG_PATH", "./config.json")
    config = Config.load(config_path)
    
    collector = PredictionCollector(config)
    await collector.initialize()
    
    setup_signal_handlers(collector)
    
    try:
        await collector.start()
    except Exception as e:
        logging.error(f"Runtime error: {e}", exc_info=True)
    finally:
        await collector.stop()


if __name__ == "__main__":
    asyncio.run(main())
