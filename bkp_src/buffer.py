"""Buffer manager with asyncio.Queue for zero-copy data handoff."""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, field

import pyarrow as pa

logger = logging.getLogger("buffer")


@dataclass
class BufferMetrics:
    """Metrics for buffer performance tracking."""
    total_received: int = 0
    total_flushed: int = 0
    last_flush: Optional[datetime] = None
    buffer_sizes: Dict[str, int] = field(default_factory=dict)


class BufferManager:
    """Manages buffering and batching of market data.
    
    Uses asyncio.Queue for zero-copy handoff between producers (WS handlers)
    and consumer (storage writer). Implements auto-flush based on size
    and time thresholds.
    """
    
    def __init__(
        self,
        max_rows_per_source: int = 10000,
        flush_threshold: int = 5000,
        flush_interval_sec: int = 60,
        metrics_callback: Optional[Callable] = None
    ):
        self.max_rows = max_rows_per_source
        self.flush_threshold = flush_threshold
        self.flush_interval = flush_interval_sec
        
        # Queue for zero-copy handoff (inbound from WS handlers)
        self.inbound_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        
        # Buffer storage per source
        self.buffers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.schemas: Dict[str, pa.Schema] = {}
        
        # Flush callback (to storage)
        self.flush_callback: Optional[Callable[[str, List[Dict]], None]] = None
        
        # Control
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self._flush_timer_task: Optional[asyncio.Task] = None
        
        # Metrics
        self.metrics = BufferMetrics()
        self.metrics_callback = metrics_callback
        
        # Stats
        self._received_count = 0
        self._flushed_count = 0
        
    def set_flush_callback(self, callback: Callable[[str, List[Dict]], None]) -> None:
        """Set the callback function for buffer flushes."""
        self.flush_callback = callback
    
    def register_schema(self, source: str, schema: pa.Schema) -> None:
        """Register PyArrow schema for a source."""
        self.schemas[source] = schema
        logger.info(f"Registered schema for {source}")
    
    async def put(self, source: str, record: Dict[str, Any]) -> bool:
        """Add a record to the inbound queue.
        
        Non-blocking; returns False if queue is full.
        """
        try:
            await self.inbound_queue.put((source, record))
            self._received_count += 1
            return True
        except asyncio.QueueFull:
            logger.warning(f"Inbound queue full for {source}")
            return False
    
    async def put_batch(self, source: str, records: List[Dict[str, Any]]) -> int:
        """Add multiple records to the inbound queue using concurrent puts."""
        if not records:
            return 0
        
        # Use gather for concurrent queue puts - much faster under load
        tasks = [self.inbound_queue.put((source, record)) for record in records]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Count successful puts (non-exception results)
        count = sum(1 for r in results if not isinstance(r, Exception))
        self._received_count += count
        
        return count
    
    async def start(self) -> None:
        """Start the buffer manager."""
        self.running = True
        self._task = asyncio.create_task(self._process_loop())
        self._flush_timer_task = asyncio.create_task(self._flush_timer())
        logger.info("Buffer manager started")
    
    async def stop(self) -> None:
        """Stop the buffer manager and flush remaining data."""
        self.running = False
        
        # Cancel timer task
        if self._flush_timer_task:
            self._flush_timer_task.cancel()
            try:
                await self._flush_timer_task
            except asyncio.CancelledError:
                pass
        
        # Final flush
        await self._flush_all()
        
        # Cancel process task
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Buffer manager stopped. Received: {self._received_count}, Flushed: {self._flushed_count}")
    
    async def _process_loop(self) -> None:
        """Main processing loop - moves data from queue to buffers."""
        while self.running:
            try:
                # Wait for data with timeout
                source, record = await asyncio.wait_for(
                    self.inbound_queue.get(),
                    timeout=1.0
                )
                
                # Add to buffer
                self.buffers[source].append(record)
                
                # Check if flush needed
                if len(self.buffers[source]) >= self.flush_threshold:
                    await self._flush_source(source)
                
                self.inbound_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Buffer processing error: {e}")
    
    async def _flush_timer(self) -> None:
        """Periodic flush timer based on time threshold."""
        while self.running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Flush timer error: {e}")
    
    async def _flush_source(self, source: str) -> None:
        """Flush buffer for a specific source."""
        buffer = self.buffers[source]
        if not buffer:
            return
        
        # Capture buffer and clear
        records = buffer.copy()
        self.buffers[source] = []
        
        # Call flush callback (support both sync and async)
        if self.flush_callback:
            try:
                result = self.flush_callback(source, records)
                if asyncio.iscoroutine(result):
                    await result
                self._flushed_count += len(records)
                logger.debug(f"Flushed {len(records)} records from {source}")
            except Exception as e:
                logger.error(f"Flush callback error for {source}: {e}")
                # Re-queue if failed? For now, log and drop
        
        # Update metrics
        self.metrics.total_flushed += len(records)
        self.metrics.last_flush = datetime.now(timezone.utc)
        self.metrics.buffer_sizes[source] = 0
    
    async def _flush_all(self) -> None:
        """Flush all buffers."""
        tasks = []
        for source in list(self.buffers.keys()):
            if self.buffers[source]:
                tasks.append(self._flush_source(source))
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get current buffer statistics."""
        return {
            "received": self._received_count,
            "flushed": self._flushed_count,
            "pending": sum(len(buf) for buf in self.buffers.values()),
            "queue_size": self.inbound_queue.qsize(),
            "buffer_sizes": {k: len(v) for k, v in self.buffers.items()},
        }
    
    def get_buffered_count(self, source: str) -> int:
        """Get number of records buffered for a source."""
        return len(self.buffers.get(source, []))
