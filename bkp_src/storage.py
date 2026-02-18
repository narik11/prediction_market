"""PyArrow-based Parquet storage with daily rotation and true append support via fastparquet."""

import os
import asyncio
import pyarrow as pa
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger("storage")

# Fastparquet import with fallback
try:
    from fastparquet import write as fp_write
    FASTPARQUET_AVAILABLE = True
except ImportError:
    FASTPARQUET_AVAILABLE = False
    logger.warning("fastparquet not available, falling back to pyarrow (no append support)")


class PartitionedParquetWriter:
    """Daily-rotating Parquet writer with fastparquet true append support."""
    
    def __init__(
        self,
        base_dir: str,
        source: str,
        schema: pa.Schema,
        compression: str = "zstd",
        row_group_size: int = 10000,
    ):
        self.base_dir = Path(base_dir)
        self.source = source
        self.schema = schema
        self.compression = compression
        self.row_group_size = row_group_size
        
        # Ensure directory exists
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        # Locks for thread safety (asyncio-safe patterns)
        self._lock = asyncio.Lock()
        
        # Deduplication: track already-written timestamps
        self._written_timestamps: set = set()
        self._load_existing_timestamps()
        
    def _get_file_for_date(self, date: datetime) -> Path:
        """Get the filepath for a given date."""
        date_str = date.strftime("%Y-%m-%d")
        return self.base_dir / f"{self.source}_{date_str}.parquet"
    
    def _get_current_date_utc(self) -> datetime:
        """Get current date in UTC."""
        return datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    
    def _load_existing_timestamps(self) -> None:
        """Load existing timestamps from today's Parquet file for deduplication."""
        try:
            current_date = self._get_current_date_utc()
            file_path = self._get_file_for_date(current_date)
            
            if not file_path.exists():
                logger.info(f"No existing file for today, starting fresh: {file_path.name}")
                return
            
            # Read existing file and extract timestamps
            import pandas as pd
            df = pd.read_parquet(file_path, columns=['timestamp'])
            
            # Convert to string set for deduplication
            self._written_timestamps = set(df['timestamp'].astype(str).tolist())
            
            logger.info(f"Loaded {len(self._written_timestamps)} existing timestamps from {file_path.name}")
            
        except Exception as e:
            logger.warning(f"Could not load existing timestamps: {e}")
            self._written_timestamps = set()
    
    def _filter_duplicates(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Filter out records with timestamps already written.
        
        Handles both already-written timestamps and duplicates within current batch.
        Keeps the last occurrence of each timestamp+market combination.
        """
        if records is None:
            return []
        
        if not records:
            return records
        
        # First, deduplicate within batch while preserving order (keep last)
        seen_indices = {}  # key -> index mapping
        
        for i, record in enumerate(records):
            ts = record.get('timestamp')
            market = record.get('market_question', '')
            
            if ts is None:
                continue
            
            ts_str = str(ts) if not isinstance(ts, str) else ts
            key = f"{ts_str}|{str(market)}"
            seen_indices[key] = i
        
        # Build deduplicated list (only keep last occurrence of each key)
        unique_records = []
        seen_in_batch = set()
        
        for i, record in enumerate(records):
            ts = record.get('timestamp')
            market = record.get('market_question', '')
            
            if ts is None:
                unique_records.append(record)
                continue
            
            ts_str = str(ts) if not isinstance(ts, str) else ts
            key = f"{ts_str}|{str(market)}"
            
            # Only keep if this is the last occurrence AND not already written
            if seen_indices.get(key) == i:
                if ts_str not in self._written_timestamps:
                    unique_records.append(record)
                    self._written_timestamps.add(ts_str)
                    seen_in_batch.add(key)
                else:
                    logger.debug(f"Skipping already-written timestamp: {ts_str}")
        
        skipped = len(records) - len(unique_records)
        if skipped > 0:
            logger.warning(f"Skipped {skipped} duplicate records")
        
        return unique_records
    
    def _records_to_pandas(self, records: List[Dict[str, Any]]) -> 'pd.DataFrame':
        """Convert records to pandas DataFrame with proper schema."""
        import pandas as pd
        import numpy as np
        
        # Validate records is a valid list/iterable
        if records is None:
            logger.warning("Records is None, returning empty DataFrame")
            return pd.DataFrame({field.name: [] for field in self.schema})
        
        # Single-pass: filter and build columns simultaneously (more efficient)
        columns = {field.name: [] for field in self.schema}
        valid_count = 0
        
        for record in records:
            # Skip invalid records inline
            if record is None or not isinstance(record, dict):
                continue
            
            valid_count += 1
            for field_name in columns:
                value = record.get(field_name)
                
                # Handle None values with appropriate defaults
                if value is None:
                    field = self.schema.field(field_name)
                    if pa.types.is_integer(field.type):
                        value = 0
                    elif pa.types.is_floating(field.type):
                        value = 0.0
                    elif pa.types.is_boolean(field.type):
                        value = False
                    elif pa.types.is_string(field.type):
                        value = ""
                    elif pa.types.is_timestamp(field.type):
                        value = pd.NaT
                
                columns[field_name].append(value)
        
        if valid_count == 0:
            logger.debug("No valid records to process, returning empty DataFrame")
            return pd.DataFrame({field.name: [] for field in self.schema})
        
        # Create DataFrame in single operation
        df = pd.DataFrame(columns)
        
        # Apply dtypes and conversions in single pass
        for field in self.schema:
            name = field.name
            if name not in df.columns:
                continue
                
            if pa.types.is_string(field.type):
                # Convert to plain Python strings, not Arrow-backed
                df[name] = df[name].fillna('').astype(str).astype('object')
            elif pa.types.is_timestamp(field.type):
                df[name] = pd.to_datetime(df[name], utc=True, errors='coerce')
            elif pa.types.is_integer(field.type):
                df[name] = df[name].fillna(0).astype('int64')
            elif pa.types.is_floating(field.type):
                df[name] = df[name].fillna(0.0).astype('float64')
        
        return df
    
    async def write_records(self, records: List[Dict[str, Any]]) -> int:
        """Write records to Parquet file using fastparquet append.
        
        Returns number of records written.
        """
        if records is None:
            logger.debug("write_records called with None, skipping")
            return 0
        
        if not records:
            return 0
        
        if not FASTPARQUET_AVAILABLE:
            logger.error("fastparquet not available, cannot write records")
            return 0
        
        async with self._lock:
            try:
                # Deduplication: filter out already-written timestamps
                records = self._filter_duplicates(records)
                
                if not records:
                    logger.debug("All records were duplicates, nothing to write")
                    return 0
                
                # Convert to pandas DataFrame with proper types
                df = self._records_to_pandas(records)
                
                # Validate DataFrame is valid and not empty
                if df is None or df.empty:
                    logger.debug("DataFrame is empty after conversion, nothing to write")
                    return 0
                
                # Check for any None/NaN columns that might cause issues
                for col in df.columns:
                    if df[col].isna().all():
                        logger.warning(f"Column '{col}' is entirely null, filling with default values")
                        field = self.schema.field(col)
                        if pa.types.is_integer(field.type):
                            df[col] = df[col].fillna(0).astype('int64')
                        elif pa.types.is_floating(field.type):
                            df[col] = df[col].fillna(0.0).astype('float64')
                        elif pa.types.is_string(field.type):
                            df[col] = df[col].fillna('').astype(str).astype('object')
                
                # Determine file path for today
                current_date = self._get_current_date_utc()
                file_path = self._get_file_for_date(current_date)
                
                # Write using fastparquet
                if file_path.exists():
                    # Append to existing file
                    try:
                        fp_write(
                            filename=str(file_path),
                            data=df,
                            compression=self.compression,
                            append=True,
                            write_index=False,
                        )
                        logger.debug(f"Appended {len(records)} records to {file_path}")
                    except Exception as e:
                        logger.error(f"Append failed, creating new file: {e}")
                        # Fallback: create new file with timestamp
                        timestamp = datetime.now(timezone.utc).strftime("%H%M%S")
                        file_path = self.base_dir / f"{self.source}_{current_date.strftime('%Y-%m-%d')}_{timestamp}.parquet"
                        fp_write(
                            filename=str(file_path),
                            data=df,
                            compression=self.compression,
                            write_index=False,
                        )
                        logger.info(f"Created new Parquet file (append failed): {file_path}")
                else:
                    # Create new file
                    fp_write(
                        filename=str(file_path),
                        data=df,
                        compression=self.compression,
                        write_index=False,
                    )
                    logger.info(f"Created new Parquet file: {file_path}")
                
                return len(records)
                
            except Exception as e:
                logger.error(f"Error writing records: {e}", exc_info=True)
                return 0
    
    async def close(self) -> None:
        """Close the writer - no-op for fastparquet (writes are immediate)."""
        logger.info(f"Storage writer closed for {self.source}")
    
    def get_current_file_size(self) -> int:
        """Get size of current file in bytes."""
        current_date = self._get_current_date_utc()
        file_path = self._get_file_for_date(current_date)
        if file_path.exists():
            return file_path.stat().st_size
        return 0


class StorageManager:
    """Manages multiple Parquet writers for different data sources."""
    
    def __init__(self, data_dir: str, compression: str = "zstd", row_group_size: int = 10000):
        self.data_dir = data_dir
        self.compression = compression
        self.row_group_size = row_group_size
        
        # Writers by source
        self.writers: Dict[str, PartitionedParquetWriter] = {}
        self.schemas: Dict[str, pa.Schema] = {}
        
        # Stats
        self._records_written: Dict[str, int] = {}
        
    def register_schema(self, source: str, schema: pa.Schema) -> PartitionedParquetWriter:
        """Register a schema and create writer for a source."""
        self.schemas[source] = schema
        
        writer = PartitionedParquetWriter(
            base_dir=self.data_dir,
            source=source,
            schema=schema,
            compression=self.compression,
            row_group_size=self.row_group_size,
        )
        
        self.writers[source] = writer
        self._records_written[source] = 0
        
        logger.info(f"Registered storage for {source}")
        return writer
    
    async def write_batch(self, source: str, records: List[Dict[str, Any]]) -> int:
        """Write a batch of records for a source."""
        if source not in self.writers:
            logger.warning(f"No writer registered for {source}")
            return 0
        
        count = await self.writers[source].write_records(records)
        self._records_written[source] += count
        
        return count
    
    async def close_all(self) -> None:
        """Close all writers."""
        await asyncio.gather(
            *[writer.close() for writer in self.writers.values()],
            return_exceptions=True
        )
        logger.info("All storage writers closed")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        current_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        
        return {
            "sources": list(self.writers.keys()),
            "records_written": self._records_written.copy(),
            "files": {
                source: str(writer._get_file_for_date(current_date))
                for source, writer in self.writers.items()
            },
        }
