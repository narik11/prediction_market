"""Configuration loader for prediction collector."""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class BufferConfig:
    """Buffer configuration."""
    max_rows_per_source: int = 10000
    flush_threshold: int = 5000
    flush_interval_sec: int = 60


@dataclass
class StorageConfig:
    """Storage configuration."""
    rotation_utc_hour: int = 0
    compression: str = "zstd"
    row_group_size: int = 10000


@dataclass
class PolymarketConfig:
    """Polymarket source configuration."""
    enabled: bool = True
    rest_url: str = "https://clob.polymarket.com"
    ws_url: str = "wss://clob.polymarket.com/ws"
    poll_interval_ms: int = 1000
    markets: Dict[str, str] = field(default_factory=lambda: {"btc_15min": "btc-15min-updown"})


@dataclass
class KalshiCredentials:
    """Kalshi API credentials."""
    email: str = ""
    api_key: str = ""
    api_secret: str = ""


@dataclass
class KalshiConfig:
    """Kalshi source configuration."""
    enabled: bool = True
    rest_url: str = "https://api.elections.kalshi.com/trade-api/v2"
    ws_url: str = "wss://api.elections.kalshi.com/trade-api/ws/v2"
    poll_interval_ms: int = 1000
    markets: Dict[str, str] = field(default_factory=lambda: {"btc_15min": "BTCUSD-15MIN-"})
    credentials: KalshiCredentials = field(default_factory=KalshiCredentials)


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class Config:
    """Main application configuration."""
    project_name: str = "prediction_collector"
    data_dir: str = "./data"
    polymarket: PolymarketConfig = field(default_factory=PolymarketConfig)
    kalshi: KalshiConfig = field(default_factory=KalshiConfig)
    buffer: BufferConfig = field(default_factory=BufferConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        """Create Config from dictionary."""
        sources = data.get("sources", {})
        
        # Polymarket config
        pm_data = sources.get("polymarket", {})
        polymarket = PolymarketConfig(
            enabled=pm_data.get("enabled", True),
            rest_url=pm_data.get("rest_url", "https://clob.polymarket.com"),
            ws_url=pm_data.get("ws_url", "wss://clob.polymarket.com/ws"),
            poll_interval_ms=pm_data.get("poll_interval_ms", 1000),
            markets=pm_data.get("markets", {"btc_15min": "btc-15min-updown"}),
        )
        
        # Kalshi config
        ks_data = sources.get("kalshi", {})
        ks_creds = ks_data.get("credentials", {})
        kalshi = KalshiConfig(
            enabled=ks_data.get("enabled", True),
            rest_url=ks_data.get("rest_url", "https://api.elections.kalshi.com/trade-api/v2"),
            ws_url=ks_data.get("ws_url", "wss://api.elections.kalshi.com/trade-api/ws/v2"),
            poll_interval_ms=ks_data.get("poll_interval_ms", 1000),
            markets=ks_data.get("markets", {"btc_15min": "BTCUSD-15MIN-"}),
            credentials=KalshiCredentials(
                email=ks_creds.get("email", ""),
                api_key=ks_creds.get("api_key", ""),
                api_secret=ks_creds.get("api_secret", ""),
            ),
        )
        
        # Buffer config
        buf_data = data.get("buffer", {})
        buffer = BufferConfig(
            max_rows_per_source=buf_data.get("max_rows_per_source", 10000),
            flush_threshold=buf_data.get("flush_threshold", 5000),
            flush_interval_sec=buf_data.get("flush_interval_sec", 60),
        )
        
        # Storage config
        stor_data = data.get("storage", {})
        storage = StorageConfig(
            rotation_utc_hour=stor_data.get("rotation_utc_hour", 0),
            compression=stor_data.get("compression", "zstd"),
            row_group_size=stor_data.get("row_group_size", 10000),
        )
        
        # Logging config
        log_data = data.get("logging", {})
        logging = LoggingConfig(
            level=log_data.get("level", "INFO"),
            format=log_data.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
        )
        
        return cls(
            project_name=data.get("project_name", "prediction_collector"),
            data_dir=data.get("data_dir", "./data"),
            polymarket=polymarket,
            kalshi=kalshi,
            buffer=buffer,
            storage=storage,
            logging=logging,
        )
    
    @classmethod
    def load(cls, path: Optional[str] = None) -> "Config":
        """Load configuration from JSON file."""
        if path is None:
            path = os.getenv("CONFIG_PATH", "./config.json")
        
        config_path = Path(path)
        
        if not config_path.exists():
            # Create default config if file doesn't exist
            default_config = cls()
            default_config.save(path)
            return default_config
        
        with open(config_path, 'r') as f:
            data = json.load(f)
        
        return cls.from_dict(data)
    
    def save(self, path: str = "./config.json") -> None:
        """Save configuration to JSON file."""
        config_path = Path(path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Convert to dict for JSON serialization
        data = {
            "project_name": self.project_name,
            "data_dir": self.data_dir,
            "sources": {
                "polymarket": {
                    "enabled": self.polymarket.enabled,
                    "rest_url": self.polymarket.rest_url,
                    "ws_url": self.polymarket.ws_url,
                    "markets": self.polymarket.markets,
                    "poll_interval_ms": self.polymarket.poll_interval_ms,
                },
                "kalshi": {
                    "enabled": self.kalshi.enabled,
                    "rest_url": self.kalshi.rest_url,
                    "ws_url": self.kalshi.ws_url,
                    "markets": self.kalshi.markets,
                    "poll_interval_ms": self.kalshi.poll_interval_ms,
                    "credentials": {
                        "email": self.kalshi.credentials.email,
                        "api_key": self.kalshi.credentials.api_key,
                        "api_secret": self.kalshi.credentials.api_secret,
                    },
                },
            },
            "buffer": {
                "max_rows_per_source": self.buffer.max_rows_per_source,
                "flush_threshold": self.buffer.flush_threshold,
                "flush_interval_sec": self.buffer.flush_interval_sec,
            },
            "storage": {
                "rotation_utc_hour": self.storage.rotation_utc_hour,
                "compression": self.storage.compression,
                "row_group_size": self.storage.row_group_size,
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
            },
        }
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def ensure_data_dir(self) -> None:
        """Create data directory if it doesn't exist."""
        Path(self.data_dir).mkdir(parents=True, exist_ok=True)
