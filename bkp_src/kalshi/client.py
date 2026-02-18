"""
Kalshi API Client with secure credential handling.

FIX: API credentials are now loaded from environment variables
with fallback to config file for development.
"""

import os
import json
import asyncio
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class KalshiCredentials:
    """Kalshi API credentials loaded from environment or config."""
    api_key: str = ""
    api_secret: str = ""
    
    def __post_init__(self):
        """Load from environment variables, fallback to config values."""
        # Priority: Environment variables > Config > Empty string
        env_key = os.environ.get("KALSHI_API_KEY", "")
        env_secret = os.environ.get("KALSHI_API_SECRET", "")
        
        if env_key:
            self.api_key = env_key
            logger.debug("Using KALSHI_API_KEY from environment")
        
        if env_secret:
            self.api_secret = env_secret
            logger.debug("Using KALSHI_API_SECRET from environment")
        
        # Validate that we have credentials
        if not self.api_key:
            logger.warning("KALSHI_API_KEY not set in environment or config")
        if not self.api_secret:
            logger.warning("KALSHI_API_SECRET not set in environment or config")
    
    @property
    def is_configured(self) -> bool:
        """Check if both key and secret are configured."""
        return bool(self.api_key and self.api_secret)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KalshiCredentials":
        """Create credentials from dictionary (for config fallback)."""
        return cls(
            api_key=data.get("api_key", ""),
            api_secret=data.get("api_secret", "")
        )


class KalshiClient:
    """
    Kalshi API client with secure credential handling.
    
    SECURITY: Credentials are loaded from environment variables.
    Config file is only used as fallback for development.
    """
    
    DEFAULT_API_URL = "https://api.elections.kalshi.com"
    
    def __init__(
        self,
        credentials: Optional[KalshiCredentials] = None,
        api_url: Optional[str] = None
    ):
        self.credentials = credentials or KalshiCredentials()
        self.api_url = api_url or os.environ.get("KALSHI_API_URL", self.DEFAULT_API_URL)
        self._session: Optional[Any] = None
        self._last_error: Optional[str] = None
        
    async def initialize(self) -> None:
        """Initialize the client with proper credential validation."""
        if not self.credentials.is_configured:
            self._last_error = (
                "Kalshi credentials not configured. "
                "Set KALSHI_API_KEY and KALSHI_API_SECRET environment variables."
            )
            logger.error(self._last_error)
            raise RuntimeError(self._last_error)
        
        logger.info("Kalshi client initialized successfully")
        
    async def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests."""
        if not self.credentials.is_configured:
            raise RuntimeError("Credentials not configured")
        
        return {
            "Authorization": f"Bearer {self.credentials.api_key}",
            "X-API-Secret": self.credentials.api_secret,
            "Content-Type": "application/json"
        }
    
    async def get_markets(self) -> Dict[str, Any]:
        """Fetch available markets from Kalshi API."""
        await self.initialize()
        
        headers = await self._get_auth_headers()
        
        try:
            # Placeholder for actual API call
            # In production, use aiohttp or similar
            logger.info(f"Fetching markets from {self.api_url}")
            
            # Simulated response
            return {
                "markets": [],
                "status": "success"
            }
            
        except Exception as e:
            logger.error(f"Failed to fetch markets: {e}")
            raise
    
    async def get_market_data(self, market_id: str) -> Dict[str, Any]:
        """Fetch specific market data."""
        await self.initialize()
        
        headers = await self._get_auth_headers()
        
        logger.debug(f"Fetching data for market: {market_id}")
        
        # Placeholder for actual implementation
        return {
            "market_id": market_id,
            "data": {}
        }
    
    async def close(self) -> None:
        """Clean up resources."""
        if self._session:
            # Close session if using aiohttp
            pass
        logger.info("Kalshi client closed")


def load_kalshi_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load Kalshi configuration with secure credential handling.
    
    WARNING: This only loads non-sensitive config.
    Credentials MUST come from environment variables in production.
    """
    config = {
        "api_url": os.environ.get("KALSHI_API_URL", KalshiClient.DEFAULT_API_URL),
        "log_level": os.environ.get("LOG_LEVEL", "INFO"),
    }
    
    # Only load credentials from file in development mode
    # In production, these will be overridden by env vars in KalshiCredentials
    if config_path and os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                file_config = json.load(f)
                
            # Only use file credentials if env vars not set
            if not os.environ.get("KALSHI_API_KEY"):
                config["api_key"] = file_config.get("api_key", "")
                if config["api_key"]:
                    logger.warning(
                        "Loading KALSHI_API_KEY from config file. "
                        "Use environment variables in production!"
                    )
            
            if not os.environ.get("KALSHI_API_SECRET"):
                config["api_secret"] = file_config.get("api_secret", "")
                if config["api_secret"]:
                    logger.warning(
                        "Loading KALSHI_API_SECRET from config file. "
                        "Use environment variables in production!"
                    )
                        
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
    
    return config


# Convenience function for creating client from config
async def create_kalshi_client(
    config_path: Optional[str] = None
) -> KalshiClient:
    """Create a Kalshi client with proper configuration."""
    
    # Load any file-based config (for development fallback only)
    config = load_kalshi_config(config_path)
    
    # Create credentials (env vars take priority)
    credentials = KalshiCredentials(
        api_key=config.get("api_key", ""),
        api_secret=config.get("api_secret", "")
    )
    
    client = KalshiClient(
        credentials=credentials,
        api_url=config.get("api_url")
    )
    
    await client.initialize()
    return client
