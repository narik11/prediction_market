"""Kalshi API authentication using HMAC-SHA256."""

import hmac
import hashlib
import base64
from datetime import datetime, timezone
from typing import Dict, Optional


class KalshiAuth:
    """HMAC authentication for Kalshi API."""
    
    def __init__(self, api_key: str, api_secret: str, email: Optional[str] = None):
        self.api_key = api_key
        self.api_secret = api_secret.encode('utf-8')  # Secret needed as bytes
        self.email = email
    
    def generate_signature(self, timestamp: str, method: str = "GET", path: str = "/") -> str:
        """Generate HMAC-SHA256 signature for Kalshi API.
        
        Args:
            timestamp: ISO8601 timestamp string
            method: HTTP method (GET, POST, etc.)
            path: API path
            
        Returns:
            Base64 encoded signature
        """
        message = f"{timestamp}{method.upper()}{path}"
        signature = hmac.new(
            self.api_secret,
            message.encode('utf-8'),
            hashlib.sha256
        ).digest()
        return base64.b64encode(signature).decode('utf-8')
    
    def get_auth_headers(self, method: str = "GET", path: str = "/") -> Dict[str, str]:
        """Get authentication headers for API requests.
        
        Returns:
            Dictionary with required headers
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        
        headers = {
            "KALSHI-ACCESS-KEY": self.api_key,
            "KALSHI-ACCESS-SIGNATURE": self.generate_signature(timestamp, method, path),
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
        }
        
        if self.email:
            headers["KALSHI-EMAIL"] = self.email
        
        return headers
    
    def get_ws_auth_message(self) -> Dict:
        """Get authentication message for WebSocket connection."""
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        method = "GET"
        path = "/trade-api/ws/v2"
        signature = self.generate_signature(timestamp, method, path)
        
        return {
            "type": "auth",
            "access_key": self.api_key,
            "access_signature": signature,
            "access_timestamp": timestamp,
        }
    
    @classmethod
    def from_config(cls, config: Dict) -> Optional["KalshiAuth"]:
        """Create auth instance from config dict."""
        creds = config.get("kalshi", {}).get("credentials", {})
        
        api_key = creds.get("api_key", "")
        api_secret = creds.get("api_secret", "")
        
        if not api_key or not api_secret:
            return None
        
        return cls(
            api_key=api_key,
            api_secret=api_secret,
            email=creds.get("email")
        )
