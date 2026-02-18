"""Kalshi data collection module."""

try:
    from .client import KalshiClient
    from .schema import KalshiSchema
except ImportError:
    from client import KalshiClient
    from schema import KalshiSchema

__all__ = ["KalshiClient", "KalshiSchema"]
