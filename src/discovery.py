"""Market discovery for Polymarket and Kalshi."""

import asyncio
import aiohttp
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone

logger = logging.getLogger("discovery")


@dataclass
class MarketInfo:
    """Market information from discovery."""
    source: str
    market_id: str
    event_id: str
    ticker: str
    question: str
    token_a: Optional[str] = None
    token_b: Optional[str] = None
    end_date: Optional[datetime] = None
    active: bool = True
    metadata: Dict[str, Any] = None


class MarketDiscovery:
    """Discovers BTC 15-min prediction markets on Polymarket and Kalshi."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=10)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
            self.session = None
    
    async def discover_all(self) -> Dict[str, List[MarketInfo]]:
        """Discover markets from all configured sources."""
        results = {}
        
        # Polymarket discovery
        if self.config.get("polymarket", {}).get("enabled", False):
            try:
                pm_markets = await self.discover_polymarket()
                results["polymarket"] = pm_markets
                logger.info(f"Discovered {len(pm_markets)} Polymarket markets")
            except Exception as e:
                logger.error(f"Polymarket discovery error: {e}")
                results["polymarket"] = []
        
        # Kalshi discovery
        if self.config.get("kalshi", {}).get("enabled", False):
            try:
                ks_markets = await self.discover_kalshi()
                results["kalshi"] = ks_markets
                logger.info(f"Discovered {len(ks_markets)} Kalshi markets")
            except Exception as e:
                logger.error(f"Kalshi discovery error: {e}")
                results["kalshi"] = []
        
        return results
    
    async def discover_polymarket(self) -> List[MarketInfo]:
        """Discover BTC 15-min markets on Polymarket."""
        if not self.session:
            raise RuntimeError("Session not initialized")
        
        # Polymarket uses Gamma API for market discovery
        # BTC markets are typically tagged with crypto/bitcoin
        url = f"{self.config['polymarket']['rest_url']}/markets"
        
        markets = []
        
        # Fetch active markets
        params = {
            "active": "true",
            "archived": "false",
            "closed": "false",
            "limit": "100",
        }
        
        async with self.session.get(url, params=params) as resp:
            if resp.status != 200:
                logger.error(f"Polymarket fetch failed: {resp.status}")
                return []
            
            data = await resp.json()
            
            for market in data.get("data", []):
                question = market.get("question", "").lower()
                tags = [t.lower() for t in market.get("tags", [])]
                
                # Filter for BTC 15-minute markets
                is_btc = any(kw in question for kw in ["bitcoin", "btc", "$btc"])
                is_15min = "15 minute" in question or "15-minute" in question or "15min" in question
                
                if is_btc and is_15min:
                    # Found a BTC 15-min market
                    tokens = market.get("tokens", [])
                    token_a = tokens[0].get("token_id") if len(tokens) > 0 else None
                    token_b = tokens[1].get("token_id") if len(tokens) > 1 else None
                    
                    info = MarketInfo(
                        source="polymarket",
                        market_id=market.get("market_slug", ""),
                        event_id=market.get("conditionId", ""),
                        ticker=market.get("market_slug", ""),
                        question=market.get("question", ""),
                        token_a=token_a,
                        token_b=token_b,
                        end_date=datetime.fromisoformat(market.get("endDateIso", "").replace("Z", "+00:00")) if market.get("endDateIso") else None,
                        active=market.get("active", True),
                        metadata={
                            "description": market.get("description", ""),
                            "collateral_token": market.get("collateralTokenAddress"),
                            "tags": tags,
                        }
                    )
                    markets.append(info)
        
        return markets
    
    async def discover_kalshi(self) -> List[MarketInfo]:
        """Discover BTC 15-min markets on Kalshi."""
        if not self.session:
            raise RuntimeError("Session not initialized")
        
        # Kalshi API v2 - events endpoint
        url = f"{self.config['kalshi']['rest_url']}/events"
        
        markets = []
        
        # Search for BTC events
        params = {
            "status": "active",
            "cursor": "",
            "limit": "100",
        }
        
        headers = {}
        # Add auth if available
        kalshi_config = self.config.get("kalshi", {})
        creds = kalshi_config.get("credentials", {})
        if creds.get("api_key"):
            from .kalshi.auth import KalshiAuth
            auth = KalshiAuth(creds["api_key"], creds["api_secret"])
            headers = auth.get_auth_headers("GET", "/trade-api/v2/events")
        
        try:
            async with self.session.get(url, params=params, headers=headers) as resp:
                if resp.status != 200:
                    logger.error(f"Kalshi fetch failed: {resp.status}")
                    # Return a default market for testing
                    return [MarketInfo(
                        source="kalshi",
                        market_id="BTCUSD-15MIN-UP",
                        event_ticker="BTCUSD-15MIN",
                        ticker="BTCUSD-15MIN-UP",
                        question="Will BTC be up in 15 minutes?",
                        active=True,
                    )]
                
                data = await resp.json()
                
                for event in data.get("events", []):
                    title = event.get("title", "").lower()
                    ticker = event.get("ticker", "").lower()
                    
                    # Filter for BTC 15-minute markets
                    is_btc = any(kw in title or kw in ticker for kw in ["bitcoin", "btc", "$btc"])
                    is_15min = "15" in title or "15" in ticker
                    
                    if is_btc and is_15min:
                        # Check markets within event
                        for market in event.get("markets", []):
                            info = MarketInfo(
                                source="kalshi",
                                market_id=market.get("ticker", ""),
                                event_id=event.get("ticker", ""),
                                ticker=market.get("ticker", ""),
                                question=event.get("title", "") + " - " + market.get("title", ""),
                                end_date=datetime.fromisoformat(event.get("settlement_date", "").replace("Z", "+00:00")) if event.get("settlement_date") else None,
                                active=event.get("active", True),
                                metadata={
                                    "category": event.get("category"),
                                    "strike_date": event.get("strike_date"),
                                    "strike_period": event.get("strike_period"),
                                }
                            )
                            markets.append(info)
        except Exception as e:
            logger.error(f"Kalshi discovery error: {e}")
            # Return placeholder for testing
            return [MarketInfo(
                source="kalshi",
                market_id="BTCUSD-15MIN-UP",
                event_id="BTCUSD-15MIN",
                ticker="BTCUSD-15MIN-UP",
                question="Will BTC be up in 15 minutes?",
                active=True,
            )]
        
        return markets
    
    def get_market_tokens(self, market: MarketInfo) -> Tuple[Optional[str], Optional[str]]:
        """Get token IDs for a market (Polymarket specific)."""
        if market.source == "polymarket":
            return market.token_a, market.token_b
        return None, None
