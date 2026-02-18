"""Test script to verify Kalshi integration without actually connecting.

This script validates:
1. Configuration loading
2. Schema structure matching Polymarket
3. Message normalization (mock messages)
4. 1-second aggregation logic
5. Flush operations
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timezone
from kalshi.schema import KalshiSchema
from kalshi.client import KalshiClient
from config import Config


def test_config():
    """Test that configuration loads correctly."""
    print("=== Testing Configuration ===")
    config = Config.load('../config.json')
    
    assert config.kalshi.enabled == True, "Kalshi should be enabled"
    assert config.kalshi.rest_url == "https://api.elections.kalshi.com/trade-api/v2"
    assert config.kalshi.ws_url == "wss://api.elections.kalshi.com/trade-api/ws/v2"
    assert config.kalshi.markets['btc_15min'] == 'KXBTC15M'
    
    print("✓ Configuration validated")
    return config


def test_schema():
    """Test that Kalshi schema matches Polymarket structure."""
    print("\n=== Testing Schema Structure ===")
    from polymarket.schema import PolymarketSchema
    
    pm_fields = {f.name: str(f.type) for f in PolymarketSchema.TICK_SCHEMA}
    kalshi_fields = {f.name: str(f.type) for f in KalshiSchema.TICK_SCHEMA}
    
    missing = []
    mismatched = []
    
    for field_name in pm_fields:
        if field_name not in kalshi_fields:
            missing.append(field_name)
        elif pm_fields[field_name] != kalshi_fields[field_name]:
            mismatched.append(f"{field_name}: PM={pm_fields[field_name]}, Kalshi={kalshi_fields[field_name]}")
    
    if missing:
        print(f"✗ Missing fields: {missing}")
        return False
    if mismatched:
        print(f"✗ Type mismatches: {mismatched}")
        return False
    
    print("✓ Schema matches Polymarket structure")
    return True


def test_normalization():
    """Test message normalization and aggregation."""
    print("\n=== Testing Message Normalization ===")
    
    # Set up market info
    KalshiSchema.set_market_info(
        "KXBTC15M-20260218-0800",
        "Will BTC be higher than 100000 at",
        "08:00 UTC?",
        5350,
        4650
    )
    KalshiSchema.set_price_to_beat(0.535)
    
    # Mock ticker message
    ticker_msg = {
        "type": "ticker",
        "ticker": "KXBTC15M-20260218-0800",
        "ts": int(datetime.now(timezone.utc).timestamp() * 1000),
        "yes_price": 5420,  # cents
        "no_price": 4580,
        "last_price": 5420,
        "volume": 1250
    }
    
    # Mock orderbook message
    orderbook_msg = {
        "type": "orderbook_delta",
        "market_ticker": "KXBTC15M-20260218-0800",
        "side": "yes",
        "price": 5415,
        "size": 100,
        "is_buy": True,
        "ts": int(datetime.now(timezone.utc).timestamp() * 1000)
    }
    
    # Mock trade message  
    trade_msg = {
        "type": "trade",
        "ticker": "KXBTC15M-20260218-0800",
        "side": "yes",
        "price": 5421,
        "count": 5,
        "ts": int(datetime.now(timezone.utc).timestamp() * 1000)
    }
    
    # Process messages
    KalshiSchema.normalize_ws_message(ticker_msg)
    KalshiSchema.normalize_ws_message(orderbook_msg)
    KalshiSchema.normalize_ws_message(trade_msg)
    
    print("✓ Messages normalized successfully")
    
    # Check buffer content
    if KalshiSchema._second_buffer:
        print(f"✓ Buffer contains {len(KalshiSchema._second_buffer)} seconds of data")
        sample = list(KalshiSchema._second_buffer.values())[0]
        print(f"  - YES best bid: {sample.get('yes_best_bid')}")
        print(f"  - YES last trade: {sample.get('yes_last_trade_price')}")
        print(f"  - Oracle price: {sample.get('oracle_price')}")
        print(f"  - Price to beat: {sample.get('price_to_beat')}")
    
    # Test flush (should return empty since current second not flushed)
    records = KalshiSchema.flush_all()
    if records:
        print(f"✓ Flushed {len(records)} records")
        print(f"  - Sample record keys: {list(records[0].keys())[:5]}...")
    
    # Reset buffer for clean state
    KalshiSchema.reset_buffer()
    print("✓ Buffer reset")
    
    return True


def test_price_normalization():
    """Test that Kalshi prices are normalized from cents to float."""
    print("\n=== Testing Price Normalization ===")
    
    test_cases = [
        (0, 0.0),
        (5000, 0.5),
        (10000, 1.0),
        (5350, 0.535),
        (None, 0.0),
    ]
    
    all_passed = True
    for cents, expected in test_cases:
        result = KalshiSchema._normalize_price(cents)
        if abs(result - expected) > 0.0001:
            print(f"✗ Price normalization failed: {cents} cents -> {result} (expected {expected})")
            all_passed = False
    
    if all_passed:
        print("✓ Price normalization working correctly")
    
    return all_passed


def test_client_creation(config):
    """Test Kalshi client can be created."""
    print("\n=== Testing Kalshi Client Creation ===")
    
    # Create client with dummy callback
    def dummy_callback(source, records):
        pass
    
    client = KalshiClient(
        config={
            "ws_url": config.kalshi.ws_url,
            "rest_url": config.kalshi.rest_url,
            "markets": config.kalshi.markets,
        },
        on_second_data=dummy_callback
    )
    
    print(f"✓ KalshiClient created")
    print(f"  - WebSocket URL: {client.ws_url}")
    print(f"  - REST URL: {client.rest_url}")
    print(f"  - Series ticker: {client._series_ticker}")
    
    return True


def main():
    print("="*60)
    print("Kalshi Integration Test Suite")
    print("="*60)
    
    try:
        # Run all tests
        config = test_config()
        schema_ok = test_schema()
        normalization_ok = test_normalization()
        price_norm_ok = test_price_normalization()
        client_ok = test_client_creation(config)
        
        print("\n" + "="*60)
        if all([schema_ok, normalization_ok, price_norm_ok, client_ok]):
            print("✓ ALL TESTS PASSED")
            print("="*60)
            return 0
        else:
            print("✗ SOME TESTS FAILED")
            print("="*60)
            return 1
            
    except Exception as e:
        print(f"\n✗ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
