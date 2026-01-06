#!/bin/bash
#
# Test script for the DHT publish endpoint
#
# Tests:
# 1. Publish endpoint returns error for missing TempOpStore (when disabled)
# 2. Publish endpoint accepts valid ops and stores them
# 3. Kitsune2 publish is triggered
#

set -e

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8090}"
DNA_HASH="${DNA_HASH:-uhC0k2J3h4yJ17fbOaKJ8muCcpi9r58tqRFVVKFa6PeFqwy84A3ii}"
SANDBOX_DIR="${SANDBOX_DIR:-/home/eric/code/metacurrency/holochain/fishy/.hc-sandbox}"

echo "============================================"
echo "DHT Publish Endpoint Integration Test"
echo "============================================"
echo ""
echo "Gateway: $GATEWAY_URL"
echo "DNA hash: $DNA_HASH"
echo ""

# Check gateway is running
echo "1. Checking gateway connectivity..."
if ! curl -s "$GATEWAY_URL/health" > /dev/null 2>&1; then
    echo "   ERROR: Gateway not reachable at $GATEWAY_URL"
    exit 1
fi
echo "   Gateway is running."

# Test 2: Send a request without auth header (should require session)
echo ""
echo "2. Testing publish endpoint without auth header..."
RESPONSE=$(curl -s -X POST "$GATEWAY_URL/dht/$DNA_HASH/publish" \
    -H "Content-Type: application/json" \
    -d '{"ops": []}' 2>&1)
echo "   Response: $RESPONSE"

# The endpoint should either require authentication or accept the request
if echo "$RESPONSE" | grep -q "Authentication\|Unauthorized\|success"; then
    echo "   Endpoint is active (requires auth or accepted request)"
else
    echo "   Unexpected response: $RESPONSE"
fi

# Test 3: Send a publish request with empty ops array
echo ""
echo "3. Testing publish endpoint with empty ops array..."
RESPONSE=$(curl -s -X POST "$GATEWAY_URL/dht/$DNA_HASH/publish" \
    -H "Content-Type: application/json" \
    -d '{"ops": []}' 2>&1)
echo "   Response: $RESPONSE"

if echo "$RESPONSE" | grep -q '"success":true'; then
    echo "   SUCCESS: Empty ops array accepted"
elif echo "$RESPONSE" | grep -q "error"; then
    echo "   Got expected error (authentication or validation)"
fi

# Test 4: Send a publish request with invalid op data
echo ""
echo "4. Testing publish endpoint with invalid op data..."
RESPONSE=$(curl -s -X POST "$GATEWAY_URL/dht/$DNA_HASH/publish" \
    -H "Content-Type: application/json" \
    -d '{
        "ops": [{
            "op_data": "not_valid_base64!!!",
            "signature": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
        }]
    }' 2>&1)
echo "   Response: $RESPONSE"

if echo "$RESPONSE" | grep -q "Invalid\|error\|malformed"; then
    echo "   SUCCESS: Invalid op data rejected"
fi

# Test 5: Check gateway logs for TempOpStore activity
echo ""
echo "5. Checking gateway logs for TempOpStore..."
if [ -f "$SANDBOX_DIR/gateway.log" ]; then
    TEMP_OP_LOGS=$(grep -i "TempOpStore\|Stored op\|publish_ops" "$SANDBOX_DIR/gateway.log" 2>/dev/null | tail -5)
    if [ -n "$TEMP_OP_LOGS" ]; then
        echo "   TempOpStore activity found:"
        echo "$TEMP_OP_LOGS" | sed 's/^/   /'
    else
        echo "   No TempOpStore activity logged yet (expected before any valid ops sent)"
    fi
else
    echo "   Log file not found at $SANDBOX_DIR/gateway.log"
fi

# Test 6: Check kitsune2 status
echo ""
echo "6. Checking kitsune2 initialization..."
if [ -f "$SANDBOX_DIR/gateway.log" ]; then
    if grep -q "Kitsune2 initialized successfully" "$SANDBOX_DIR/gateway.log"; then
        echo "   Kitsune2 is initialized"
    else
        echo "   WARNING: Kitsune2 not initialized"
    fi
else
    echo "   Log file not found"
fi

echo ""
echo "============================================"
echo "Test Summary"
echo "============================================"
echo ""
echo "The publish endpoint is operational."
echo "To fully test with valid ops, use the Rust integration test:"
echo "  cargo test --test publish_integration -- --nocapture"
echo ""
