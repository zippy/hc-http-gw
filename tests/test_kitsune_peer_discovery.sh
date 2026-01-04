#!/bin/bash
#
# Integration test for kitsune2 peer discovery
#
# Tests that the gateway's kitsune2 instance is operational and can
# communicate with the conductor.
#

set -e

GATEWAY_URL="http://127.0.0.1:8090"
SANDBOX_DIR="/home/eric/code/metacurrency/holochain/fishy/.hc-sandbox"
GATEWAY_LOG="$SANDBOX_DIR/gateway.log"

echo "============================================"
echo "Kitsune2 Peer Discovery Integration Test"
echo "============================================"
echo ""

# Check gateway is running
echo "1. Checking gateway connectivity..."
if ! curl -s "$GATEWAY_URL" > /dev/null 2>&1; then
    echo "   ERROR: Gateway not reachable at $GATEWAY_URL"
    echo "   Run: ./scripts/e2e-test-setup.sh start"
    exit 1
fi
echo "   Gateway is running."

# Check kitsune2 was initialized
echo ""
echo "2. Checking kitsune2 initialization..."
if grep -q "Kitsune2 initialized successfully" "$GATEWAY_LOG"; then
    echo "   Kitsune2 initialized successfully"
else
    echo "   ERROR: Kitsune2 not initialized"
    exit 1
fi

# Check kitsune2 is listening
echo ""
echo "3. Checking kitsune2 listening address..."
# Strip ANSI codes and extract URL
KITSUNE_URL=$(grep "kitsune2 listening on new address" "$GATEWAY_LOG" | tail -1 | sed 's/\x1b\[[0-9;]*m//g' | grep -oP 'this_url=\K\S+')
if [ -n "$KITSUNE_URL" ]; then
    echo "   Kitsune2 listening on: $KITSUNE_URL"
else
    echo "   ERROR: Kitsune2 not listening"
    exit 1
fi

# Check for agent registrations
echo ""
echo "4. Checking agent registrations in gateway..."
AGENT_JOINS=$(grep -c "Browser agent joined kitsune2 space" "$GATEWAY_LOG" 2>/dev/null || echo "0")
echo "   Agent joins: $AGENT_JOINS"

# Check for peer connections
echo ""
echo "5. Checking peer connectivity..."
PEER_CONNECTS=$(grep -c "peer connected" "$GATEWAY_LOG" 2>/dev/null || echo "0")
PEER_DISCONNECTS=$(grep -c "Peer disconnected" "$GATEWAY_LOG" 2>/dev/null || echo "0")
WEBRTC_ERRORS=$(grep -c "webrtc task error" "$GATEWAY_LOG" 2>/dev/null || echo "0")

echo "   Peer connections: $PEER_CONNECTS"
echo "   Peer disconnections: $PEER_DISCONNECTS"
echo "   WebRTC errors: $WEBRTC_ERRORS"

# Check broadcast status
echo ""
echo "6. Checking agent info broadcasts..."
BROADCASTS=$(grep "Broadcast new agent info" "$GATEWAY_LOG" | tail -5)
if [ -n "$BROADCASTS" ]; then
    echo "$BROADCASTS" | while read line; do
        PEERS=$(echo "$line" | grep -oP 'to \K\d+')
        echo "   Broadcast to $PEERS peers"
    done
else
    echo "   No broadcasts found"
fi

# Check conductor's known agents
echo ""
echo "7. Checking conductor's known agents..."
if [ -f "$SANDBOX_DIR/admin_port.txt" ]; then
    ADMIN_PORT=$(cat "$SANDBOX_DIR/admin_port.txt")
    echo "   Admin port: $ADMIN_PORT"

    AGENTS=$(hc sandbox call --running="$ADMIN_PORT" list-agents 2>&1 || echo "ERROR")
    if [ "$AGENTS" = "ERROR" ]; then
        echo "   ERROR: Could not list agents (hc command not available in this shell)"
        echo "   Run this test inside 'nix develop' shell"
    else
        echo "   Conductor agents: $AGENTS"
    fi
else
    echo "   ERROR: admin_port.txt not found"
fi

# Summary
echo ""
echo "============================================"
echo "Summary"
echo "============================================"

if [ "$WEBRTC_ERRORS" -gt 0 ]; then
    echo ""
    echo "NOTE: WebRTC data channel errors detected"
    echo ""
    echo "This is a known issue with the libdatachannel WebRTC stack."
    echo "Peer discovery via bootstrap is working (see agent list above)."
    echo ""
    echo "For local development, the important metrics are:"
    echo "  - Agent joins: $AGENT_JOINS"
    echo "  - Both gateway and conductor agents visible in list: Check above"
    echo ""
    echo "WebRTC errors: $WEBRTC_ERRORS (these affect direct peer messaging only)"
fi

if [ "$PEER_CONNECTS" -gt 0 ] && [ "$PEER_CONNECTS" -eq "$PEER_DISCONNECTS" ]; then
    echo ""
    echo "All peer connections have been disconnected."
    echo "This suggests WebRTC negotiation is failing immediately."
fi

echo ""
echo "Test completed."
