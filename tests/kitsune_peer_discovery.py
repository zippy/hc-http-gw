#!/usr/bin/env python3
"""
Integration test for kitsune2 peer discovery.

This test verifies that:
1. A browser agent registered via the gateway's WebSocket
2. Gets broadcast on the kitsune2 network
3. Can be discovered by the conductor

Prerequisites:
- Gateway running on port 8090 with kitsune2 enabled
- Conductor running (from e2e-test-setup.sh)
- DNA installed (fixture1.happ)

Usage:
    python3 tests/kitsune_peer_discovery.py
"""

import asyncio
import json
import subprocess
import sys

try:
    import websockets
except ImportError:
    print("Installing websockets...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets


async def test_peer_discovery():
    """Test that registering an agent via gateway makes it discoverable."""

    gateway_url = "ws://127.0.0.1:8090/ws"

    # Get DNA hash from the sandbox
    try:
        with open("/home/eric/code/metacurrency/holochain/fishy/.hc-sandbox/dna_hash.txt") as f:
            dna_hash = f.read().strip()
    except FileNotFoundError:
        print("ERROR: dna_hash.txt not found. Run e2e-test-setup.sh first.")
        return False

    # Create a test agent key (simulated browser agent)
    # Using a fake agent key for testing
    # Real format: uhCAk + 32 bytes base64 + 4 byte DHT location
    test_agent = "uhCAkTestAgentForKitsuneDiscoveryTestAAAAAA"

    print(f"Testing peer discovery...")
    print(f"  Gateway: {gateway_url}")
    print(f"  DNA: {dna_hash}")
    print(f"  Agent: {test_agent}")

    try:
        async with websockets.connect(gateway_url) as ws:
            # Step 1: Authenticate (no auth required in test mode)
            auth_msg = json.dumps({"type": "auth", "session_token": "test"})
            print(f"\n1. Sending auth: {auth_msg}")
            await ws.send(auth_msg)

            response = await asyncio.wait_for(ws.recv(), timeout=5.0)
            print(f"   Response: {response}")
            resp_data = json.loads(response)
            if resp_data.get("type") != "auth_ok":
                print(f"   ERROR: Expected auth_ok, got {resp_data}")
                return False

            # Step 2: Register agent
            register_msg = json.dumps({
                "type": "register",
                "dna_hash": dna_hash,
                "agent_pubkey": test_agent
            })
            print(f"\n2. Sending register: {register_msg}")
            await ws.send(register_msg)

            response = await asyncio.wait_for(ws.recv(), timeout=5.0)
            print(f"   Response: {response}")
            resp_data = json.loads(response)

            if resp_data.get("type") == "error":
                print(f"   ERROR: Registration failed: {resp_data.get('message')}")
                # This is expected for invalid agent key format
                if "Invalid agent pubkey" in resp_data.get("message", ""):
                    print("   (Expected - test agent key is not a valid HoloHash)")
                return False

            if resp_data.get("type") != "registered":
                print(f"   ERROR: Expected registered, got {resp_data}")
                return False

            print(f"   Agent registered successfully!")

            # Step 3: Wait for agent info to propagate
            print("\n3. Waiting for agent info broadcast (10 seconds)...")
            await asyncio.sleep(10)

            # Step 4: Check conductor's known agents
            print("\n4. Checking conductor's known agents...")
            try:
                admin_port_file = "/home/eric/code/metacurrency/holochain/fishy/.hc-sandbox/admin_port.txt"
                with open(admin_port_file) as f:
                    admin_port = f.read().strip()

                result = subprocess.run(
                    ["hc", "sandbox", "call", f"--running={admin_port}", "list-agents"],
                    capture_output=True,
                    text=True,
                    timeout=30
                )

                print(f"   Conductor agents: {result.stdout.strip()}")

                # Check if our test agent appears
                if test_agent in result.stdout:
                    print(f"\n   SUCCESS: Test agent discovered by conductor!")
                    return True
                else:
                    print(f"\n   PARTIAL: Agent registered but not yet discovered by conductor")
                    print("   (This could be due to WebRTC/STUN configuration issues)")
                    return False

            except subprocess.TimeoutExpired:
                print("   ERROR: hc sandbox call timed out")
                return False
            except FileNotFoundError:
                print("   ERROR: Could not find admin port")
                return False

    except websockets.exceptions.ConnectionRefused:
        print(f"ERROR: Could not connect to gateway at {gateway_url}")
        print("Make sure the gateway is running (e2e-test-setup.sh start)")
        return False
    except asyncio.TimeoutError:
        print("ERROR: Timeout waiting for gateway response")
        return False


async def main():
    print("=" * 60)
    print("Kitsune2 Peer Discovery Integration Test")
    print("=" * 60)

    success = await test_peer_discovery()

    print("\n" + "=" * 60)
    if success:
        print("TEST PASSED")
    else:
        print("TEST FAILED")
    print("=" * 60)

    return 0 if success else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
