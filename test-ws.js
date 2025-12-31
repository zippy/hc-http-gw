#!/usr/bin/env node
/**
 * Simple WebSocket test client for the gateway.
 *
 * Prerequisites:
 *   npm install ws
 *
 * Usage:
 *   node test-ws.js [gateway-url]
 *
 *   Default gateway URL: ws://localhost:8000/ws
 *
 * This script:
 *   1. Connects to the gateway WebSocket endpoint
 *   2. Sends an auth message (unauthenticated mode)
 *   3. Sends a register message
 *   4. Sends ping/pong heartbeats
 *   5. Logs all messages received
 */

const WebSocket = require('ws');

const gatewayUrl = process.argv[2] || 'ws://localhost:8000/ws';

console.log(`Connecting to ${gatewayUrl}...`);

const ws = new WebSocket(gatewayUrl);

ws.on('open', () => {
  console.log('Connected!');

  // Step 1: Authenticate (no session token since auth may be disabled)
  console.log('Sending auth message...');
  ws.send(JSON.stringify({
    type: 'auth',
    session_token: ''  // Empty token - works if authenticator is not configured
  }));
});

ws.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  console.log('Received:', JSON.stringify(msg, null, 2));

  // Step 2: After auth_ok, register an agent
  if (msg.type === 'auth_ok') {
    console.log('Authenticated! Sending register message...');
    ws.send(JSON.stringify({
      type: 'register',
      dna_hash: 'dGVzdC1kbmEtaGFzaA==',  // "test-dna-hash" base64
      agent_pubkey: 'dGVzdC1hZ2VudC1wdWJrZXk='  // "test-agent-pubkey" base64
    }));
  }

  // Step 3: After registered, test ping
  if (msg.type === 'registered') {
    console.log('Registered! Testing ping...');
    ws.send(JSON.stringify({ type: 'ping' }));
  }

  // Step 4: After pong, test unregister
  if (msg.type === 'pong') {
    console.log('Pong received! Testing unregister...');
    ws.send(JSON.stringify({
      type: 'unregister',
      dna_hash: 'dGVzdC1kbmEtaGFzaA==',
      agent_pubkey: 'dGVzdC1hZ2VudC1wdWJrZXk='
    }));
  }

  // Done
  if (msg.type === 'unregistered') {
    console.log('All tests passed! Closing connection...');
    ws.close();
  }
});

ws.on('error', (err) => {
  console.error('WebSocket error:', err.message);
});

ws.on('close', (code, reason) => {
  console.log(`Connection closed: ${code} ${reason}`);
  process.exit(0);
});

// Timeout after 10 seconds
setTimeout(() => {
  console.error('Timeout - no response within 10 seconds');
  ws.close();
  process.exit(1);
}, 10000);
