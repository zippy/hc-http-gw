# Step 9.6: Remote Signal Forwarding - Design Document

## Overview

This document captures findings from exploring the kitsune2 API to understand how the gateway can receive `send_remote_signal` messages on behalf of browser extension agents.

## Problem Statement

Step 9.5 implemented signal forwarding for `emit_signal` (local signals), but `send_remote_signal` travels through the kitsune2 p2p network. Browser agents are "zero-arc" - they don't have cells in the conductor and their private keys live in the browser extension.

For `send_remote_signal` to reach browser agents, the gateway must:
1. Run its own kitsune2 instance
2. Register browser agents with the bootstrap server
3. Receive `RemoteSignalEvt` messages via kitsune2
4. Forward signals to browser via WebSocket

## Kitsune2 API Summary

### Creating a K2 Instance

```rust
use kitsune2::default_builder;

// Create builder with default factories
let builder = default_builder().with_default_config()?;

// Build the kitsune instance
let k2: DynKitsune = builder.build().await?;

// Register the handler (must be done before using spaces)
k2.register_handler(my_handler).await?;

// Get or create a space for a DNA
let space: DynSpace = k2.space(space_id, None).await?;

// Join a local agent to the space
space.local_agent_join(local_agent).await?;
```

### Key Traits

#### `KitsuneHandler` (top-level handler)
```rust
trait KitsuneHandler: Send + Sync + Debug {
    /// Called when a new space is needed
    fn create_space(
        &self,
        space_id: SpaceId,
        config_override: Option<&Config>,
    ) -> BoxFut<'_, K2Result<DynSpaceHandler>>;

    // Also: new_listening_address, peer_disconnect, preflight_*
}
```

#### `SpaceHandler` (per-space handler)
```rust
trait SpaceHandler: Send + Sync + Debug {
    /// Called when a notification arrives from a peer
    fn recv_notify(
        &self,
        from_peer: Url,
        space_id: SpaceId,
        data: bytes::Bytes,
    ) -> K2Result<()>;
}
```

#### `LocalAgent` (agent that can sign)
```rust
trait LocalAgent: Signer + Send + Sync + Debug {
    fn agent(&self) -> &AgentId;
    fn register_cb(&self, cb: Arc<dyn Fn() + Send + Sync>);
    fn invoke_cb(&self);
    fn get_cur_storage_arc(&self) -> DhtArc;
    fn set_cur_storage_arc(&self, arc: DhtArc);
    fn get_tgt_storage_arc(&self) -> DhtArc;
    fn set_tgt_storage_arc_hint(&self, arc: DhtArc);
}

trait Signer {
    fn sign<'a>(
        &'a self,
        agent_info: &AgentInfo,
        message: &[u8],
    ) -> BoxFut<'a, K2Result<bytes::Bytes>>;
}
```

### Wire Message Format

Messages are encoded with msgpack (`rmp_serde`). The `RemoteSignalEvt` variant:

```rust
// From holochain_p2p::types::wire
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", content = "content")]
enum WireMessage {
    // ... other variants ...
    RemoteSignalEvt {
        to_agent: AgentPubKey,
        zome_call_params_serialized: ExternIO,
        signature: Signature,
    },
    // ...
}

// Decoding
let messages = WireMessage::decode_batch(&data)?;
for msg in messages {
    if let WireMessage::RemoteSignalEvt { to_agent, zome_call_params_serialized, signature } = msg {
        // Handle the signal
    }
}
```

### Signal Flow in Conductor

```
recv_notify(from_peer, space_id, data)
    │
    ├── WireMessage::decode_batch(&data)
    │
    └── for msg in messages:
            match msg {
                RemoteSignalEvt { to_agent, zome_call_params_serialized, signature } => {
                    // Fire-and-forget: call handle_call_remote
                    evt_handler.handle_call_remote(
                        dna_hash,
                        to_agent,
                        zome_call_params_serialized,
                        signature,
                    ).await;
                }
            }
```

## Gateway Implementation Strategy

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              GATEWAY                                      │
│                                                                          │
│  ┌──────────────┐    ┌──────────────────┐    ┌────────────────────┐    │
│  │              │    │                  │    │                    │    │
│  │   Kitsune2   │───►│  KitsuneProxy    │───►│  AgentProxyManager │    │
│  │   Instance   │    │  (SpaceHandler)  │    │                    │    │
│  │              │    │                  │    │                    │    │
│  └──────────────┘    └──────────────────┘    └─────────┬──────────┘    │
│         │                                              │               │
│         │ local_agent_join                             │ WebSocket     │
│         ▼                                              ▼               │
│  ┌──────────────┐                             ┌────────────────────┐    │
│  │              │                             │                    │    │
│  │ ProxyAgent   │                             │    /ws endpoint    │    │
│  │ (LocalAgent) │                             │                    │    │
│  │              │                             │                    │    │
│  └──────────────┘                             └────────────────────┘    │
│                                                        │               │
└────────────────────────────────────────────────────────│───────────────┘
                                                         │
                                                         ▼
                                                 Browser Extension
```

### Key Components

#### 1. `KitsuneProxy` - Top-level handler

```rust
struct KitsuneProxy {
    agent_proxy: AgentProxyManager,
}

impl KitsuneHandler for KitsuneProxy {
    fn create_space(
        &self,
        space_id: SpaceId,
        config_override: Option<&Config>,
    ) -> BoxFut<'_, K2Result<DynSpaceHandler>> {
        Box::pin(async move {
            Ok(Arc::new(ProxySpaceHandler {
                space_id,
                agent_proxy: self.agent_proxy.clone(),
            }) as DynSpaceHandler)
        })
    }
}
```

#### 2. `ProxySpaceHandler` - Per-space signal receiver

```rust
struct ProxySpaceHandler {
    space_id: SpaceId,
    agent_proxy: AgentProxyManager,
}

impl SpaceHandler for ProxySpaceHandler {
    fn recv_notify(
        &self,
        from_peer: Url,
        space_id: SpaceId,
        data: bytes::Bytes,
    ) -> K2Result<()> {
        // Decode wire messages
        let messages = WireMessage::decode_batch(&data)?;

        for msg in messages {
            if let WireMessage::RemoteSignalEvt {
                to_agent,
                zome_call_params_serialized,
                signature
            } = msg {
                // Check if agent is registered
                let dna_hash = space_id_to_dna_hash(&space_id);
                let agent_b64 = base64::encode(to_agent.get_raw_39());
                let dna_b64 = base64::encode(dna_hash.get_raw_39());

                // Forward to WebSocket
                let signal_msg = ServerMessage::Signal {
                    dna_hash: dna_b64,
                    from_agent: agent_b64.clone(),
                    zome_name: "recv_remote_signal".to_string(),
                    signal: base64::encode(zome_call_params_serialized.0),
                };

                self.agent_proxy.send_signal(&dna_b64, &agent_b64, signal_msg).await;
            }
        }

        Ok(())
    }
}
```

#### 3. `ProxyAgent` - Browser agent proxy

This is the tricky part - we need to implement `LocalAgent` which requires `Signer`.

**Option A: Async signing via WebSocket**
```rust
struct ProxyAgent {
    agent_id: AgentId,
    ws_sender: mpsc::Sender<SigningRequest>,
}

impl Signer for ProxyAgent {
    fn sign(&self, agent_info: &AgentInfo, message: &[u8]) -> BoxFut<'_, K2Result<Bytes>> {
        Box::pin(async move {
            // Send signing request to browser
            let (tx, rx) = oneshot::channel();
            self.ws_sender.send(SigningRequest { message, response: tx }).await?;
            // Wait for signature from browser
            rx.await?
        })
    }
}
```

**Option B: Pre-signed agent info**
- Browser signs agent info when registering
- Gateway stores pre-signed info and uses it for bootstrap registration
- Simpler but less flexible

## Open Questions

### 1. Signing for AgentInfoSigned

When registering an agent with the bootstrap server, kitsune2 needs to create an `AgentInfoSigned`. This requires the agent's signature.

**Options:**
- Browser sends pre-signed agent info during registration
- Gateway requests signature from browser on-demand (adds latency)
- Gateway uses a "proxy signature" scheme (would need protocol changes)

### 2. Bootstrap Server URL

The gateway needs to know which bootstrap server to use. This could be:
- Configured in gateway settings
- Provided by browser during registration
- Same as the conductor's bootstrap server

### 3. Space Configuration

Should the gateway create spaces on-demand (when first agent registers for a DNA) or pre-configure them?

### 4. Connection Lifecycle

When a browser disconnects:
- Unregister all its agents from kitsune2 spaces
- Publish tombstone agent infos to bootstrap
- Clean up resources

### 5. Multiple DNAs

Each DNA is a separate "space" in kitsune2. The gateway needs to:
- Track which spaces have registered agents
- Create/join spaces as needed
- Handle space cleanup when no agents remain

## Dependencies Required

**Version Compatibility (for holochain 0.6.0 tag):**

| Crate | Version | Notes |
|-------|---------|-------|
| kitsune2 | 0.3.0 | Match holochain 0.6.0 tag's dependency |
| kitsune2_api | 0.3.0 | Match holochain 0.6.0 tag's dependency |
| kitsune2_core | 0.3.0 | Match holochain 0.6.0 tag's dependency |
| holochain_p2p | 0.6.0 | For WireMessage types |

**Add to hc-http-gw-fork/Cargo.toml:**
```toml
[dependencies]
kitsune2 = { version = "0.3.0", default-features = false, features = ["transport-tx5-datachannel-vendored"] }
kitsune2_api = "0.3.0"
holochain_p2p = "0.6.0"  # For WireMessage decoding
```

**Note:** The local holochain checkout is at the 0.6.0 tag which uses kitsune2 0.3.0.

## Next Steps

1. **Spike Phase 1**: Create minimal `KitsuneProxy` that receives `recv_notify` callbacks
2. **Spike Phase 2**: Decode `RemoteSignalEvt` and forward to `AgentProxyManager`
3. **Spike Phase 3**: Implement `ProxyAgent` with pre-signed agent info
4. **Test**: Verify signal flows from SweetConductor to gateway to WebSocket

## Files to Create

| File | Purpose |
|------|---------|
| `src/kitsune_proxy.rs` | KitsuneProxy, ProxySpaceHandler implementations |
| `src/proxy_agent.rs` | ProxyAgent implementation |
| `tests/remote_signal_spike.rs` | Integration test |

## Summary of Findings

The kitsune2 API is well-designed for this use case:

1. **SpaceHandler::recv_notify** is the entry point for receiving all network messages
2. **WireMessage::RemoteSignalEvt** is the message type for remote signals
3. **LocalAgent** trait allows joining agents to spaces (requires signing capability)
4. **kitsune2::default_builder()** provides a ready-to-use builder

**The main challenges are:**

1. **Signing**: Browser agents can't sign locally - need to delegate to browser via WebSocket
2. **Dependencies**: Version mismatch between gateway (0.6.0) and current holochain (0.7.0)
3. **Integration**: Need to wire kitsune2 into the gateway's startup/shutdown lifecycle

**Recommended approach:**
1. Update gateway to use local holochain paths (enables 0.7.0 compatibility)
2. Implement pre-signed agent info pattern (browser signs during registration)
3. Create KitsuneProxy that receives and forwards signals

This is a significant piece of work that goes beyond a simple spike - it requires:
- Updating dependency management
- Implementing new traits (KitsuneHandler, SpaceHandler, LocalAgent)
- Modifying gateway startup to create kitsune2 instance
- Adding WebSocket protocol messages for signing requests

---

## Spike Implementation (2026-01-01)

### What Was Built

Created `src/kitsune_proxy.rs` with:

1. **KitsuneProxy** - Top-level `KitsuneHandler` implementation
   - Creates `ProxySpaceHandler` for each space (DNA)
   - Logs new listening addresses and peer disconnections

2. **ProxySpaceHandler** - Per-space `SpaceHandler` implementation
   - Implements `recv_notify` to receive kitsune2 messages
   - Decodes `WireMessage::RemoteSignalEvt` using `WireMessage::decode_batch`
   - Logs received signals (forwarding to AgentProxyManager is TODO)

3. **KitsuneProxyBuilder** - Builder pattern for gateway kitsune2 setup
   - Configures bootstrap server URL
   - Configures signal server URL (tx5 transport)
   - Uses `kitsune2::default_builder()` internally

### Dependencies Added

```toml
holochain_p2p = "0.6.0"
kitsune2 = { version = "0.3.0", default-features = false, features = ["datachannel-vendored"] }
kitsune2_api = "0.3.0"
kitsune2_core = "0.3.0"
kitsune2_transport_tx5 = "0.3.0"
bytes = "1"
rmp-serde = "1"
```

### Flake Update

Added clang/llvm to nix devShell for datachannel-vendored compilation:
```nix
pkgs.llvmPackages.clang
pkgs.llvmPackages.libclang
```

### Tests Added

4 unit tests in `kitsune_proxy::tests`:
- `test_kitsune_proxy_creation` - Verifies handler creation
- `test_space_handler_creation` - Verifies space handler creation
- `test_decode_remote_signal_evt` - Verifies WireMessage encoding/decoding round-trip
- `test_space_handler_recv_notify` - Verifies recv_notify processes RemoteSignalEvt

### Key Findings from Spike

1. **Wire Message Encoding**: Must use `WireMessage::encode_batch` (uses `rmp_serde::encode::write_named`) for proper msgpack encoding that `decode_batch` understands.

2. **Type Conversions**:
   - `SpaceId::from(Bytes::from(vec![...]))` - not directly from Vec
   - `Signature::from([u8; 64])` - requires fixed-size array
   - `Url::from_str("...")` - not `Url::parse`

3. **Kitsune2 0.3.0 API** confirmed stable and sufficient for gateway use case.

### Next Steps for Full Implementation

1. ~~**Wire forwarding to AgentProxyManager**: The spike logs signals but doesn't forward yet~~ DONE
2. **LocalAgent implementation**: Need ProxyAgent that delegates signing to browser
3. **Space lifecycle**: Create/join spaces when browser agents register
4. **Bootstrap integration**: Register proxy agents with bootstrap server
5. **Test with real conductor**: Integration test with SweetConductor sending signals

---

## Signal Forwarding Implementation (2026-01-01)

### Changes Made

Added signal forwarding from `recv_notify` to `AgentProxyManager`:

1. **Conversion utilities** (in `kitsune_proxy.rs`):
   - `space_id_to_dna_b64()` - Convert SpaceId bytes to base64 DnaHash
   - `agent_to_b64()` - Convert AgentPubKey to base64
   - `signal_to_b64()` - Convert ExternIO payload to base64

2. **Updated `handle_wire_message()`**:
   - Extracts `to_agent` and `zome_call_params_serialized` from `RemoteSignalEvt`
   - Creates `ServerMessage::Signal` with:
     - `dna_hash`: base64-encoded SpaceId
     - `from_agent`: "remote" (sender agent not available in wire message)
     - `zome_name`: "recv_remote_signal" (marker for remote signals)
     - `signal`: base64-encoded payload
   - Uses `tokio::spawn` to call async `agent_proxy.send_signal()`

### Tests Added

2 new async tests:
- `test_signal_forwarding_to_registered_agent` - Verifies signals are forwarded to registered agents
- `test_signal_not_forwarded_to_unregistered_agent` - Verifies no crash when agent not registered

### Current Test Count

6 tests in `kitsune_proxy::tests`:
- `test_kitsune_proxy_creation`
- `test_space_handler_creation`
- `test_decode_remote_signal_evt`
- `test_space_handler_recv_notify`
- `test_signal_forwarding_to_registered_agent`
- `test_signal_not_forwarded_to_unregistered_agent`

### Remaining Work

The signal forwarding path is complete:
```
recv_notify(RemoteSignalEvt) → decode → AgentProxyManager.send_signal() → WebSocket
```

But the gateway kitsune2 instance is not yet:
1. Started in gateway startup
2. Connected to the same bootstrap/signal servers as the conductor
3. Joining spaces for registered browser agents

These require implementing ProxyAgent (LocalAgent trait) for agent registration.

---

## ProxyAgent Implementation (2026-01-01)

### What Was Built

Created `src/proxy_agent.rs` with:

1. **ProxyAgent** - Implements `LocalAgent` trait for browser agents
   - Holds the agent's public key (`AgentId`)
   - Implements `Signer` trait (returns error - signing delegated to browser)
   - Zero-arc storage arcs (browser agents don't store DHT data)
   - Callback registration for kitsune2 state changes

### Key Design Decisions

1. **Signing returns error**: Since private keys are in the browser, the gateway's
   ProxyAgent cannot sign locally. The `sign()` method returns an error. For agent
   registration with bootstrap, we'll use pre-signed AgentInfo (browser signs during
   WebSocket registration).

2. **Zero-arc agents**: Browser agents have `DhtArc::Empty` for both current and
   target storage arcs. They don't store or serve DHT data.

### Tests Added

6 unit tests in `proxy_agent::tests`:
- `test_proxy_agent_creation`
- `test_proxy_agent_from_base64`
- `test_proxy_agent_storage_arcs`
- `test_proxy_agent_callback`
- `test_proxy_agent_sign_returns_error`
- `test_proxy_agent_debug`

---

## Space Lifecycle Implementation (2026-01-01)

### What Was Built

Added `GatewayKitsune` to `src/kitsune_proxy.rs`:

1. **GatewayKitsune** - Manages kitsune2 spaces and agent lifecycle
   - Wraps `DynKitsune` instance
   - Tracks active spaces by DNA hash
   - Tracks registered agents by (dna_b64, agent_b64)

2. **Key methods**:
   - `agent_join(dna_b64, agent_pubkey)` - Join agent to space
   - `agent_leave(dna_b64, agent_pubkey)` - Leave agent from space
   - `shutdown()` - Leave all agents (publishes tombstones)
   - `is_agent_joined()`, `agent_count()`, `space_count()` - Query state

3. **Space lifecycle**:
   - Spaces created on-demand when first agent joins
   - Spaces removed when last agent leaves
   - Agents tracked with Arc<ProxyAgent> for potential future use

### Current Test Count

12 tests total:
- 6 in `kitsune_proxy::tests`
- 6 in `proxy_agent::tests`

### Remaining Work

1. ~~ProxyAgent (LocalAgent trait)~~ DONE
2. ~~Space lifecycle management~~ DONE
3. ~~Start gateway kitsune2 instance in service startup~~ DONE
4. **Integration test with SweetConductor**

---

## Gateway Kitsune2 Integration (2026-01-01)

### What Was Built

1. **Configuration** (`src/config.rs`):
   - Added `Kitsune2Config` with `enabled`, `bootstrap_url`, `signal_url` fields
   - Added `kitsune2` field to `Configuration`

2. **AppState** (`src/service.rs`):
   - Added optional `gateway_kitsune: Option<GatewayKitsune>` field
   - Updated Debug impl to show `has_gateway_kitsune`

3. **Router** (`src/router.rs`):
   - Added `hc_http_gateway_router_full` that accepts all optional features
   - Modified `hc_http_gateway_router_with_auth` to call through to `_full`

4. **WebSocket Handler** (`src/routes/websocket.rs`):
   - On `Register`: calls `gateway_kitsune.agent_join()` if kitsune2 enabled
   - On `Unregister`: calls `gateway_kitsune.agent_leave()` if kitsune2 enabled
   - On disconnect: leaves all registered agents from kitsune2 spaces

### Usage Example

```rust
// Create kitsune2 instance if configured
let gateway_kitsune = if config.kitsune2.enabled {
    let proxy = KitsuneProxy::new(agent_proxy.clone());
    let kitsune = KitsuneProxyBuilder::new(proxy)
        .with_bootstrap_url(config.kitsune2.bootstrap_url.as_ref().unwrap())
        .with_signal_url(config.kitsune2.signal_url.as_ref().unwrap())
        .build()
        .await?;
    Some(GatewayKitsune::new(kitsune))
} else {
    None
};

// Create router with all features
let router = hc_http_gateway_router_full(
    config,
    admin_call,
    app_call,
    authenticator,
    Some(agent_proxy),
    gateway_kitsune,
);
```

### Test Count

112 library tests passing:
- 6 kitsune_proxy tests
- 6 proxy_agent tests
- 8 websocket handler integration tests
- 92 other gateway tests
