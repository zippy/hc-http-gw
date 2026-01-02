//! WebSocket handler for browser extension connections.
//!
//! This module provides a WebSocket endpoint for browser extensions to:
//! - Authenticate using session tokens
//! - Register agents for specific DNAs
//! - Receive signals forwarded from the Holochain network

use crate::agent_proxy::WsSender;
use crate::service::AppState;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::Response,
};
use base64::Engine;
use futures::{SinkExt, StreamExt};
use holochain_types::prelude::AgentPubKey;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::interval;

/// Messages sent from browser to gateway.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Authenticate the connection with a session token.
    Auth {
        /// Session token from /auth/verify.
        session_token: String,
    },
    /// Register an agent for a specific DNA to receive signals.
    Register {
        /// DNA hash (base64 encoded).
        dna_hash: String,
        /// Agent public key (base64 encoded).
        agent_pubkey: String,
    },
    /// Unregister an agent from a DNA.
    Unregister {
        /// DNA hash (base64 encoded).
        dna_hash: String,
        /// Agent public key (base64 encoded).
        agent_pubkey: String,
    },
    /// Ping for heartbeat.
    Ping,
}

/// Messages sent from gateway to browser.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Authentication succeeded.
    AuthOk,
    /// Authentication failed.
    AuthError {
        /// Error message.
        message: String,
    },
    /// Agent registration confirmed.
    Registered {
        /// DNA hash.
        dna_hash: String,
        /// Agent public key.
        agent_pubkey: String,
    },
    /// Agent unregistration confirmed.
    Unregistered {
        /// DNA hash.
        dna_hash: String,
        /// Agent public key.
        agent_pubkey: String,
    },
    /// Signal forwarded from the network.
    Signal {
        /// DNA hash.
        dna_hash: String,
        /// Sender agent.
        from_agent: String,
        /// Zome that emitted the signal.
        zome_name: String,
        /// Signal payload (base64 encoded msgpack).
        signal: String,
    },
    /// Pong response to ping.
    Pong,
    /// Error message.
    Error {
        /// Error description.
        message: String,
    },
}

/// Connection state for a WebSocket client.
#[derive(Debug)]
struct ConnectionState {
    /// Whether the client has authenticated.
    authenticated: bool,
    /// The authenticated agent (if any).
    agent: Option<AgentPubKey>,
    /// Last activity timestamp.
    last_activity: Instant,
    /// Registered agent-DNA pairs.
    registrations: Vec<(String, String)>, // (dna_hash, agent_pubkey)
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            authenticated: false,
            agent: None,
            last_activity: Instant::now(),
            registrations: Vec::new(),
        }
    }
}

/// WebSocket upgrade handler.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// Handle an upgraded WebSocket connection.
async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut conn_state = ConnectionState::default();

    // Get config values
    let heartbeat_interval = state.configuration.websocket.heartbeat_interval;
    let heartbeat_timeout = state.configuration.websocket.heartbeat_timeout;
    let idle_timeout = state.configuration.websocket.idle_timeout;

    // Channel for sending messages to the client
    let (tx, mut rx) = mpsc::channel::<ServerMessage>(32);

    // Spawn task to forward messages from channel to WebSocket
    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let json = match serde_json::to_string(&msg) {
                Ok(j) => j,
                Err(e) => {
                    tracing::error!("Failed to serialize message: {}", e);
                    continue;
                }
            };
            if sender.send(Message::Text(json.into())).await.is_err() {
                break;
            }
        }
    });

    // Create heartbeat interval timer
    let mut heartbeat = interval(heartbeat_interval);
    let mut last_pong = Instant::now();

    loop {
        tokio::select! {
            // Handle incoming messages
            msg = receiver.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        conn_state.last_activity = Instant::now();

                        match serde_json::from_str::<ClientMessage>(&text) {
                            Ok(client_msg) => {
                                let response = handle_client_message(
                                    client_msg,
                                    &mut conn_state,
                                    &state,
                                    &tx,
                                ).await;

                                if let Some(resp) = response {
                                    if tx.send(resp).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                let _ = tx.send(ServerMessage::Error {
                                    message: format!("Invalid message format: {}", e),
                                }).await;
                            }
                        }
                    }
                    Some(Ok(Message::Pong(_))) => {
                        last_pong = Instant::now();
                        conn_state.last_activity = Instant::now();
                    }
                    Some(Ok(Message::Close(_))) => {
                        tracing::debug!("Client closed connection");
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::warn!("WebSocket error: {}", e);
                        break;
                    }
                    None => {
                        tracing::debug!("WebSocket stream ended");
                        break;
                    }
                    _ => {}
                }
            }

            // Heartbeat tick
            _ = heartbeat.tick() => {
                // Check if we received a pong recently
                if last_pong.elapsed() > heartbeat_interval + heartbeat_timeout {
                    tracing::debug!("Client heartbeat timeout");
                    break;
                }

                // Check idle timeout
                if conn_state.last_activity.elapsed() > idle_timeout {
                    tracing::debug!("Client idle timeout");
                    break;
                }

                // Send ping (handled at protocol level by axum)
                // For application-level heartbeat, we could send a ping message
            }
        }
    }

    // Cleanup: unregister all agents from the proxy manager
    state.agent_proxy.unregister_all(&tx).await;

    // If kitsune2 is configured, leave all agents from their spaces
    if let Some(ref gateway_kitsune) = state.gateway_kitsune {
        for (dna_hash, agent_pubkey) in &conn_state.registrations {
            if let Ok(agent_bytes) = base64::engine::general_purpose::STANDARD.decode(agent_pubkey)
            {
                gateway_kitsune.agent_leave(dna_hash, agent_bytes).await;
            }
        }
    }

    // Wait for send task to complete
    send_task.abort();
}

/// Handle a client message and return an optional response.
async fn handle_client_message(
    msg: ClientMessage,
    state: &mut ConnectionState,
    app_state: &AppState,
    sender: &WsSender,
) -> Option<ServerMessage> {
    match msg {
        ClientMessage::Auth { session_token } => {
            // Verify the session token
            match &app_state.authenticator {
                Some(auth) => {
                    match auth.verify_session(&session_token).await {
                        Ok(agent) => {
                            state.authenticated = true;
                            state.agent = Some(agent);
                            Some(ServerMessage::AuthOk)
                        }
                        Err(e) => Some(ServerMessage::AuthError {
                            message: format!("Invalid session: {}", e),
                        }),
                    }
                }
                None => {
                    // No authenticator configured - allow unauthenticated connections
                    state.authenticated = true;
                    Some(ServerMessage::AuthOk)
                }
            }
        }

        ClientMessage::Register { dna_hash, agent_pubkey } => {
            if !state.authenticated {
                return Some(ServerMessage::Error {
                    message: "Not authenticated".to_string(),
                });
            }

            // Check if already registered locally
            let key = (dna_hash.clone(), agent_pubkey.clone());
            if !state.registrations.contains(&key) {
                state.registrations.push(key);
            }

            // Register with agent proxy manager to receive signals
            app_state
                .agent_proxy
                .register(dna_hash.clone(), agent_pubkey.clone(), sender.clone())
                .await;

            // If kitsune2 is configured, join the agent to the space
            if let Some(ref gateway_kitsune) = app_state.gateway_kitsune {
                // Decode the agent pubkey from base64 to get raw bytes
                match base64::engine::general_purpose::STANDARD.decode(&agent_pubkey) {
                    Ok(agent_bytes) => {
                        if let Err(e) = gateway_kitsune.agent_join(&dna_hash, agent_bytes).await {
                            tracing::warn!(
                                dna = %dna_hash,
                                agent = %agent_pubkey,
                                error = %e,
                                "Failed to join agent to kitsune2 space"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(
                            agent = %agent_pubkey,
                            error = %e,
                            "Failed to decode agent pubkey for kitsune2 join"
                        );
                    }
                }
            }

            Some(ServerMessage::Registered { dna_hash, agent_pubkey })
        }

        ClientMessage::Unregister { dna_hash, agent_pubkey } => {
            if !state.authenticated {
                return Some(ServerMessage::Error {
                    message: "Not authenticated".to_string(),
                });
            }

            let key = (dna_hash.clone(), agent_pubkey.clone());
            state.registrations.retain(|r| r != &key);

            // Unregister from agent proxy manager
            app_state.agent_proxy.unregister(&dna_hash, &agent_pubkey).await;

            // If kitsune2 is configured, leave the agent from the space
            if let Some(ref gateway_kitsune) = app_state.gateway_kitsune {
                if let Ok(agent_bytes) = base64::engine::general_purpose::STANDARD.decode(&agent_pubkey) {
                    gateway_kitsune.agent_leave(&dna_hash, agent_bytes).await;
                }
            }

            Some(ServerMessage::Unregistered { dna_hash, agent_pubkey })
        }

        ClientMessage::Ping => {
            Some(ServerMessage::Pong)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_message_auth_deserialization() {
        let auth_json = r#"{"type": "auth", "session_token": "abc123"}"#;
        let msg: ClientMessage = serde_json::from_str(auth_json).unwrap();
        assert!(matches!(msg, ClientMessage::Auth { session_token } if session_token == "abc123"));

        // Empty session token
        let auth_json = r#"{"type": "auth", "session_token": ""}"#;
        let msg: ClientMessage = serde_json::from_str(auth_json).unwrap();
        assert!(matches!(msg, ClientMessage::Auth { session_token } if session_token.is_empty()));
    }

    #[test]
    fn test_client_message_register_deserialization() {
        let register_json = r#"{"type": "register", "dna_hash": "dna1", "agent_pubkey": "agent1"}"#;
        let msg: ClientMessage = serde_json::from_str(register_json).unwrap();
        assert!(matches!(msg, ClientMessage::Register { dna_hash, agent_pubkey }
            if dna_hash == "dna1" && agent_pubkey == "agent1"));
    }

    #[test]
    fn test_client_message_unregister_deserialization() {
        let unregister_json = r#"{"type": "unregister", "dna_hash": "dna1", "agent_pubkey": "agent1"}"#;
        let msg: ClientMessage = serde_json::from_str(unregister_json).unwrap();
        assert!(matches!(msg, ClientMessage::Unregister { dna_hash, agent_pubkey }
            if dna_hash == "dna1" && agent_pubkey == "agent1"));
    }

    #[test]
    fn test_client_message_ping_deserialization() {
        let ping_json = r#"{"type": "ping"}"#;
        let msg: ClientMessage = serde_json::from_str(ping_json).unwrap();
        assert!(matches!(msg, ClientMessage::Ping));
    }

    #[test]
    fn test_client_message_invalid_type() {
        let invalid_json = r#"{"type": "invalid_type"}"#;
        let result: Result<ClientMessage, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_client_message_missing_field() {
        // Missing session_token
        let invalid_json = r#"{"type": "auth"}"#;
        let result: Result<ClientMessage, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());

        // Missing dna_hash
        let invalid_json = r#"{"type": "register", "agent_pubkey": "agent1"}"#;
        let result: Result<ClientMessage, _> = serde_json::from_str(invalid_json);
        assert!(result.is_err());
    }

    #[test]
    fn test_server_message_auth_ok() {
        let msg = ServerMessage::AuthOk;
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"auth_ok"}"#);
    }

    #[test]
    fn test_server_message_auth_error() {
        let msg = ServerMessage::AuthError {
            message: "Invalid session".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"auth_error""#));
        assert!(json.contains(r#""message":"Invalid session""#));
    }

    #[test]
    fn test_server_message_registered() {
        let msg = ServerMessage::Registered {
            dna_hash: "dna1".to_string(),
            agent_pubkey: "agent1".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"registered""#));
        assert!(json.contains(r#""dna_hash":"dna1""#));
        assert!(json.contains(r#""agent_pubkey":"agent1""#));
    }

    #[test]
    fn test_server_message_signal() {
        let msg = ServerMessage::Signal {
            dna_hash: "dna1".to_string(),
            from_agent: "agent1".to_string(),
            zome_name: "zome1".to_string(),
            signal: "c2lnbmFsX2RhdGE=".to_string(), // base64 "signal_data"
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"signal""#));
        assert!(json.contains(r#""dna_hash":"dna1""#));
        assert!(json.contains(r#""from_agent":"agent1""#));
        assert!(json.contains(r#""zome_name":"zome1""#));
    }

    #[test]
    fn test_server_message_pong() {
        let msg = ServerMessage::Pong;
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"pong"}"#);
    }

    #[test]
    fn test_server_message_error() {
        let msg = ServerMessage::Error {
            message: "Something went wrong".to_string(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":"error""#));
        assert!(json.contains(r#""message":"Something went wrong""#));
    }

    #[test]
    fn test_connection_state_default() {
        let state = ConnectionState::default();
        assert!(!state.authenticated);
        assert!(state.agent.is_none());
        assert!(state.registrations.is_empty());
    }

    #[test]
    fn test_connection_state_registrations() {
        let mut state = ConnectionState::default();
        state.authenticated = true;

        // Add registration
        let key = ("dna1".to_string(), "agent1".to_string());
        state.registrations.push(key.clone());
        assert_eq!(state.registrations.len(), 1);

        // Duplicate registration check
        if !state.registrations.contains(&key) {
            state.registrations.push(key.clone());
        }
        assert_eq!(state.registrations.len(), 1); // Should still be 1

        // Remove registration
        state.registrations.retain(|r| r != &key);
        assert!(state.registrations.is_empty());
    }

    // Integration tests for handle_client_message with AgentProxyManager
    mod handler_integration {
        use super::*;
        use crate::agent_proxy::AgentProxyManager;
        use crate::config::{AllowedAppIds, Configuration, Kitsune2Config, WebSocketConfig};
        use crate::service::AppState;
        use crate::{MockAdminCall, MockAppCall};
        use std::str::FromStr;
        use std::sync::Arc;

        fn create_test_app_state() -> AppState {
            let admin_call = Arc::new(MockAdminCall::new());
            let app_call = Arc::new(MockAppCall::new());

            let config = Configuration {
                admin_socket_addr: "127.0.0.1:9999".parse().unwrap(),
                payload_limit_bytes: 1024,
                allowed_app_ids: AllowedAppIds::from_str("").unwrap(),
                allowed_fns: Default::default(),
                max_app_connections: 10,
                zome_call_timeout: std::time::Duration::from_secs(10),
                websocket: WebSocketConfig::default(),
                kitsune2: Kitsune2Config::default(),
            };

            AppState {
                configuration: config,
                admin_call,
                app_call,
                app_info_cache: Default::default(),
                authenticator: None,
                agent_proxy: AgentProxyManager::new(),
                gateway_kitsune: None,
            }
        }

        #[tokio::test]
        async fn test_register_adds_to_agent_proxy() {
            let app_state = create_test_app_state();
            let (tx, _rx) = mpsc::channel(32);
            let mut conn_state = ConnectionState::default();
            conn_state.authenticated = true;

            // Register an agent
            let msg = ClientMessage::Register {
                dna_hash: "dna1".to_string(),
                agent_pubkey: "agent1".to_string(),
            };

            let response = handle_client_message(msg, &mut conn_state, &app_state, &tx).await;

            // Should return Registered
            assert!(matches!(response, Some(ServerMessage::Registered { .. })));

            // Should be registered in AgentProxyManager
            assert!(app_state.agent_proxy.is_registered("dna1", "agent1").await);
            assert_eq!(app_state.agent_proxy.registration_count().await, 1);
        }

        #[tokio::test]
        async fn test_unregister_removes_from_agent_proxy() {
            let app_state = create_test_app_state();
            let (tx, _rx) = mpsc::channel(32);
            let mut conn_state = ConnectionState::default();
            conn_state.authenticated = true;

            // First register an agent
            let register_msg = ClientMessage::Register {
                dna_hash: "dna1".to_string(),
                agent_pubkey: "agent1".to_string(),
            };
            handle_client_message(register_msg, &mut conn_state, &app_state, &tx).await;
            assert!(app_state.agent_proxy.is_registered("dna1", "agent1").await);

            // Now unregister
            let unregister_msg = ClientMessage::Unregister {
                dna_hash: "dna1".to_string(),
                agent_pubkey: "agent1".to_string(),
            };
            let response = handle_client_message(unregister_msg, &mut conn_state, &app_state, &tx).await;

            // Should return Unregistered
            assert!(matches!(response, Some(ServerMessage::Unregistered { .. })));

            // Should no longer be registered
            assert!(!app_state.agent_proxy.is_registered("dna1", "agent1").await);
            assert_eq!(app_state.agent_proxy.registration_count().await, 0);
        }

        #[tokio::test]
        async fn test_register_requires_authentication() {
            let app_state = create_test_app_state();
            let (tx, _rx) = mpsc::channel(32);
            let mut conn_state = ConnectionState::default();
            // Not authenticated

            let msg = ClientMessage::Register {
                dna_hash: "dna1".to_string(),
                agent_pubkey: "agent1".to_string(),
            };

            let response = handle_client_message(msg, &mut conn_state, &app_state, &tx).await;

            // Should return error
            assert!(matches!(response, Some(ServerMessage::Error { .. })));

            // Should NOT be registered
            assert!(!app_state.agent_proxy.is_registered("dna1", "agent1").await);
        }

        #[tokio::test]
        async fn test_auth_without_authenticator_succeeds() {
            let app_state = create_test_app_state();
            let (tx, _rx) = mpsc::channel(32);
            let mut conn_state = ConnectionState::default();

            let msg = ClientMessage::Auth {
                session_token: "any_token".to_string(),
            };

            let response = handle_client_message(msg, &mut conn_state, &app_state, &tx).await;

            // Should return AuthOk (no authenticator configured)
            assert!(matches!(response, Some(ServerMessage::AuthOk)));
            assert!(conn_state.authenticated);
        }

        #[tokio::test]
        async fn test_multiple_registrations_same_connection() {
            let app_state = create_test_app_state();
            let (tx, _rx) = mpsc::channel(32);
            let mut conn_state = ConnectionState::default();
            conn_state.authenticated = true;

            // Register first agent
            let msg1 = ClientMessage::Register {
                dna_hash: "dna1".to_string(),
                agent_pubkey: "agent1".to_string(),
            };
            handle_client_message(msg1, &mut conn_state, &app_state, &tx).await;

            // Register second agent on same connection
            let msg2 = ClientMessage::Register {
                dna_hash: "dna2".to_string(),
                agent_pubkey: "agent2".to_string(),
            };
            handle_client_message(msg2, &mut conn_state, &app_state, &tx).await;

            // Both should be registered
            assert!(app_state.agent_proxy.is_registered("dna1", "agent1").await);
            assert!(app_state.agent_proxy.is_registered("dna2", "agent2").await);
            assert_eq!(app_state.agent_proxy.registration_count().await, 2);

            // Local state should track both
            assert_eq!(conn_state.registrations.len(), 2);
        }

        #[tokio::test]
        async fn test_unregister_all_on_disconnect() {
            let app_state = create_test_app_state();
            let (tx, _rx) = mpsc::channel(32);
            let mut conn_state = ConnectionState::default();
            conn_state.authenticated = true;

            // Register multiple agents
            let msg1 = ClientMessage::Register {
                dna_hash: "dna1".to_string(),
                agent_pubkey: "agent1".to_string(),
            };
            handle_client_message(msg1, &mut conn_state, &app_state, &tx).await;

            let msg2 = ClientMessage::Register {
                dna_hash: "dna2".to_string(),
                agent_pubkey: "agent2".to_string(),
            };
            handle_client_message(msg2, &mut conn_state, &app_state, &tx).await;

            assert_eq!(app_state.agent_proxy.registration_count().await, 2);

            // Simulate disconnect cleanup
            app_state.agent_proxy.unregister_all(&tx).await;

            // All should be unregistered
            assert!(!app_state.agent_proxy.is_registered("dna1", "agent1").await);
            assert!(!app_state.agent_proxy.is_registered("dna2", "agent2").await);
            assert_eq!(app_state.agent_proxy.registration_count().await, 0);
        }
    }
}
