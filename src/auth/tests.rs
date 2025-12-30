//! Tests for authentication module.

use super::*;
use ed25519_dalek::{SigningKey, Signer};
use holochain_types::prelude::AgentPubKey;
use std::collections::HashSet;
use std::time::Duration;

/// Create a test keypair and return (signing_key, agent_pub_key)
fn create_test_keypair() -> (SigningKey, AgentPubKey) {
    let signing_key = SigningKey::from_bytes(&[1u8; 32]);
    let verifying_key = signing_key.verifying_key();
    let agent_pub_key = AgentPubKey::from_raw_32(verifying_key.as_bytes().to_vec());
    (signing_key, agent_pub_key)
}

mod session_manager_tests {
    use super::*;

    #[test]
    fn test_create_and_verify_session() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let (_, agent) = create_test_keypair();

        let session = manager.create_session(agent.clone());
        assert!(!session.token.is_empty());

        let verified = manager.verify(&session.token).unwrap();
        assert_eq!(verified, agent);
    }

    #[test]
    fn test_invalid_token_fails() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let result = manager.verify("invalid-token");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalidate_session() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let (_, agent) = create_test_keypair();

        let session = manager.create_session(agent);
        assert!(manager.verify(&session.token).is_ok());

        manager.invalidate(&session.token);
        assert!(manager.verify(&session.token).is_err());
    }

    #[test]
    fn test_multiple_sessions_for_same_agent() {
        let manager = SessionManager::new(Duration::from_secs(3600));
        let (_, agent) = create_test_keypair();

        let session1 = manager.create_session(agent.clone());
        let session2 = manager.create_session(agent.clone());

        // Both sessions should be valid
        assert!(manager.verify(&session1.token).is_ok());
        assert!(manager.verify(&session2.token).is_ok());

        // Tokens should be different
        assert_ne!(session1.token, session2.token);
    }
}

mod config_list_authenticator_tests {
    use super::*;

    #[tokio::test]
    async fn test_unauthorized_agent_rejected() {
        let authenticator =
            ConfigListAuthenticator::new(HashSet::new(), Duration::from_secs(3600));

        let (signing_key, agent) = create_test_keypair();
        let nonce = b"test-nonce";
        let signature = signing_key.sign(nonce);

        let request = ParsedAuthRequest {
            agent_pub_key: agent,
            signature: signature.to_bytes().to_vec(),
            nonce: nonce.to_vec(),
        };

        let result = authenticator.authenticate(request).await;
        assert!(matches!(result, Err(AuthError::AgentNotAuthorized(_))));
    }

    #[tokio::test]
    async fn test_authorized_agent_with_valid_signature() {
        let (signing_key, agent) = create_test_keypair();

        let mut allowed = HashSet::new();
        allowed.insert(agent.clone());
        let authenticator = ConfigListAuthenticator::new(allowed, Duration::from_secs(3600));

        let nonce = b"test-nonce";
        let signature = signing_key.sign(nonce);

        let request = ParsedAuthRequest {
            agent_pub_key: agent.clone(),
            signature: signature.to_bytes().to_vec(),
            nonce: nonce.to_vec(),
        };

        let session = authenticator.authenticate(request).await.unwrap();
        assert!(!session.token.is_empty());

        // Verify session works
        let verified = authenticator.verify_session(&session.token).await.unwrap();
        assert_eq!(verified, agent);
    }

    #[tokio::test]
    async fn test_authorized_agent_with_invalid_signature() {
        let (_, agent) = create_test_keypair();

        let mut allowed = HashSet::new();
        allowed.insert(agent.clone());
        let authenticator = ConfigListAuthenticator::new(allowed, Duration::from_secs(3600));

        let request = ParsedAuthRequest {
            agent_pub_key: agent,
            signature: vec![0u8; 64], // Invalid signature
            nonce: b"test-nonce".to_vec(),
        };

        let result = authenticator.authenticate(request).await;
        assert!(matches!(result, Err(AuthError::InvalidSignature)));
    }

    #[tokio::test]
    async fn test_is_agent_authorized() {
        let (_, agent) = create_test_keypair();
        let (_, other_agent) = {
            let key = SigningKey::from_bytes(&[2u8; 32]);
            let vk = key.verifying_key();
            (key, AgentPubKey::from_raw_32(vk.as_bytes().to_vec()))
        };

        let mut allowed = HashSet::new();
        allowed.insert(agent.clone());
        let authenticator = ConfigListAuthenticator::new(allowed, Duration::from_secs(3600));

        assert!(authenticator.is_agent_authorized(&agent).await);
        assert!(!authenticator.is_agent_authorized(&other_agent).await);
    }

    #[tokio::test]
    async fn test_from_base64_list() {
        let (_, agent) = create_test_keypair();
        let agent_str = agent.to_string();

        let authenticator =
            ConfigListAuthenticator::from_base64_list(&[agent_str], Duration::from_secs(3600))
                .unwrap();

        assert!(authenticator.is_agent_authorized(&agent).await);
    }
}

mod auth_challenge_tests {
    use super::*;

    #[test]
    fn test_auth_challenge_fields() {
        let challenge = AuthChallenge {
            nonce: "test-nonce".to_string(),
            expires_at: 12345,
        };

        assert_eq!(challenge.nonce, "test-nonce");
        assert_eq!(challenge.expires_at, 12345);
    }

    #[test]
    fn test_auth_verify_request_fields() {
        let request = AuthVerifyRequest {
            agent_pub_key: "test-key".to_string(),
            signature: "test-sig".to_string(),
            nonce: "test-nonce".to_string(),
        };

        assert_eq!(request.agent_pub_key, "test-key");
        assert_eq!(request.signature, "test-sig");
        assert_eq!(request.nonce, "test-nonce");
    }
}
