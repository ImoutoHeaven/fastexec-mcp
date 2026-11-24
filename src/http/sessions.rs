//! A count bound for rmcp's retained HTTP legacy sessions. Modern requests do not use it.

use futures_core::Stream;
use rmcp::model::{ClientJsonRpcMessage, ServerJsonRpcMessage};
use rmcp::transport::streamable_http_server::session::local::{
    LocalSessionManager, LocalSessionManagerError, SessionConfig,
};
use rmcp::transport::streamable_http_server::session::{
    ServerSseMessage, SessionId, SessionManager,
};
use tokio::sync::Mutex;

pub(super) struct Sessions {
    inner: LocalSessionManager,
    // Serialize checks and insertion. Removal never needs this lock.
    admission: Mutex<()>,
    limit: usize,
}

impl Sessions {
    pub(super) fn new(session_config: SessionConfig, limit: usize) -> Self {
        let mut inner = LocalSessionManager::default();
        inner.session_config = session_config;
        Self {
            inner,
            admission: Mutex::new(()),
            limit,
        }
    }
}

#[derive(Debug)]
pub(super) enum SessionError {
    Full,
    Local(LocalSessionManagerError),
}

impl std::fmt::Display for SessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => f.write_str("too many retained HTTP sessions"),
            Self::Local(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Local(error) => Some(error),
            Self::Full => None,
        }
    }
}

impl SessionManager for Sessions {
    type Error = SessionError;
    type Transport = <LocalSessionManager as SessionManager>::Transport;

    async fn create_session(&self) -> Result<(SessionId, Self::Transport), Self::Error> {
        let _admission = self.admission.lock().await;
        if self.inner.sessions.read().await.len() >= self.limit {
            return Err(SessionError::Full);
        }
        self.inner
            .create_session()
            .await
            .map_err(SessionError::Local)
    }

    async fn initialize_session(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<ServerJsonRpcMessage, Self::Error> {
        self.inner
            .initialize_session(id, message)
            .await
            .map_err(SessionError::Local)
    }

    async fn has_session(&self, id: &SessionId) -> Result<bool, Self::Error> {
        self.inner
            .has_session(id)
            .await
            .map_err(SessionError::Local)
    }

    async fn close_session(&self, id: &SessionId) -> Result<(), Self::Error> {
        self.inner
            .close_session(id)
            .await
            .map_err(SessionError::Local)
    }

    async fn create_stream(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner
            .create_stream(id, message)
            .await
            .map_err(SessionError::Local)
    }

    async fn accept_message(
        &self,
        id: &SessionId,
        message: ClientJsonRpcMessage,
    ) -> Result<(), Self::Error> {
        self.inner
            .accept_message(id, message)
            .await
            .map_err(SessionError::Local)
    }

    async fn create_standalone_stream(
        &self,
        id: &SessionId,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner
            .create_standalone_stream(id)
            .await
            .map_err(SessionError::Local)
    }

    async fn resume(
        &self,
        id: &SessionId,
        last_event_id: String,
    ) -> Result<impl Stream<Item = ServerSseMessage> + Send + Sync + 'static, Self::Error> {
        self.inner
            .resume(id, last_event_id)
            .await
            .map_err(SessionError::Local)
    }

    // No external SessionStore or EventStore is configured. The trait defaults
    // intentionally do not let restoration create a session outside admission.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn creation_cap_is_atomic_and_explicit_close_frees_the_slot() {
        let sessions = Sessions::new(SessionConfig::default(), 1);
        let (first, second) = tokio::join!(sessions.create_session(), sessions.create_session());
        let (id, transport) = match (first, second) {
            (Ok(created), Err(SessionError::Full)) | (Err(SessionError::Full), Ok(created)) => {
                created
            }
            _ => panic!("exactly one slot should be granted"),
        };
        assert!(sessions.has_session(&id).await.unwrap());
        drop(transport);
        sessions.close_session(&id).await.unwrap();
        assert!(!sessions.has_session(&id).await.unwrap());
        let (new_id, _transport) = sessions.create_session().await.unwrap();
        assert_ne!(new_id, id);
    }

    #[tokio::test(start_paused = true)]
    async fn modern_requests_bypass_the_legacy_cap_and_idle_sessions_release_it() {
        use bytes::Bytes;
        use http::{Request, StatusCode};
        use http_body_util::{BodyExt, Full};
        use rmcp::{
            ServerHandler,
            transport::streamable_http_server::{
                StreamableHttpServerConfig, StreamableHttpService,
            },
        };
        use std::{sync::Arc, time::Duration};
        use tower_service::Service as _;

        struct Handler;
        impl ServerHandler for Handler {}

        fn post() -> http::request::Builder {
            Request::post("/mcp")
                .header("host", "localhost")
                .header("accept", "application/json, text/event-stream")
                .header("content-type", "application/json")
        }
        const INIT: &str = r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#;
        const DISCOVER: &str = r#"{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}"#;
        let mut session_config = SessionConfig::default();
        session_config.keep_alive = Some(Duration::from_secs(10));
        let manager = Arc::new(Sessions::new(session_config, 1));
        let mut config = StreamableHttpServerConfig::default();
        config.legacy_session_mode = true;
        config.json_response = true;
        config.sse_retry = None;
        let mut service = StreamableHttpService::new(|| Ok(Handler), manager.clone(), config);

        let initialize = || {
            post()
                .body(Full::new(Bytes::from_static(INIT.as_bytes())))
                .unwrap()
        };
        let first = service.call(initialize()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert!(first.headers().contains_key("mcp-session-id"));
        let refused = service.call(initialize()).await.unwrap();
        assert_eq!(refused.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let modern = post()
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "server/discover")
            .body(Full::new(Bytes::from_static(DISCOVER.as_bytes())))
            .unwrap();
        let modern = service.call(modern).await.unwrap();
        assert_eq!(modern.status(), StatusCode::OK);
        assert!(!modern.headers().contains_key("mcp-session-id"));
        let body = modern.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("supportedVersions"));

        // The actual SDK worker's idle path must remove it from the retained map.
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(manager.inner.sessions.read().await.is_empty());
        let next = service.call(initialize()).await.unwrap();
        assert_eq!(next.status(), StatusCode::OK);
    }
}
