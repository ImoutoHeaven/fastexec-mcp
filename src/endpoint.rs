//! The MCP endpoint in `on` mode: one link to the namespace's daemon, and the calls that travel
//! over it.
//!
//! The endpoint is the only thing here that knows the daemon is a process. It identifies this
//! build once, connects on the first call, and from then on multiplexes every call over one
//! connection: each call carries an ID that stays its name across a reconnect, so a caller's
//! `Cancel` still names the right call after the link was replaced.

use crate::ipc::{self, ToDaemon, ToEndpoint};
use crate::runtime;
use crate::service::Render;
use crate::session::LaunchContext;
use crate::transport;
use crate::wait;
use rmcp::model::{CallToolResult, ContentBlock};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::{Mutex, OnceCell, oneshot, watch};
use tokio_util::sync::CancellationToken;

/// How long a call may take to reach the daemon before the link is treated as gone.
const WRITE_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
/// How long `bye` may take, locks and write together.
const BYE_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
/// How long a `Cancel` may take, the writer lock included.
const CANCEL_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// This build's identity and runtime directory, or why they could not be read.
type Identity = Result<(runtime::Identity, runtime::Paths), String>;

/// One endpoint's daemon link, and everything it knows before it has one.
pub struct Endpoint {
    /// Set once, by the task started in [`Endpoint::new`]: the hash reads the whole image.
    identity: Arc<OnceCell<Identity>>,
    /// Tells a waiting call that the identity is settled. The startup task's end closes it, so a
    /// wait never hangs on a task that died.
    ready: watch::Receiver<bool>,
    launch: Arc<LaunchContext>,
    render: Render,
    /// This process's session, fixed for its life: a reconnect keeps the same session name.
    session: String,
    /// Call IDs are endpoint-wide, so one never names two calls, across reconnects included.
    next_call: AtomicU64,
    /// Connections are counted, so the daemon can tell a newer connection from an older one.
    generation: AtomicU64,
    link: Arc<Mutex<Option<Arc<Link>>>>,
}

#[cfg(not(test))]
type Writer = WriteHalf<transport::Client>;
#[cfg(test)]
type Writer = tests::ControlledWriter;

/// A live connection to the daemon, shared by every call in flight.
struct Link {
    epoch: String,
    writer: Mutex<Writer>,
    pending: std::sync::Mutex<HashMap<u64, Pending>>,
    /// Cancelled when the link ends: every call on it is lost, and the reader stops.
    closed: CancellationToken,
}

/// A call waiting for its result.
struct Pending {
    result: oneshot::Sender<CallToolResult>,
    progress: Option<watch::Sender<Option<wait::Progress>>>,
}

impl Endpoint {
    /// Creates the endpoint and starts identifying this build at once: the hash reads the whole
    /// image, so one task owns it from start to finish and callers only wait on the result.
    pub fn new(launch: Arc<LaunchContext>, render: Render) -> std::io::Result<Self> {
        let session = crate::tasks::random_chars(26)?;
        let identity: Arc<OnceCell<Identity>> = Arc::new(OnceCell::new());
        let (announce, ready) = watch::channel(false);
        let stored = Arc::clone(&identity);
        let identifying = Arc::clone(&launch);
        tokio::spawn(async move {
            let outcome =
                match tokio::task::spawn_blocking(move || runtime::identity(&identifying)).await {
                    Ok(Ok(identity)) => match runtime::paths(&identity.namespace) {
                        Ok(paths) => Ok((identity, paths)),
                        Err(error) => Err(format!("cannot prepare the runtime directory: {error}")),
                    },
                    Ok(Err(error)) => Err(format!("cannot identify this fastexec build: {error}")),
                    Err(error) => Err(format!("cannot wait for the build identity: {error}")),
                };
            let _ = stored.set(outcome);
            // Then this task ends, dropping the sender: a later wait sees the channel close.
            let _ = announce.send(true);
        });
        Ok(Self {
            identity,
            ready,
            launch,
            render,
            session,
            next_call: AtomicU64::new(1),
            generation: AtomicU64::new(1),
            link: Arc::new(Mutex::new(None)),
        })
    }

    /// Waits for the identity the startup task settles, and answers with it. The wait holds no
    /// work, so a caller dropped mid-wait strands nothing and repeats nothing.
    async fn identity(&self) -> Result<&(runtime::Identity, runtime::Paths), String> {
        if let Some(outcome) = self.identity.get() {
            return settled(outcome);
        }
        let mut ready = self.ready.clone();
        let _ = ready.changed().await;
        match self.identity.get() {
            Some(outcome) => settled(outcome),
            None => Err("the build identity was not read".to_owned()),
        }
    }

    /// Runs one call on the daemon and answers with its result.
    pub async fn call(
        &self,
        tool: &str,
        arguments: serde_json::Value,
        request_id: String,
        client_name: &str,
        cancel: CancellationToken,
        progress: Option<watch::Sender<Option<wait::Progress>>>,
    ) -> CallToolResult {
        let action = arguments
            .get("action")
            .and_then(|action| action.as_str())
            .unwrap_or("call")
            .to_owned();
        // A call this large is refused here: sending it would only make the daemon refuse it,
        // and the caller would have waited for a connection to hear that.
        match serde_json::to_vec(&arguments) {
            Ok(body) if body.len() > ipc::CALL_LIMIT => {
                return error(format!(
                    "The call is {} bytes; calls are limited to 3 MiB.",
                    body.len()
                ));
            }
            Ok(_) => {}
            Err(problem) => return error(format!("cannot serialize the call: {problem}")),
        }
        let id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let deadline = std::time::Instant::now() + runtime::START_WAIT;
        let connecting = async {
            match self.link(client_name, deadline).await {
                Ok(link) => Ok(link),
                // No call was written before a Welcome, so one reconnect replays nothing.
                Err(_) => self.link(client_name, deadline).await,
            }
        };
        let link = tokio::select! {
            biased;
            _ = cancel.cancelled() => return error("Cancelled before the call was sent."),
            linked = connecting => match linked {
                Ok(link) => link,
                Err(message) => {
                    return error(format!("Cannot reach the fastexec daemon: {message}"));
                }
            }
        };
        let (sender, answer) = oneshot::channel();
        {
            let mut pending = lock(&link.pending);
            pending.insert(
                id,
                Pending {
                    result: sender,
                    progress,
                },
            );
        }
        let frame = ToDaemon::Call {
            id,
            tool: tool.to_owned(),
            request_id: request_id.clone(),
            arguments,
        };
        let write_deadline = tokio::time::Instant::now() + WRITE_WAIT;
        let writer = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                lock(&link.pending).remove(&id);
                return error("Cancelled before the call was sent.");
            }
            _ = link.closed.cancelled() => None,
            writer = tokio::time::timeout_at(write_deadline, link.writer.lock()) => writer.ok(),
        };
        let mut writing_started = false;
        let written = if let Some(mut writer) = writer {
            tokio::select! {
                biased;
                _ = link.closed.cancelled() => None,
                _ = cancel.cancelled() => None,
                written = async {
                    writing_started = true;
                    tokio::time::timeout_at(
                        write_deadline,
                        ipc::write_frame(&mut *writer, &frame),
                    ).await
                } => Some(written),
            }
        } else {
            None
        };
        if !writing_started && cancel.is_cancelled() {
            lock(&link.pending).remove(&id);
            return error("Cancelled before the call was sent.");
        }
        if !matches!(written, Some(Ok(Ok(())))) {
            // Cancellation after writing began may leave a partial frame.
            self.close(&link);
        }
        // Closing drops all pending senders. A complete result sent before EOF wins even
        // if the reader has already closed the link by the time the caller resumes.
        tokio::select! {
            biased;
            answer = answer => match answer {
                Ok(result) => result,
                // The reader dropped the entry, so the link is gone.
                Err(_) => self.lost(&link, tool, &action, id, &request_id),
            },
            _ = cancel.cancelled() => {
                // The wait ends here; the task keeps running on the daemon.
                let sent = tokio::time::timeout(CANCEL_WAIT, async {
                    let mut writer = link.writer.lock().await;
                    ipc::write_frame(&mut *writer, &ToDaemon::Cancel { id }).await
                })
                .await;
                lock(&link.pending).remove(&id);
                if !matches!(sent, Ok(Ok(()))) {
                    // A Cancel that did not land may have left half a frame, so the stream is
                    // not used again. The whole branch stays inside its one second.
                    self.close(&link);
                    return self.lost(&link, tool, &action, id, &request_id);
                }
                error("Cancelled; the task keeps running.")
            }
        }
    }

    /// Says Bye when there is a link and waits for the daemon to read it, so this session is gone
    /// before the process exits. A failed write is the daemon being gone already, which is the
    /// same outcome.
    pub async fn bye(&self) {
        let _ = tokio::time::timeout(BYE_WAIT, async {
            let link = self.link.lock().await.clone();
            let Some(link) = link else {
                return;
            };
            let sent = {
                let mut writer = link.writer.lock().await;
                ipc::write_frame(&mut *writer, &ToDaemon::Bye).await.is_ok()
            };
            // The reader drains what the daemon still sends until it reads the Bye and closes the
            // stream, which cancels `closed`.
            if sent {
                link.closed.cancelled().await;
            }
        })
        .await;
    }

    /// The live link, connecting first when there is none. One caller connects at a time; the
    /// others find the link it made.
    async fn link(
        &self,
        client_name: &str,
        deadline: std::time::Instant,
    ) -> Result<Arc<Link>, String> {
        let mut slot = self.link.lock().await;
        if let Some(link) = slot.as_ref()
            && !link.closed.is_cancelled()
        {
            return Ok(Arc::clone(link));
        }
        *slot = None;
        let (identity, paths) = self.identity().await?;
        let (mut client, startup) =
            runtime::connect_or_start(identity, paths, deadline, &[]).await?;
        let (cwd, env) = self.launch.to_wire();
        let hello = ToDaemon::Hello {
            protocol: ipc::PROTOCOL,
            namespace: identity.namespace.clone(),
            build: identity.build.clone(),
            session: self.session.clone(),
            connection: self.generation.fetch_add(1, Ordering::Relaxed),
            client_name: client_name.to_owned(),
            output_mode: self.render.output_mode.name().to_owned(),
            structured_content: self.render.structured_content,
            cwd,
            env,
        };
        let epoch = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            handshake(&mut client, hello, identity),
        )
        .await
        {
            Ok(answer) => answer?,
            Err(_) => return Err("the daemon did not answer the Hello in time".to_owned()),
        };
        // The start is exclusive until the daemon has answered, and no longer.
        drop(startup);
        let (reader, writer) = tokio::io::split(client);
        #[cfg(test)]
        let writer = tests::ControlledWriter::new(writer);
        let link = Arc::new(Link {
            epoch,
            writer: Mutex::new(writer),
            pending: std::sync::Mutex::new(HashMap::new()),
            closed: CancellationToken::new(),
        });
        tokio::spawn(read_loop(Arc::clone(&link), reader, Arc::clone(&self.link)));
        *slot = Some(Arc::clone(&link));
        Ok(link)
    }

    /// Ends a link at once: every waiting call is lost and the reader stops, which drops the
    /// read half; the write half goes with the last reference to the link. Nothing here waits,
    /// so a caller's own bound stays the caller's. A failed write may have left half a frame, so
    /// the stream is never used again.
    fn close(&self, link: &Link) {
        link.closed.cancel();
        lock(&link.pending).clear();
    }

    /// The call may have run, so it is reported, never replayed.
    fn lost(
        &self,
        link: &Link,
        tool: &str,
        action: &str,
        id: u64,
        request_id: &str,
    ) -> CallToolResult {
        if tool == "exec" || tool == "codemode" {
            let action = if tool == "codemode" { tool } else { action };
            return error(format!(
                "The connection to the fastexec daemon (epoch {}) closed during {action} call {id} (request {request_id}, session {}); the operation may have run. Use action list to check.",
                link.epoch, self.session
            ));
        }
        error(format!(
            "The connection to the fastexec daemon (epoch {}) closed during {tool} call {id} (request {request_id}, session {}); the call may or may not have run.",
            link.epoch, self.session
        ))
    }
}

#[cfg(test)]
impl Endpoint {
    /// An endpoint whose identity is already settled and whose daemon is the listener at
    /// `paths.endpoint`. The unit tests own calls, links and cancellation, not identity
    /// creation, so nothing here hashes an image or starts a process.
    fn for_test(identity: runtime::Identity, paths: runtime::Paths) -> Self {
        let settled = OnceCell::new();
        let _ = settled.set(Ok((identity, paths)));
        let (announce, ready) = watch::channel(true);
        drop(announce);
        Self {
            identity: Arc::new(settled),
            ready,
            launch: Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new())),
            render: Render {
                output_mode: crate::service::OutputMode::Text,
                structured_content: true,
            },
            session: "s".repeat(26),
            next_call: AtomicU64::new(1),
            generation: AtomicU64::new(1),
            link: Arc::new(Mutex::new(None)),
        }
    }
}

/// Reads every frame the daemon sends: a call's progress, its result, and the end of the
/// connection, which loses every call still waiting.
async fn read_loop(
    link: Arc<Link>,
    mut reader: ReadHalf<transport::Client>,
    slot: Arc<Mutex<Option<Arc<Link>>>>,
) {
    loop {
        // Selecting on `closed` is what lets a closing link wake a reader that is waiting for
        // bytes: a flag alone would leave it parked in `read_frame`.
        let frame = tokio::select! {
            _ = link.closed.cancelled() => None,
            frame = ipc::read_frame::<_, ToEndpoint>(&mut reader, ipc::RESPONSE_LIMIT) => {
                frame.ok().flatten()
            }
        };
        match frame {
            Some(ToEndpoint::Progress {
                id,
                seconds,
                message,
            }) => {
                let pending = lock(&link.pending);
                if let Some(progress) = pending.get(&id).and_then(|entry| entry.progress.as_ref()) {
                    progress.send_replace(Some((seconds, message)));
                }
            }
            Some(ToEndpoint::Result { id, result }) => {
                // A call that was cancelled is no longer pending, and its late result is not
                // anyone's answer.
                let entry = lock(&link.pending).remove(&id);
                if let Some(entry) = entry {
                    let _ = entry.result.send(result);
                }
            }
            // Nothing else belongs on this connection after the Welcome.
            _ => break,
        }
    }
    link.closed.cancel();
    // Dropping the senders is what makes a waiting call see the link end.
    lock(&link.pending).clear();
    drop(reader);
    // A reconnect holds the slot while handshaking; do not keep the old writer alive behind it.
    let ended = Arc::downgrade(&link);
    drop(link);
    let mut slot = slot.lock().await;
    if slot
        .as_ref()
        .is_some_and(|current| ended.ptr_eq(&Arc::downgrade(current)))
    {
        *slot = None;
    }
}

/// Sends the Hello and returns the Welcome's epoch, or the reason the daemon refused it.
pub async fn handshake(
    client: &mut transport::Client,
    hello: ToDaemon,
    identity: &runtime::Identity,
) -> Result<String, String> {
    ipc::write_frame(client, &hello)
        .await
        .map_err(|error| format!("cannot send the Hello: {error}"))?;
    match ipc::read_frame::<_, ToEndpoint>(client, ipc::RESPONSE_LIMIT).await {
        Ok(Some(ToEndpoint::Welcome {
            protocol,
            namespace,
            build,
            epoch,
        })) => {
            // A Welcome is only a Welcome from the daemon this endpoint meant to reach.
            if protocol != ipc::PROTOCOL {
                return Err(format!(
                    "the daemon speaks protocol {protocol}, not {}",
                    ipc::PROTOCOL
                ));
            }
            if namespace != identity.namespace {
                return Err(format!(
                    "the daemon serves namespace {namespace}, not {}",
                    identity.namespace
                ));
            }
            if build != identity.build {
                return Err(format!(
                    "the daemon runs build {build}, not {}",
                    identity.build
                ));
            }
            Ok(epoch)
        }
        Ok(Some(ToEndpoint::Reject { message })) => Err(message),
        Ok(other) => Err(format!("the daemon answered the Hello with {other:?}")),
        Err(error) => Err(format!("cannot read the Welcome: {error}")),
    }
}

fn settled(outcome: &Identity) -> Result<&(runtime::Identity, runtime::Paths), String> {
    outcome.as_ref().map_err(|message| message.clone())
}

fn error(text: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(text.into())])
}

/// A lock whose holder never awaits while it is held.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::OsText;
    use crate::transport::{Endpoint as TestEndpoint, Listener};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::AsyncWrite;
    use tokio::time::timeout;

    /// An opt-in write barrier for one link, independent of the transport's buffering.
    pub(super) struct ControlledWriter {
        inner: WriteHalf<transport::Client>,
        pause: Option<WritePause>,
    }

    struct WritePause {
        remaining: usize,
        blocked: Option<oneshot::Sender<()>>,
    }

    impl ControlledWriter {
        pub(super) fn new(inner: WriteHalf<transport::Client>) -> Self {
            Self { inner, pause: None }
        }
    }

    impl AsyncWrite for ControlledWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if bytes.is_empty() {
                return Poll::Ready(Ok(0));
            }
            let length = if let Some(pause) = &mut this.pause {
                if pause.remaining == 0 {
                    if let Some(blocked) = pause.blocked.take() {
                        let _ = blocked.send(());
                    }
                    return Poll::Pending;
                }
                bytes.len().min(pause.remaining)
            } else {
                bytes.len()
            };
            let written = Pin::new(&mut this.inner).poll_write(cx, &bytes[..length]);
            if let Poll::Ready(Ok(count)) = &written
                && let Some(pause) = &mut this.pause
            {
                pause.remaining -= count;
            }
            written
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    /// Answers one Hello on a real endpoint with `welcome`, then holds the connection until the
    /// client lets go.
    async fn welcome_with(
        endpoint: &TestEndpoint,
        welcome: ToEndpoint,
    ) -> tokio::task::JoinHandle<()> {
        let mut listener = Listener::bind(endpoint.name()).unwrap();
        tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let _ = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await;
            let _ = ipc::write_frame(&mut conn, &welcome).await;
            let mut buffer = [0u8; 1];
            let _ = tokio::io::AsyncReadExt::read(&mut conn, &mut buffer).await;
        })
    }

    fn identity() -> runtime::Identity {
        runtime::Identity {
            build: "b".repeat(64),
            namespace: "0123456789abcdef".to_owned(),
        }
    }

    fn hello(identity: &runtime::Identity) -> ToDaemon {
        ToDaemon::Hello {
            protocol: ipc::PROTOCOL,
            namespace: identity.namespace.clone(),
            build: identity.build.clone(),
            session: "s".repeat(26),
            connection: 1,
            client_name: "test".to_owned(),
            output_mode: "text".to_owned(),
            structured_content: true,
            cwd: OsText::Text("/work".to_owned()),
            env: Vec::new(),
        }
    }

    const TEST_EPOCH: &str = "eeeeeeee";

    fn welcome(identity: &runtime::Identity) -> ToEndpoint {
        ToEndpoint::Welcome {
            protocol: ipc::PROTOCOL,
            namespace: identity.namespace.clone(),
            build: identity.build.clone(),
            epoch: TEST_EPOCH.to_owned(),
        }
    }

    /// A Welcome is only accepted from the daemon the endpoint meant to reach: the protocol, the
    /// namespace and the build all have to be this one's.
    #[tokio::test]
    async fn a_welcome_must_name_this_protocol_namespace_and_build() {
        let identity = identity();
        let mut wrong_protocol = welcome(&identity);
        if let ToEndpoint::Welcome { protocol, .. } = &mut wrong_protocol {
            *protocol = ipc::PROTOCOL + 1;
        }
        let mut wrong_namespace = welcome(&identity);
        if let ToEndpoint::Welcome { namespace, .. } = &mut wrong_namespace {
            *namespace = "ffffffffffffffff".to_owned();
        }
        let mut wrong_build = welcome(&identity);
        if let ToEndpoint::Welcome { build, .. } = &mut wrong_build {
            *build = "c".repeat(64);
        }
        let cases = [
            (welcome(&identity), None),
            (wrong_protocol, Some("speaks protocol")),
            (wrong_namespace, Some("serves namespace")),
            (wrong_build, Some("runs build")),
        ];
        for (index, (answer, expected)) in cases.into_iter().enumerate() {
            let endpoint = TestEndpoint::new(&format!("welcome{index}"));
            let server = welcome_with(&endpoint, answer).await;
            let mut client = transport::connect(endpoint.name()).await.unwrap();
            // The whole exchange is bounded, so a missing Hello or Welcome fails here instead of
            // hanging the harness.
            let answer = timeout(
                Duration::from_secs(30),
                handshake(&mut client, hello(&identity), &identity),
            )
            .await
            .expect("the exchange finished");
            drop(client);
            let _ = timeout(Duration::from_secs(30), server).await;
            match expected {
                None => assert_eq!(answer.unwrap(), TEST_EPOCH),
                Some(text) => {
                    let message = answer.unwrap_err();
                    assert!(message.contains(text), "{message}");
                }
            }
        }
    }

    /// A daemon the test drives: a real listener at a path the endpoint is pointed at, so the
    /// frames the endpoint sends are the thing under test.
    fn test_daemon(tag: &str) -> (runtime::Identity, runtime::Paths, Listener, TestEndpoint) {
        let target = TestEndpoint::new(tag);
        let listener = Listener::bind(target.name()).unwrap();
        let paths = runtime::Paths {
            root: std::env::temp_dir().join(format!("fastexec-endpoint-{tag}")),
            ns: std::env::temp_dir().join(format!("fastexec-endpoint-{tag}-ns")),
            endpoint: target.name().to_owned(),
        };
        // The target stays with the caller: on Unix it removes its socket directory when it is
        // dropped, and the endpoint has to be able to connect first.
        (identity(), paths, listener, target)
    }

    /// The peer has finished, and said so: a peer that panicked or hung is a failed test, not a
    /// quiet one.
    async fn finish(peer: tokio::task::JoinHandle<()>) {
        timeout(Duration::from_secs(10), peer)
            .await
            .expect("the peer finished")
            .expect("the peer did not panic");
    }

    #[tokio::test]
    async fn cancellation_during_a_handshake_never_dispatches_the_call() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel-hello");
        let (announced, hello_seen) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            announced.send(()).unwrap();
            // No Welcome: cancellation must end the connection without sending a Call.
            assert!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        let endpoint = Endpoint::for_test(identity, paths);
        let cancel = CancellationToken::new();
        let calling = endpoint.call(
            "exec",
            serde_json::json!({"action": "list"}),
            "1".into(),
            "test",
            cancel.clone(),
            None,
        );
        tokio::pin!(calling);
        timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut calling => panic!("call ended before the handshake: {result:?}"),
                seen = hello_seen => seen.unwrap(),
            }
            cancel.cancel();
            let result = (&mut calling).await;
            assert_eq!(
                result.content[0].as_text().unwrap().text,
                "Cancelled before the call was sent."
            );
        })
        .await
        .expect("cancellation does not wait for a Welcome");
        assert!(endpoint.link.lock().await.is_none());
        finish(peer).await;
    }

    #[tokio::test]
    async fn cancellation_while_queued_for_the_writer_preserves_the_link() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel-queue");
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            // The first call was cancelled before sending; only the second may arrive.
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 2, .. })
            ));
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::Result {
                    id: 2,
                    result: CallToolResult::success(vec![ContentBlock::text("ok")]),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });
        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        let busy = link.writer.lock().await;
        let cancel = CancellationToken::new();
        let calling = {
            let (endpoint, cancel) = (Arc::clone(&endpoint), cancel.clone());
            tokio::spawn(async move {
                endpoint
                    .call(
                        "exec",
                        serde_json::json!({"action": "list"}),
                        "1".into(),
                        "test",
                        cancel,
                        None,
                    )
                    .await
            })
        };
        timeout(Duration::from_secs(5), async {
            while !lock(&link.pending).contains_key(&1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the call queued for the writer");
        cancel.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the queued cancellation ended")
            .unwrap();
        assert_eq!(
            result.content[0].as_text().unwrap().text,
            "Cancelled before the call was sent."
        );
        assert!(!link.closed.is_cancelled());
        assert!(lock(&link.pending).is_empty());
        drop(busy);
        let result = timeout(
            Duration::from_secs(5),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "2".into(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .expect("the existing link still works");
        assert_eq!(result.content[0].as_text().unwrap().text, "ok");
        endpoint.bye().await;
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }

    #[tokio::test]
    async fn cancellation_during_a_frame_write_closes_then_reconnects() {
        use tokio::io::AsyncReadExt;

        const PARTIAL_BYTES: usize = 5;
        let (identity, paths, mut listener, _target) = test_daemon("cancel-frame");
        let welcome = welcome(&identity);
        let (announced, writing) = oneshot::channel();
        let (paused, blocked) = oneshot::channel();
        let (release, drain) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut first = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut first, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut first, &welcome).await.unwrap();
            let mut bytes = vec![0_u8; PARTIAL_BYTES];
            first.read_exact(&mut bytes).await.unwrap();
            announced.send(()).unwrap();
            drain.await.unwrap();
            first.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes.len(), PARTIAL_BYTES);
            let declared = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
            assert!(
                bytes.len() < declared + 4,
                "the fixture must leave a partial frame"
            );
            drop(first);

            let mut second = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut second, &welcome).await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 2, .. })
            ));
            ipc::write_frame(
                &mut second,
                &ToEndpoint::Result {
                    id: 2,
                    result: CallToolResult::success(vec![ContentBlock::text("ok")]),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });
        let endpoint = Endpoint::for_test(identity, paths);
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        link.writer.lock().await.pause = Some(WritePause {
            remaining: PARTIAL_BYTES,
            blocked: Some(paused),
        });
        let cancel = CancellationToken::new();
        let calling = endpoint.call(
            "write",
            serde_json::json!({"path": "unused", "content": "partial"}),
            "1".into(),
            "test",
            cancel.clone(),
            None,
        );
        tokio::pin!(calling);
        timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut calling => panic!("call ended before writing: {result:?}"),
                () = async {
                    blocked.await.unwrap();
                    writing.await.unwrap();
                } => {},
            }
            cancel.cancel();
            let result = (&mut calling).await;
            assert!(
                result.content[0]
                    .as_text()
                    .unwrap()
                    .text
                    .contains("the call may or may not have run")
            );
        })
        .await
        .expect("cancellation does not wait for a blocked frame write");
        assert!(link.closed.is_cancelled());
        assert!(lock(&link.pending).is_empty());
        drop(link);
        release.send(()).unwrap();
        let result = timeout(
            Duration::from_secs(5),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "2".into(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .expect("a later call reconnects");
        assert_eq!(result.content[0].as_text().unwrap().text, "ok");
        endpoint.bye().await;
        finish(peer).await;
    }

    #[tokio::test]
    async fn a_cancel_that_cannot_be_sent_reports_the_lost_link() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel-blocked");
        let welcome = welcome(&identity);
        let (announced, call_seen) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 1, .. })
            ));
            announced.send(()).unwrap();
            // The writer stays locked until Cancel times out; no Cancel frame can arrive.
            assert!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        let cancel = CancellationToken::new();
        let calling = {
            let (endpoint, cancel) = (Arc::clone(&endpoint), cancel.clone());
            tokio::spawn(async move {
                endpoint
                    .call(
                        "write",
                        serde_json::json!({"path": "unused", "content": "complete"}),
                        "1".into(),
                        "test",
                        cancel,
                        None,
                    )
                    .await
            })
        };
        timeout(Duration::from_secs(5), call_seen)
            .await
            .expect("the peer received the full call")
            .unwrap();
        let busy = timeout(Duration::from_secs(5), link.writer.lock())
            .await
            .expect("the call released the writer");
        cancel.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the blocked Cancel finished")
            .unwrap();
        assert!(
            result.content[0]
                .as_text()
                .unwrap()
                .text
                .contains("the call may or may not have run")
        );
        assert!(link.closed.is_cancelled());
        assert!(lock(&link.pending).is_empty());
        drop(busy);
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }

    /// The endpoint's own boundary: a cancelled call writes `Cancel` for its ID to the daemon,
    /// stops waiting, and leaves nothing pending.
    #[tokio::test]
    async fn a_cancel_reaches_the_daemon_and_ends_the_wait() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let Ok(Some(ToDaemon::Hello { .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            else {
                return;
            };
            if ipc::write_frame(&mut conn, &welcome).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            {
                let seen = match frame {
                    ToDaemon::Call { id, .. } => format!("call {id}"),
                    ToDaemon::Cancel { id } => format!("cancel {id}"),
                    ToDaemon::Bye => break,
                    _ => continue,
                };
                observed.lock().unwrap().push(seen);
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let cancel = CancellationToken::new();
        let calling = {
            let endpoint = Arc::clone(&endpoint);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                endpoint
                    .call(
                        "exec",
                        serde_json::json!({"action": "list"}),
                        "1".to_owned(),
                        "test",
                        cancel,
                        None,
                    )
                    .await
            })
        };
        // The call reaches the daemon, which never answers it.
        let mut waited = 0;
        while seen.lock().unwrap().is_empty() && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        assert_eq!(*seen.lock().unwrap(), vec!["call 1".to_owned()]);

        cancel.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the cancelled call ended")
            .unwrap();
        assert_eq!(
            result.content[0].as_text().map(|text| text.text.clone()),
            Some("Cancelled; the task keeps running.".to_owned())
        );

        let mut waited = 0;
        while seen.lock().unwrap().len() < 2 && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        // The cancelled call left nothing behind: its ID no longer waits for an answer.
        let link = endpoint.link.lock().await.clone().expect("a live link");
        assert!(
            lock(&link.pending).is_empty(),
            "the cancelled call is still pending"
        );

        endpoint.bye().await;
        drop(endpoint);
        finish(peer).await;
        // Every frame is in: the call, then its Cancel, and nothing else.
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["call 1".to_owned(), "cancel 1".to_owned()]
        );
    }

    /// A connection that ends or is refused before its Welcome is retried once, under the same
    /// deadline: the call is not replayed, and it keeps the ID it was given.
    #[tokio::test]
    async fn a_connection_that_ends_before_a_welcome_is_retried_once() {
        let (identity, paths, mut listener, _target) = test_daemon("retry");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            // The first connection is refused before any Welcome.
            let Ok(mut first) = listener.accept().await else {
                return;
            };
            if let Ok(Some(ToDaemon::Hello { connection, .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut first, ipc::REQUEST_LIMIT).await
            {
                observed.lock().unwrap().push(format!("hello {connection}"));
            }
            let reject = ToEndpoint::Reject {
                message: "not yet".to_owned(),
            };
            let _ = ipc::write_frame(&mut first, &reject).await;
            drop(first);

            // The second one is answered, and the call arrives on it.
            let Ok(mut second) = listener.accept().await else {
                return;
            };
            if let Ok(Some(ToDaemon::Hello { connection, .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT).await
            {
                observed.lock().unwrap().push(format!("hello {connection}"));
            }
            if ipc::write_frame(&mut second, &welcome).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT).await
            {
                match frame {
                    ToDaemon::Call { id, .. } => {
                        observed.lock().unwrap().push(format!("call {id}"));
                        let answer = ToEndpoint::Result {
                            id,
                            result: CallToolResult::success(vec![ContentBlock::text("ok")]),
                        };
                        if ipc::write_frame(&mut second, &answer).await.is_err() {
                            return;
                        }
                    }
                    ToDaemon::Bye => break,
                    _ => {}
                }
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let result = timeout(
            Duration::from_secs(30),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "1".to_owned(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .expect("the call finished");
        assert_eq!(
            result.content[0].as_text().map(|text| text.text.clone()),
            Some("ok".to_owned()),
            "the retried connection did not answer the call"
        );
        endpoint.bye().await;
        drop(endpoint);
        finish(peer).await;
        // Two Hellos from one endpoint, the second on a newer connection, and one Call: the
        // retry sent nothing that was already sent.
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                "hello 1".to_owned(),
                "hello 2".to_owned(),
                "call 1".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn a_complete_result_survives_the_connection_ending() {
        let (identity, paths, mut listener, _target) = test_daemon("last-result");
        let welcome = welcome(&identity);
        let (seen, request_arrived) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let hello = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            assert!(matches!(hello, Some(ToDaemon::Hello { .. })));
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            let Some(ToDaemon::Call { id, .. }) = ipc::read_frame(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap()
            else {
                panic!("expected a call");
            };
            seen.send(()).unwrap();
            let result = CallToolResult::success(vec![ContentBlock::text("confirmed")]);
            ipc::write_frame(&mut conn, &ToEndpoint::Result { id, result })
                .await
                .unwrap();
        });

        let endpoint = Endpoint::for_test(identity, paths);
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        let call = endpoint.call(
            "exec",
            serde_json::json!({"action": "list"}),
            "1".to_owned(),
            "test",
            CancellationToken::new(),
            None,
        );
        tokio::pin!(call);
        // Only resume the caller after the reader has processed both the Result and EOF.
        timeout(Duration::from_secs(10), async {
            tokio::select! {
                biased;
                seen = request_arrived => seen.unwrap(),
                _ = &mut call => panic!("the call ended before the peer received it"),
            }
        })
        .await
        .expect("the daemon received the call");
        timeout(Duration::from_secs(10), link.closed.cancelled())
            .await
            .unwrap();
        let result = timeout(Duration::from_secs(10), call).await.unwrap();
        assert_eq!(result.content[0].as_text().unwrap().text, "confirmed");
        finish(peer).await;
    }

    /// A call queued behind a busy writer must let go of a link that has ended, rather than
    /// write a fresh frame onto a stream that may hold half of one.
    #[tokio::test]
    async fn a_queued_write_does_not_reach_a_link_that_has_ended() {
        let (identity, paths, mut listener, _target) = test_daemon("queued");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let Ok(Some(ToDaemon::Hello { .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            else {
                return;
            };
            if ipc::write_frame(&mut conn, &welcome).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            {
                match frame {
                    ToDaemon::Call { id, .. } => {
                        observed.lock().unwrap().push(format!("call {id}"))
                    }
                    ToDaemon::Bye => break,
                    _ => {}
                }
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .expect("a link");
        // The test holds the writer, so the call below queues behind it.
        let busy = link.writer.lock().await;
        let calling = {
            let endpoint = Arc::clone(&endpoint);
            tokio::spawn(async move {
                endpoint
                    .call(
                        "exec",
                        serde_json::json!({"action": "list"}),
                        "1".to_owned(),
                        "test",
                        CancellationToken::new(),
                        None,
                    )
                    .await
            })
        };
        // The call is on this link, queued behind the test's writer, before the link ends.
        let mut waited = 0;
        while lock(&link.pending).is_empty() && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        assert_eq!(
            lock(&link.pending).keys().copied().collect::<Vec<u64>>(),
            vec![1],
            "the call is not queued on this link"
        );
        endpoint.close(&link);
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the queued call ended")
            .unwrap();
        assert_eq!(
            result.content[0].as_text().map(|text| text.text.clone()),
            Some(format!(
                "The connection to the fastexec daemon (epoch {}) closed during list call 1 (request 1, session {}); the operation may have run. Use action list to check.",
                TEST_EPOCH, endpoint.session
            ))
        );
        drop(busy);
        // Nothing was written onto the ended link.
        assert!(
            seen.lock().unwrap().is_empty(),
            "{:?}",
            seen.lock().unwrap()
        );
        assert!(lock(&link.pending).is_empty(), "the call is still pending");

        // Dropping the last reference closes the connection, which ends the peer.
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }

    /// A file tool's lost call names the tool and says only that it may or may not have run.
    #[tokio::test]
    async fn a_lost_file_tool_call_names_its_tool_and_no_action_list() {
        let (identity, paths, mut listener, _target) = test_daemon("lost-files");
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let Ok(Some(ToDaemon::Hello { .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            else {
                return;
            };
            if ipc::write_frame(&mut conn, &welcome).await.is_err() {
                return;
            }
            // The daemon reads the call and never answers it, so the link is lost under it.
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            {
                if matches!(frame, ToDaemon::Bye) {
                    break;
                }
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .expect("a link");
        let busy = link.writer.lock().await;
        let calling = {
            let endpoint = Arc::clone(&endpoint);
            tokio::spawn(async move {
                endpoint
                    .call(
                        "read",
                        serde_json::json!({"path": "notes.txt"}),
                        "1".to_owned(),
                        "test",
                        CancellationToken::new(),
                        None,
                    )
                    .await
            })
        };
        let mut waited = 0;
        while lock(&link.pending).is_empty() && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        assert_eq!(
            lock(&link.pending).keys().copied().collect::<Vec<u64>>(),
            vec![1],
            "the call is not queued on this link"
        );
        endpoint.close(&link);
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the queued call ended")
            .unwrap();
        let text = result.content[0]
            .as_text()
            .map(|text| text.text.clone())
            .unwrap_or_default();
        assert_eq!(
            text,
            format!(
                "The connection to the fastexec daemon (epoch {}) closed during read call 1 (request 1, session {}); the call may or may not have run.",
                TEST_EPOCH, endpoint.session
            )
        );
        assert!(!text.contains("action list"), "{text}");
        drop(busy);

        // Dropping the last reference closes the connection, which ends the peer.
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }
}
