//! The wire between an endpoint and the daemon: length-prefixed JSON frames and the messages they
//! carry. This module is the format alone; the socket block wires it up, and until then only the
//! tests below use it.
#![cfg_attr(not(test), allow(dead_code))]

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// What a Hello and a Welcome must agree on.
pub const PROTOCOL: u32 = 1;

/// The largest frame the daemon accepts. A Hello carries the endpoint's whole environment, which
/// is the reason this is larger than a call.
pub const REQUEST_LIMIT: usize = 4 << 20;

/// The largest serialized arguments a call may carry. The endpoint refuses a larger call itself,
/// as a tool error, before it writes anything.
pub const CALL_LIMIT: usize = 1 << 20;

/// The largest frame the endpoint accepts. Results are sized to fit, so a larger one is a bug and
/// the endpoint closes the connection.
pub const RESPONSE_LIMIT: usize = 64 << 20;

/// A message as the bytes a frame holds: the u32 little-endian length and its JSON. Encoding
/// stands apart from writing so a caller can size the bytes it will send before it queues them.
pub fn encode_frame<T>(message: &T) -> std::io::Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let body = serde_json::to_vec(message)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let length = u32::try_from(body.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {} bytes does not fit its length", body.len()),
        )
    })?;
    let mut bytes = Vec::with_capacity(4 + body.len());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// The bytes a frame of `message` occupies, counted without holding them: the length prefix and
/// the JSON. A caller that must reserve room for a frame before it exists measures it here, so
/// no encoded buffer is ever held without the room for it.
pub fn frame_len<T>(message: &T) -> std::io::Result<usize>
where
    T: Serialize + ?Sized,
{
    struct Count(usize);

    impl std::io::Write for Count {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0 += buffer.len();
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut count = Count(0);
    serde_json::to_writer(&mut count, message)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(4 + count.0)
}

/// Writes bytes that are already a frame, then flushes them: a frame a reader waits on is never
/// left in a buffer.
pub async fn write_raw<W>(writer: &mut W, bytes: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(bytes).await?;
    writer.flush().await
}

/// Writes `message` as one frame.
pub async fn write_frame<W, T>(writer: &mut W, message: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize + ?Sized,
{
    write_raw(writer, &encode_frame(message)?).await
}

/// Reads one frame, or `None` when the reader ended cleanly before a length. A reader that ends
/// inside a frame is an error, and so is a length over `max`: the body of an oversized frame is
/// never allocated, because the length it claims is not trusted.
pub async fn read_frame<R, T>(reader: &mut R, max: usize) -> std::io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        let read = reader.read(&mut header[filled..]).await?;
        if read == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("the input ended after {filled} of 4 length bytes"),
            ));
        }
        filled += read;
    }
    let length = u32::from_le_bytes(header) as usize;
    if length > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is over the {max} byte limit"),
        ));
    }
    let mut body = vec![0u8; length];
    let mut filled = 0;
    while filled < length {
        let read = reader.read(&mut body[filled..]).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("the frame ended after {filled} of {length} bytes"),
            ));
        }
        filled += read;
    }
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

/// An OS string as the wire carries it: a JSON string when it is valid Unicode, and its native
/// units otherwise, so an environment name or value survives exactly. A path is not text here;
/// a byte that is not UTF-8 is data to hand back to the OS unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OsText {
    Text(String),
    Units { units: Vec<Unit> },
}

/// The native unit of an OS string: UTF-16 on Windows, a byte on Unix.
#[cfg(windows)]
pub type Unit = u16;
/// The native unit of an OS string: UTF-16 on Windows, a byte on Unix.
#[cfg(unix)]
pub type Unit = u8;

#[cfg(windows)]
fn units_of(value: &OsStr) -> Vec<Unit> {
    use std::os::windows::ffi::OsStrExt;
    value.encode_wide().collect()
}

#[cfg(unix)]
fn units_of(value: &OsStr) -> Vec<Unit> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn from_units(units: &[Unit]) -> OsString {
    use std::os::windows::ffi::OsStringExt;
    OsString::from_wide(units)
}

#[cfg(unix)]
fn from_units(units: &[Unit]) -> OsString {
    use std::os::unix::ffi::OsStringExt;
    OsString::from_vec(units.to_vec())
}

impl From<&OsStr> for OsText {
    fn from(value: &OsStr) -> Self {
        match value.to_str() {
            Some(text) => Self::Text(text.to_owned()),
            None => Self::Units {
                units: units_of(value),
            },
        }
    }
}

impl OsText {
    /// The OS string this text stands for. Both spellings are read as they stand: units holding
    /// nothing are the empty OS string, exactly as `Text("")` is, because an empty environment
    /// value is legitimate and neither spelling is more correct than the other.
    pub fn to_os_string(&self) -> Result<OsString, String> {
        match self {
            Self::Text(text) => Ok(OsString::from(text)),
            Self::Units { units } => Ok(from_units(units)),
        }
    }
}

/// What an endpoint sends the daemon.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToDaemon {
    /// The first frame on a connection: who this endpoint is, what it can render, and the
    /// directory and environment its tasks start with.
    Hello {
        protocol: u32,
        namespace: String,
        build: String,
        session: String,
        connection: u64,
        client_name: String,
        output_mode: String,
        structured_content: bool,
        cwd: OsText,
        env: Vec<(OsText, OsText)>,
    },
    /// One call. `id` comes from an endpoint-wide counter, so it keeps naming this call across a
    /// reconnect; `request_id` is the MCP JSON-RPC ID, kept as a label because clients reuse it.
    Call {
        id: u64,
        request_id: String,
        arguments: serde_json::Value,
    },
    /// The request was cancelled: its wait ends and its task keeps running.
    Cancel { id: u64 },
    /// The endpoint's input ended, so the daemon drops the session at once.
    Bye,
    /// Report the daemon's state. Only a connection that has a `Welcome` may ask.
    Status,
    /// Stop the daemon. Only a connection that has a `Welcome` may ask.
    Stop,
}

/// What the daemon sends an endpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToEndpoint {
    /// The handshake was accepted, and `epoch` names the daemon instance the session is bound to.
    Welcome {
        protocol: u32,
        namespace: String,
        build: String,
        epoch: String,
    },
    /// The handshake was refused, and `message` says why.
    Reject { message: String },
    /// A call is still running: `seconds` since it started, and what it is waiting on.
    Progress {
        id: u64,
        seconds: f64,
        message: String,
    },
    /// A call finished.
    Result {
        id: u64,
        result: rmcp::model::CallToolResult,
    },
    /// The answer to `Status`: which daemon this is, and what it holds.
    Status {
        epoch: String,
        build: String,
        pid: u32,
        sessions: usize,
        tasks: usize,
        /// The address it serves Streamable HTTP on, when it serves it.
        http: Option<String>,
    },
}

impl crate::session::LaunchContext {
    /// The context a Hello describes, or the field that could not be read.
    pub fn from_wire(cwd: &OsText, env: &[(OsText, OsText)]) -> Result<Self, String> {
        let cwd = cwd
            .to_os_string()
            .map_err(|problem| format!("cwd: {problem}"))?;
        let mut entries = Vec::with_capacity(env.len());
        for (index, (name, value)) in env.iter().enumerate() {
            let name = name
                .to_os_string()
                .map_err(|problem| format!("env[{index}] name: {problem}"))?;
            let value = value
                .to_os_string()
                .map_err(|problem| format!("env[{index}] value: {problem}"))?;
            entries.push((name, value));
        }
        Ok(Self::new(cwd.into(), entries))
    }

    /// This context as a Hello carries it.
    pub fn to_wire(&self) -> (OsText, Vec<(OsText, OsText)>) {
        let env = self
            .env
            .iter()
            .map(|(name, value)| {
                (
                    OsText::from(name.as_os_str()),
                    OsText::from(value.as_os_str()),
                )
            })
            .collect();
        (OsText::from(self.cwd.as_os_str()), env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::OutputMode;
    use serde_json::json;
    use std::path::PathBuf;

    /// The bytes a frame occupies, as a reader would find them.
    async fn frame_bytes<T: Serialize>(message: &T) -> Vec<u8> {
        let bytes = encode_frame(message).unwrap();
        // What the encoder produced is what the writer sends, unchanged. The writer buffers, so
        // the flush inside `write_raw` is what gets the frame out.
        let (writer, mut reader) = tokio::io::duplex(1 << 16);
        let mut writer = tokio::io::BufWriter::new(writer);
        let reading = async {
            let mut sent = Vec::new();
            reader.read_to_end(&mut sent).await.unwrap();
            sent
        };
        let writing = async {
            write_raw(&mut writer, &bytes).await.unwrap();
            drop(writer);
        };
        assert_eq!(tokio::join!(reading, writing).0, bytes);
        bytes
    }

    async fn round_trip<T>(message: &T, max: usize) -> T
    where
        T: Serialize + DeserializeOwned,
    {
        let bytes = frame_bytes(message).await;
        let mut reader = std::io::Cursor::new(bytes);
        read_frame(&mut reader, max)
            .await
            .unwrap()
            .expect("a frame")
    }

    fn hello() -> ToDaemon {
        ToDaemon::Hello {
            protocol: PROTOCOL,
            namespace: "fastexec".to_owned(),
            build: "0.1.0".to_owned(),
            session: "3f2a".to_owned(),
            connection: 2,
            client_name: "probe".to_owned(),
            output_mode: "both".to_owned(),
            structured_content: true,
            cwd: OsText::Text("/work".to_owned()),
            env: vec![(
                OsText::Text("PATH".to_owned()),
                OsText::Text("/usr/bin".to_owned()),
            )],
        }
    }

    #[tokio::test]
    async fn a_frame_is_a_length_and_its_json() {
        let body = br#"{"type":"bye"}"#;
        let mut expected = u32::try_from(body.len()).unwrap().to_le_bytes().to_vec();
        expected.extend_from_slice(body);
        assert_eq!(frame_bytes(&ToDaemon::Bye).await, expected);
    }

    #[tokio::test]
    async fn every_message_survives_the_wire() {
        let mut result =
            rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::Text(
                rmcp::model::TextContent::new("done"),
            )]);
        result.structured_content = Some(json!({"state": "exited"}));
        let sent = [
            hello(),
            ToDaemon::Call {
                id: 7,
                request_id: "3".to_owned(),
                arguments: json!({"action": "start", "command": "echo hi"}),
            },
            ToDaemon::Cancel { id: 7 },
            ToDaemon::Bye,
        ];
        for message in &sent {
            assert_eq!(&round_trip(message, REQUEST_LIMIT).await, message);
        }
        let received = [
            ToEndpoint::Welcome {
                protocol: PROTOCOL,
                namespace: "fastexec".to_owned(),
                build: "0.1.0".to_owned(),
                epoch: "e1".to_owned(),
            },
            ToEndpoint::Reject {
                message: "protocol 2".to_owned(),
            },
            ToEndpoint::Progress {
                id: 7,
                seconds: 1.5,
                message: "still running".to_owned(),
            },
            ToEndpoint::Result {
                id: 7,
                result: result.clone(),
            },
        ];
        for message in &received {
            assert_eq!(&round_trip(message, RESPONSE_LIMIT).await, message);
        }
    }

    #[tokio::test]
    async fn arguments_at_the_call_limit_still_fit_a_frame_the_daemon_reads() {
        // The endpoint's limit is on the serialized arguments, so a call at that limit carries an
        // envelope over it and is still a frame the daemon reads: the daemon's limit is larger.
        let arguments = json!({"command": "x".repeat(CALL_LIMIT - 14)});
        assert_eq!(serde_json::to_vec(&arguments).unwrap().len(), CALL_LIMIT);
        let message = ToDaemon::Call {
            id: 5,
            request_id: "5".to_owned(),
            arguments,
        };
        let bytes = frame_bytes(&message).await;
        let body = bytes.len() - 4;
        assert!(body > CALL_LIMIT, "{body} is not over {CALL_LIMIT}");
        assert!(body <= REQUEST_LIMIT, "{body} is over {REQUEST_LIMIT}");
        let mut reader = std::io::Cursor::new(bytes);
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, Some(message));
    }

    #[tokio::test]
    async fn a_measured_frame_is_the_frame_that_is_written() {
        for message in [hello(), ToDaemon::Bye, ToDaemon::Cancel { id: 3 }] {
            assert_eq!(
                frame_len(&message).unwrap(),
                encode_frame(&message).unwrap().len(),
                "{message:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_written_frame_is_the_encoded_frame() {
        let message = ToDaemon::Cancel { id: 9 };
        let (mut writer, mut reader) = tokio::io::duplex(1 << 16);
        let reading = async {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            bytes
        };
        let writing = async {
            write_frame(&mut writer, &message).await.unwrap();
            drop(writer);
        };
        let (sent, ()) = tokio::join!(reading, writing);
        assert_eq!(sent, encode_frame(&message).unwrap());
        let read: Option<ToDaemon> = read_frame(&mut std::io::Cursor::new(sent), REQUEST_LIMIT)
            .await
            .unwrap();
        assert_eq!(read, Some(message));
    }

    #[tokio::test]
    async fn frames_one_after_another_read_one_after_another() {
        let first = ToDaemon::Call {
            id: 1,
            request_id: "1".to_owned(),
            arguments: json!({"action": "list"}),
        };
        let second = ToDaemon::Cancel { id: 1 };
        let mut bytes = frame_bytes(&first).await;
        bytes.extend_from_slice(&frame_bytes(&second).await);
        let mut reader = std::io::Cursor::new(bytes);
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, Some(first));
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, Some(second));
        // Then the clean end: the frames stopped where the bytes did.
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, None);
    }

    #[tokio::test]
    async fn a_frame_read_one_byte_at_a_time_is_the_same_frame() {
        let message = hello();
        // A one-byte buffer hands the reader one byte per read, as a socket may.
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let reading = async {
            let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
            read
        };
        let writing = async {
            write_frame(&mut writer, &message).await.unwrap();
            drop(writer);
        };
        let (read, ()) = tokio::join!(reading, writing);
        assert_eq!(read, Some(message));
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, None);
    }

    #[tokio::test]
    async fn a_body_exactly_at_the_limit_reads() {
        let message = ToDaemon::Call {
            id: 4,
            request_id: "4".to_owned(),
            arguments: json!({"action": "list"}),
        };
        let bytes = frame_bytes(&message).await;
        let at_limit = bytes.len() - 4;
        let mut reader = std::io::Cursor::new(bytes);
        let read: Option<ToDaemon> = read_frame(&mut reader, at_limit).await.unwrap();
        assert_eq!(read, Some(message));
    }

    #[tokio::test]
    async fn a_clean_end_of_input_is_not_a_frame() {
        let mut reader = std::io::Cursor::new(Vec::new());
        let frame: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(frame, None);
    }

    #[tokio::test]
    async fn a_frame_that_ends_early_is_an_error() {
        // A length that was cut off.
        let mut reader = std::io::Cursor::new(vec![1, 0]);
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(
            error.to_string(),
            "the input ended after 2 of 4 length bytes"
        );

        // A body that was.
        let mut bytes = 8u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(br#"{"type""#);
        let mut reader = std::io::Cursor::new(bytes);
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "the frame ended after 7 of 8 bytes");
    }

    #[tokio::test]
    async fn a_length_over_the_limit_is_refused_without_reading_the_body() {
        // A gigabyte claimed, and nothing behind it: the length is refused, not trusted.
        let mut reader = std::io::Cursor::new((1u32 << 30).to_le_bytes().to_vec());
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "a frame of 1073741824 bytes is over the 4194304 byte limit"
        );
    }

    #[tokio::test]
    async fn a_body_that_is_not_json_is_an_error() {
        let mut bytes = 4u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(b"nope");
        let mut reader = std::io::Cursor::new(bytes);
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_text_name_is_a_string_on_the_wire() {
        let text = OsText::from(OsStr::new("PATH"));
        assert_eq!(serde_json::to_value(&text).unwrap(), json!("PATH"));
        assert_eq!(text.to_os_string().unwrap(), OsString::from("PATH"),);
    }

    #[cfg(windows)]
    #[test]
    fn a_name_that_is_not_unicode_keeps_its_units() {
        use std::os::windows::ffi::OsStringExt;
        // A lone high surrogate: no String holds it, and the environment can.
        let value = OsString::from_wide(&[b'a' as u16, 0xD800, b'b' as u16]);
        let text = OsText::from(value.as_os_str());
        let json = serde_json::to_value(&text).unwrap();
        assert_eq!(json, json!({"units": [97, 55296, 98]}));
        // Decoding is the half that can lose the units, so they come back through serde.
        let decoded: OsText = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, text);
        assert_eq!(decoded.to_os_string().unwrap(), value);
    }

    #[cfg(unix)]
    #[test]
    fn a_name_that_is_not_unicode_keeps_its_units() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let value = OsString::from_vec(vec![b'a', 0xFF, b'b']);
        let text = OsText::from(value.as_os_str());
        let json = serde_json::to_value(&text).unwrap();
        assert_eq!(json, json!({"units": [97, 255, 98]}));
        // Decoding is the half that can lose the bytes, so the units come back through serde.
        let decoded: OsText = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, text);
        assert_eq!(decoded.to_os_string().unwrap(), value);
        assert_eq!(value.as_bytes(), &[b'a', 0xFF, b'b']);
    }

    #[test]
    fn an_empty_name_reads_either_way_it_is_spelled() {
        for text in [
            OsText::Text(String::new()),
            OsText::Units { units: Vec::new() },
        ] {
            assert_eq!(text.to_os_string().unwrap(), OsString::new());
        }
    }

    #[test]
    fn a_launch_context_survives_the_wire() {
        let context = crate::session::LaunchContext::new(
            PathBuf::from("/work"),
            vec![
                (OsString::from("PATH"), OsString::from("/usr/bin")),
                (OsString::from("LANG"), OsString::from("C.UTF-8")),
            ],
        );
        let (cwd, env) = context.to_wire();
        let back = crate::session::LaunchContext::from_wire(&cwd, &env).unwrap();
        assert_eq!(back.cwd, context.cwd);
        assert_eq!(back.env, context.env);

        // An empty value is a value: the wire carries it and the context keeps it.
        let empty = [(
            OsText::Text("TERM".to_owned()),
            OsText::Units { units: Vec::new() },
        )];
        let back = crate::session::LaunchContext::from_wire(&cwd, &empty).unwrap();
        assert_eq!(back.env, [(OsString::from("TERM"), OsString::new())]);
    }

    #[test]
    fn an_output_mode_round_trips_by_name() {
        for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
            assert_eq!(OutputMode::parse(mode.name()).unwrap(), mode);
        }
        assert_eq!(OutputMode::Text.name(), "text");
        assert_eq!(OutputMode::Both.name(), "both");
        assert_eq!(OutputMode::Structured.name(), "structured");
        assert_eq!(
            OutputMode::parse("Text").unwrap_err(),
            "unknown output mode \"Text\""
        );
    }
}
