//! The local byte streams the daemon listens on and a client connects to: a named pipe whose
//! access is the current user's alone on Windows, a Unix socket elsewhere.
#![cfg_attr(not(test), allow(dead_code))]

use std::ffi::OsStr;
use std::io;

/// The server side of one accepted connection.
#[cfg(windows)]
pub type Conn = tokio::net::windows::named_pipe::NamedPipeServer;
/// The server side of one accepted connection.
#[cfg(unix)]
pub type Conn = tokio::net::UnixStream;

/// The client side of one connection.
#[cfg(windows)]
pub type Client = tokio::net::windows::named_pipe::NamedPipeClient;
/// The client side of one connection.
#[cfg(unix)]
pub type Client = tokio::net::UnixStream;

/// How long a client keeps retrying a pipe name every instance of which is busy.
#[cfg(windows)]
const CONNECT_WAIT: std::time::Duration = std::time::Duration::from_secs(2);
/// How long a client waits between attempts on a busy pipe.
#[cfg(windows)]
const CONNECT_PAUSE: std::time::Duration = std::time::Duration::from_millis(50);

/// A listening endpoint: the daemon's side of every connection it will accept.
pub struct Listener {
    #[cfg(windows)]
    name: std::ffi::OsString,
    #[cfg(windows)]
    attributes: SecurityAttributes,
    #[cfg(windows)]
    current: Conn,
    #[cfg(unix)]
    inner: tokio::net::UnixListener,
}

impl Listener {
    /// Listens on `name`: a pipe name on Windows, a socket path on Unix, whose parent directory
    /// the caller made private.
    pub fn bind(name: &OsStr) -> io::Result<Listener> {
        #[cfg(windows)]
        {
            let attributes = SecurityAttributes::owner_only(false)?;
            // Only the first instance creates the name, so a second listener on a live name
            // fails instead of joining it.
            let options = server_options(true);
            let current = attributes.with_raw(|raw| {
                // SAFETY: `raw` points at valid attributes for the length of this call.
                unsafe { options.create_with_security_attributes_raw(name, raw) }
            })?;
            Ok(Listener {
                name: name.to_os_string(),
                attributes,
                current,
            })
        }
        #[cfg(unix)]
        {
            Ok(Listener {
                inner: tokio::net::UnixListener::bind(name)?,
            })
        }
    }

    /// Waits for the next connection.
    pub async fn accept(&mut self) -> io::Result<Conn> {
        #[cfg(windows)]
        {
            self.current.connect().await?;
            // The next instance exists before the accepted one is handed out: a client that
            // connects the moment this returns finds a pipe to open rather than a busy name.
            let options = server_options(false);
            let next = self.attributes.with_raw(|raw| {
                // SAFETY: `raw` points at valid attributes for the length of this call.
                unsafe { options.create_with_security_attributes_raw(&self.name, raw) }
            })?;
            Ok(std::mem::replace(&mut self.current, next))
        }
        #[cfg(unix)]
        {
            let (stream, _address) = self.inner.accept().await?;
            Ok(stream)
        }
    }
}

/// Connects to the listener on `name`.
pub async fn connect(name: &OsStr) -> io::Result<Client> {
    #[cfg(windows)]
    {
        let deadline = std::time::Instant::now() + CONNECT_WAIT;
        loop {
            match tokio::net::windows::named_pipe::ClientOptions::new().open(name) {
                Ok(client) => return Ok(client),
                // Every instance is serving someone else: wait for one to come free.
                Err(error)
                    if error.raw_os_error()
                        == Some(windows_sys::Win32::Foundation::ERROR_PIPE_BUSY as i32)
                        && std::time::Instant::now() < deadline =>
                {
                    tokio::time::sleep(CONNECT_PAUSE).await;
                }
                Err(error) => return Err(error),
            }
        }
    }
    #[cfg(unix)]
    {
        tokio::net::UnixStream::connect(name).await
    }
}

#[cfg(windows)]
fn server_options(first: bool) -> tokio::net::windows::named_pipe::ServerOptions {
    let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
    // A pipe name is machine-wide, so it refuses remote clients: only this machine reaches it,
    // and the DACL below limits it to this user.
    options
        .first_pipe_instance(first)
        .reject_remote_clients(true);
    options
}

/// A security descriptor with a protected DACL that grants full access to the current user and to
/// nobody else. A pipe and a directory both use one, with different inheritance.
///
/// The descriptor is held as an address rather than a pointer, so a listener stays `Send`: it is
/// read only while an object is created, and this value owns it until it is freed.
#[cfg(windows)]
pub(crate) struct SecurityAttributes {
    descriptor: usize,
}

#[cfg(windows)]
impl SecurityAttributes {
    /// The owner-only descriptor. `inherit` also applies the ACE to children, which a directory
    /// needs and a pipe does not.
    pub(crate) fn owner_only(inherit: bool) -> io::Result<Self> {
        let ace = if inherit { "OICI" } else { "" };
        let sddl = format!("D:P(A;{ace};GA;;;{})", user_sid()?);
        let wide = wide(&sddl);
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: `wide` is NUL-terminated, and the call writes the descriptor it allocates.
        let converted = unsafe {
            windows_sys::Win32::Security::Authorization::ConvertStringSecurityDescriptorToSecurityDescriptorW(
                wide.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                std::ptr::null_mut(),
            )
        };
        if converted == 0 {
            return Err(io::Error::other(format!(
                "cannot build the security descriptor {sddl:?}: {}",
                io::Error::last_os_error()
            )));
        }
        Ok(Self {
            descriptor: descriptor as usize,
        })
    }

    /// Calls `create` with the raw attributes, which are valid for the length of the call.
    pub(crate) fn with_raw<T>(&self, create: impl FnOnce(*mut std::ffi::c_void) -> T) -> T {
        let mut raw = windows_sys::Win32::Security::SECURITY_ATTRIBUTES {
            nLength: std::mem::size_of::<windows_sys::Win32::Security::SECURITY_ATTRIBUTES>()
                as u32,
            lpSecurityDescriptor: self.descriptor
                as windows_sys::Win32::Security::PSECURITY_DESCRIPTOR,
            bInheritHandle: 0,
        };
        create((&mut raw as *mut windows_sys::Win32::Security::SECURITY_ATTRIBUTES).cast())
    }
}

#[cfg(windows)]
impl Drop for SecurityAttributes {
    fn drop(&mut self) {
        // SAFETY: the descriptor came from the SDDL conversion above and is freed once.
        unsafe {
            windows_sys::Win32::Foundation::LocalFree(
                self.descriptor as windows_sys::Win32::Foundation::HLOCAL,
            );
        }
    }
}

#[cfg(windows)]
const SDDL_REVISION_1: u32 = 1;

/// The current user's SID as `S-1-5-...`, which is what the pipe's DACL names.
#[cfg(windows)]
pub fn user_sid() -> io::Result<String> {
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::Security::TOKEN_QUERY;
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    let mut token: HANDLE = std::ptr::null_mut();
    // SAFETY: the call opens this process's token for query, and the handle is closed below.
    let opened = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) };
    if opened == 0 {
        return Err(io::Error::last_os_error());
    }
    let sid = token_user_sid(token);
    // SAFETY: the handle came from OpenProcessToken and is closed once.
    unsafe { CloseHandle(token) };
    sid
}

#[cfg(windows)]
fn token_user_sid(token: windows_sys::Win32::Foundation::HANDLE) -> io::Result<String> {
    use windows_sys::Win32::Foundation::{HLOCAL, LocalFree};
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_USER, TokenUser};

    let mut needed = 0u32;
    // SAFETY: a null buffer with no length is the documented size query; its error is expected.
    unsafe { GetTokenInformation(token, TokenUser, std::ptr::null_mut(), 0, &mut needed) };
    if needed == 0 {
        return Err(io::Error::last_os_error());
    }
    // The buffer is word-sized, so the TOKEN_USER the call writes into it is aligned.
    let mut buffer = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
    let length = (buffer.len() * std::mem::size_of::<usize>()) as u32;
    // SAFETY: the buffer holds `length` bytes, and the call writes at most `needed` of them.
    let filled = unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast::<std::ffi::c_void>(),
            length,
            &mut needed,
        )
    };
    if filled == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the call filled the buffer with a TOKEN_USER whose SID points inside it.
    let user: &TOKEN_USER = unsafe { &*buffer.as_ptr().cast::<TOKEN_USER>() };
    let mut text: *mut u16 = std::ptr::null_mut();
    // SAFETY: `user.User.Sid` is a valid SID, and the string it returns is freed below.
    let converted = unsafe { ConvertSidToStringSidW(user.User.Sid, &mut text) };
    if converted == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `text` is a NUL-terminated string the call allocated.
    let sid = unsafe { wide_to_string(text) };
    // SAFETY: the string came from ConvertSidToStringSidW and is freed once.
    unsafe { LocalFree(text as HLOCAL) };
    Ok(sid)
}

#[cfg(windows)]
fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Reads a NUL-terminated UTF-16 string.
///
/// # Safety
///
/// `text` must point at a NUL-terminated UTF-16 string.
#[cfg(windows)]
unsafe fn wide_to_string(text: *const u16) -> String {
    let mut length = 0;
    // SAFETY: the caller promises a terminated string, so this stops at its terminator.
    while unsafe { *text.add(length) } != 0 {
        length += 1;
    }
    // SAFETY: the string is `length` units long and the loop above reached its terminator.
    String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, length) })
}

/// A unique endpoint for one test: a pipe name, or a socket in a private temporary directory.
#[cfg(test)]
pub(crate) struct Endpoint {
    name: std::ffi::OsString,
    #[cfg(unix)]
    dir: std::path::PathBuf,
}

#[cfg(test)]
impl Endpoint {
    pub(crate) fn new(tag: &str) -> Endpoint {
        let unique = format!("fastexec-test-{}-{tag}", std::process::id());
        #[cfg(windows)]
        {
            Endpoint {
                name: format!(r"\\.\pipe\{unique}").into(),
            }
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let dir = std::env::temp_dir().join(&unique);
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir(&dir).unwrap();
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
            let name = dir.join("socket").into_os_string();
            Endpoint { name, dir }
        }
    }

    pub(crate) fn name(&self) -> &OsStr {
        &self.name
    }
}

#[cfg(all(test, unix))]
impl Drop for Endpoint {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::time::timeout;

    /// Every scenario here is bounded, so a transport regression fails instead of hanging Cargo.
    const EXCHANGE_WAIT: Duration = Duration::from_secs(30);

    #[tokio::test]
    async fn three_connections_are_served_from_one_listener() {
        timeout(EXCHANGE_WAIT, async {
            let endpoint = Endpoint::new("three");
            let mut listener = Listener::bind(endpoint.name()).unwrap();
            let mut clients = Vec::new();
            for _ in 0..3 {
                let name = endpoint.name().to_owned();
                clients.push(tokio::spawn(async move {
                    let mut client = connect(&name).await.unwrap();
                    client.write_all(b"ping").await.unwrap();
                    let mut answer = [0u8; 4];
                    client.read_exact(&mut answer).await.unwrap();
                    assert_eq!(&answer, b"pong");
                }));
            }
            let mut servers = Vec::new();
            for _ in 0..3 {
                let mut conn = listener.accept().await.unwrap();
                servers.push(tokio::spawn(async move {
                    let mut greeting = [0u8; 4];
                    conn.read_exact(&mut greeting).await.unwrap();
                    conn.write_all(b"pong").await.unwrap();
                }));
            }
            for client in clients {
                client.await.unwrap();
            }
            for server in servers {
                server.await.unwrap();
            }
        })
        .await
        .expect("three connections were served");
    }

    /// The next instance exists before an accept returns: a client that connects after the
    /// previous connection ended is taken without the daemon accepting anything in between.
    #[tokio::test]
    async fn a_second_client_connects_before_the_next_accept() {
        timeout(EXCHANGE_WAIT, async {
            let endpoint = Endpoint::new("ready");
            let mut listener = Listener::bind(endpoint.name()).unwrap();
            let first = connect(endpoint.name()).await.unwrap();
            let served = listener.accept().await.unwrap();
            drop(first);
            drop(served);

            // Nothing has called accept since, and the name still takes a connection.
            let mut second = connect(endpoint.name()).await.unwrap();
            second.write_all(b"ping").await.unwrap();
            let mut served = listener.accept().await.unwrap();
            let mut greeting = [0u8; 4];
            served.read_exact(&mut greeting).await.unwrap();
            assert_eq!(&greeting, b"ping");
        })
        .await
        .expect("the name took a second client before the next accept");
    }

    /// A second listener on a live pipe name fails, and the DACL names this user alone.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_pipe_name_belongs_to_one_listener_and_to_one_user() {
        let endpoint = Endpoint::new("bind");
        let _listener = Listener::bind(endpoint.name()).unwrap();
        assert!(
            Listener::bind(endpoint.name()).is_err(),
            "a second listener took a live pipe name"
        );
        // S-1-5-21-... for a local account, S-1-12-1-... for an Entra one.
        let sid = user_sid().unwrap();
        let parts: Vec<&str> = sid.split('-').collect();
        assert!(parts.len() >= 3, "{sid}");
        assert_eq!((parts[0], parts[1]), ("S", "1"), "{sid}");
        assert!(
            parts[2..]
                .iter()
                .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit())),
            "{sid}"
        );
    }
}
