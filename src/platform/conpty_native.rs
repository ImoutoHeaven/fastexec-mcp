//! Native Windows pseudoconsole handles, process attributes, and exit-status waits.
//!
//! The ConPTY export lookup follows portable-pty 0.9's Windows implementation
//! (https://github.com/wez/wezterm), Copyright 2018 Wez Furlong, licensed under the
//! MIT license in `vendor/conpty/LICENSE`; see NOTICE.

use std::ffi::OsStr;
use std::io::Read;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, OwnedHandle, RawHandle};
use std::time::{Duration, Instant};
use windows_sys::Win32::Foundation::{HANDLE, S_OK, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Console::{COORD, HPCON};
use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
use windows_sys::Win32::System::Threading::{
    DeleteProcThreadAttributeList, GetExitCodeProcess, INFINITE, InitializeProcThreadAttributeList,
    LPPROC_THREAD_ATTRIBUTE_LIST, PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, TerminateProcess,
    UpdateProcThreadAttribute, WaitForSingleObject,
};

/// `PSEUDOCONSOLE_INHERIT_CURSOR`, `PSEUDOCONSOLE_RESIZE_QUIRK` and
/// `PSEUDOCONSOLE_WIN32_INPUT_MODE`: what portable-pty passed, so ConPTY's startup cursor query
/// and input encoding stay as they were.
const CONPTY_FLAGS: u32 = 0x1 | 0x2 | 0x4;

type CreatePseudoConsoleFn =
    unsafe extern "system" fn(COORD, HANDLE, HANDLE, u32, *mut HPCON) -> windows_sys::core::HRESULT;
type ClosePseudoConsoleFn = unsafe extern "system" fn(HPCON);
type ResizePseudoConsoleFn = unsafe extern "system" fn(HPCON, COORD) -> windows_sys::core::HRESULT;

/// The ConPTY entry points: from the verified bundled module's exact handle, else from the
/// already loaded system kernel. Create, resize, and close come from one module: a console
/// made by one module is resized and closed only through that module.
pub(super) struct ConPty {
    create: CreatePseudoConsoleFn,
    resize: ResizePseudoConsoleFn,
    close: ClosePseudoConsoleFn,
}

impl ConPty {
    pub(super) fn load() -> std::io::Result<Self> {
        let kernel = module("kernel32.dll")?;
        if resolve(kernel).is_none() {
            return Err(std::io::Error::other(
                "this Windows has no ConPTY; Windows 10 version 1809 or newer is required",
            ));
        }
        // Only the module loaded from the verified bundled image may override the system
        // API. A failed extraction must never search CWD or PATH for another conpty.dll.
        let bundled = crate::platform::conpty::module().and_then(resolve);
        let (create, resize, close) = match bundled.or_else(|| resolve(kernel)) {
            Some(triple) => triple,
            None => {
                return Err(std::io::Error::other(
                    "the ConPTY exports cannot be resolved",
                ));
            }
        };
        Ok(Self {
            // SAFETY: the three come from one module and have the signatures the console API
            // declares.
            create: unsafe {
                std::mem::transmute::<unsafe extern "system" fn() -> isize, CreatePseudoConsoleFn>(
                    create,
                )
            },
            resize: unsafe {
                std::mem::transmute::<unsafe extern "system" fn() -> isize, ResizePseudoConsoleFn>(
                    resize,
                )
            },
            close: unsafe {
                std::mem::transmute::<unsafe extern "system" fn() -> isize, ClosePseudoConsoleFn>(
                    close,
                )
            },
        })
    }

    /// Creates the console over the ends it reads input from and writes output to.
    pub(super) fn create(
        &self,
        input: RawHandle,
        output: RawHandle,
        columns: u16,
        rows: u16,
    ) -> std::io::Result<Terminal> {
        let size = COORD {
            X: columns as i16,
            Y: rows as i16,
        };
        let mut con: HPCON = 0;
        // SAFETY: both handles stay open for the call, and `con` outlives it.
        let result =
            unsafe { (self.create)(size, input.cast(), output.cast(), CONPTY_FLAGS, &mut con) };
        if result != S_OK {
            return Err(std::io::Error::other(format!(
                "CreatePseudoConsole failed: HRESULT {result:#x}"
            )));
        }
        Ok(Terminal {
            con,
            resize: self.resize,
            close: self.close,
        })
    }
}

/// Returns a module already loaded by the process. The system kernel is a KnownDLL loaded
/// at process startup; optional DLLs must never be loaded by a bare name from CWD or PATH.
fn module(name: &str) -> std::io::Result<HANDLE> {
    let wide: Vec<u16> = OsStr::new(name).encode_wide().chain([0]).collect();
    // SAFETY: `wide` is a NUL-terminated module name.
    let loaded = unsafe { GetModuleHandleW(wide.as_ptr()) };
    if loaded.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    Ok(loaded)
}

/// The console's create, resize, and close, taken from one module or not at all.
type Export = unsafe extern "system" fn() -> isize;

fn resolve(from: HANDLE) -> Option<(Export, Export, Export)> {
    Some((
        export(from, c"CreatePseudoConsole")?,
        export(from, c"ResizePseudoConsole")?,
        export(from, c"ClosePseudoConsole")?,
    ))
}

fn export(module: HANDLE, name: &std::ffi::CStr) -> Option<Export> {
    // SAFETY: the module is loaded and the name is NUL-terminated.
    unsafe { GetProcAddress(module, name.as_ptr().cast()) }
}

/// A console. Closing it can block while its host finishes, so the ender thread that drops the
/// task's child is what drops this, as the portable-pty master handle was dropped before.
pub struct Terminal {
    con: HPCON,
    resize: ResizePseudoConsoleFn,
    close: ClosePseudoConsoleFn,
}

impl Terminal {
    /// The console is `columns` by `rows` from now on. ConPTY repaints the whole screen into the
    /// output pipe, which keeps draining on the reading thread while this call runs, so it is
    /// called with no lock capture needs held.
    pub(crate) fn resize(&self, columns: u16, rows: u16) -> std::io::Result<()> {
        let size = COORD {
            X: columns as i16,
            Y: rows as i16,
        };
        // SAFETY: the console was made by the module this function came from, and `size` is a
        // plain value.
        let result = unsafe { (self.resize)(self.con, size) };
        if result != S_OK {
            return Err(std::io::Error::other(format!(
                "ResizePseudoConsole failed: HRESULT {result:#x}"
            )));
        }
        Ok(())
    }
}

impl Drop for Terminal {
    fn drop(&mut self) {
        // SAFETY: the console was created by this module and is closed once.
        unsafe { (self.close)(self.con) }
    }
}

/// How often the wait looks at the script's status file and at the root.
const STATUS_SLICE: Duration = Duration::from_millis(100);
/// How long a root may stay alive after its script finished before it is ended for the task.
const END_GRACE: Duration = Duration::from_secs(5);

/// Blocks until the root bash exits and returns the code it reports, and whether the end was
/// forced because its script had finished while the root itself never terminated.
///
/// A root can deadlock inside its own exit (msys-2.0.dll blocks in a handle close under a
/// pseudoconsole), and then the process never terminates and its task would never end. The
/// script's EXIT trap writes its status to `status`, so the file appearing means the script
/// finished: from there the root gets [`END_GRACE`] to exit on its own before it is terminated.
/// A native call that fails asks for no verdict at all: the plain wait then reports whatever the
/// root itself does.
pub(crate) fn wait(process: &OwnedHandle, status: Option<&std::path::Path>) -> (i32, bool) {
    let Some(status) = status else {
        return (wait_forever(process), false);
    };
    let mut seen: Option<Instant> = None;
    loop {
        // SAFETY: the handle is owned by the child and alive for the call.
        let waited = unsafe {
            WaitForSingleObject(process.as_raw_handle(), STATUS_SLICE.as_millis() as u32)
        };
        match waited {
            WAIT_OBJECT_0 => return (exit_code(process), false),
            WAIT_TIMEOUT => {}
            // A failing wait proves nothing, so the plain wait reports the root's own outcome.
            _ => return (wait_forever(process), false),
        }
        if seen.is_none() && status.exists() {
            seen = Some(Instant::now());
        }
        let Some(since) = seen else {
            continue;
        };
        if since.elapsed() < END_GRACE {
            continue;
        }
        // A file that never holds a complete record still ends the task, with status 1.
        let code = script_status(status).unwrap_or(1);
        // SAFETY: the handle is a live process of this call.
        if unsafe { TerminateProcess(process.as_raw_handle(), code as u32) } == 0 {
            // The force did not land, so nothing is claimed about the end.
            return (wait_forever(process), false);
        }
        // SAFETY: as above; the root is now ending with the code the call asked for.
        if unsafe { WaitForSingleObject(process.as_raw_handle(), END_GRACE.as_millis() as u32) }
            != WAIT_OBJECT_0
        {
            return (wait_forever(process), false);
        }
        return match exit_code_status(process) {
            // Only a force that landed and reported the script's own status is a forced end.
            Some(actual) if actual == code => (code, true),
            Some(actual) => (actual, false),
            None => (wait_forever(process), false),
        };
    }
}

/// The script's exit status, once its trap has written a complete record: a newline-terminated
/// decimal in 0..=255. A file caught mid-write, or holding anything else, is not a record yet.
fn script_status(path: &std::path::Path) -> Option<i32> {
    let mut bytes = [0_u8; 8];
    let mut file = std::fs::File::open(path).ok()?;
    let mut read = 0;
    while read < bytes.len() {
        match file.read(&mut bytes[read..]) {
            Ok(0) => break,
            Ok(count) => read += count,
            Err(_) => return None,
        }
    }
    // A record is at most four bytes; a full buffer holds trailing junk, not a status.
    if read == bytes.len() {
        return None;
    }
    let text = std::str::from_utf8(&bytes[..read]).ok()?;
    text.strip_suffix('\n')?.parse::<u8>().ok().map(i32::from)
}

/// Blocks until the root bash exits, whatever it takes.
fn wait_forever(process: &OwnedHandle) -> i32 {
    // SAFETY: the handle is owned by the child and alive for the call.
    let waited = unsafe { WaitForSingleObject(process.as_raw_handle(), INFINITE) };
    if waited != WAIT_OBJECT_0 {
        return 1;
    }
    exit_code(process)
}

/// The root's exit code, or `1` when the OS would not report one.
fn exit_code(process: &OwnedHandle) -> i32 {
    exit_code_status(process).unwrap_or(1)
}

/// The root's exit code, or `None` when the OS would not report one.
fn exit_code_status(process: &OwnedHandle) -> Option<i32> {
    // SAFETY: the handle is owned by the child and alive for the call.
    unsafe {
        let mut code = 0;
        if GetExitCodeProcess(process.as_raw_handle(), &mut code) == 0 {
            return None;
        }
        // `Child::wait` reads the msys signal convention into this code; it is not applied twice.
        Some(code as i32)
    }
}

/// The thread attributes holding the pseudoconsole, deleted when they go out of scope. The
/// storage is `usize`-aligned, as the opaque list requires.
pub(super) struct Attributes {
    data: Vec<usize>,
}

impl Attributes {
    pub(super) fn new(count: u32) -> std::io::Result<Self> {
        let mut bytes: usize = 0;
        // SAFETY: a null list is how the required size is asked for; the call reports it through
        // `bytes` and fails, which is expected here.
        unsafe {
            InitializeProcThreadAttributeList(std::ptr::null_mut(), count, 0, &mut bytes);
        }
        if bytes == 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut data = vec![0_usize; bytes.div_ceil(std::mem::size_of::<usize>())];
        // SAFETY: the storage holds at least the required number of bytes.
        if unsafe {
            InitializeProcThreadAttributeList(data.as_mut_ptr().cast(), count, 0, &mut bytes)
        } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { data })
    }

    pub(super) fn as_mut(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.data.as_mut_ptr().cast()
    }

    /// Hands the console to the process about to be created.
    pub(super) fn set_console(&mut self, terminal: &Terminal) -> std::io::Result<()> {
        // SAFETY: the list is initialized, and the value is the console itself, not its address.
        let set = unsafe {
            UpdateProcThreadAttribute(
                self.as_mut(),
                0,
                PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
                terminal.con as *const std::ffi::c_void,
                std::mem::size_of::<HPCON>(),
                std::ptr::null_mut(),
                std::ptr::null(),
            )
        };
        if set == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        // SAFETY: `new` initialized the list and this is its only deletion.
        unsafe { DeleteProcThreadAttributeList(self.as_mut()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn module_lookup_never_loads_an_unregistered_dll() {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-module-test-{}-{}",
            std::process::id(),
            crate::id::random_chars(12).unwrap()
        ));
        std::fs::create_dir(&dir).unwrap();
        let source = std::path::PathBuf::from(std::env::var_os("SystemRoot").unwrap())
            .join("System32")
            .join("version.dll");
        let target = dir.join("fastexec-untrusted-probe.dll");
        std::fs::copy(source, &target).unwrap();
        let refused = module(target.to_str().unwrap()).is_err();
        let _ = std::fs::remove_dir_all(dir);
        assert!(refused, "an optional DLL was loaded just by looking it up");
        assert!(
            module("kernel32.dll").is_ok(),
            "the system kernel is already loaded"
        );
    }

    /// What the trap's file can hold at the moment the wait looks at it.
    #[test]
    fn script_status_reads_only_a_complete_record() {
        let dir =
            std::env::temp_dir().join(format!("fastexec-script-status-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("a scratch directory");
        let cases = [
            ("0\n", Some(0)),
            ("7\n", Some(7)),
            ("255\n", Some(255)),
            ("256\n", None),
            ("-1\n", None),
            ("\n", None),
            ("7", None),
            ("7\n8\n", None),
            ("0000000\n7\n", None),
            ("x\n", None),
            ("", None),
        ];
        for (index, (contents, expected)) in cases.iter().enumerate() {
            let path = dir.join(format!("{index}.exit"));
            std::fs::write(&path, contents).expect("the case file");
            assert_eq!(script_status(&path), *expected, "{contents:?}");
        }
        assert_eq!(script_status(&dir.join("absent.exit")), None);
        std::fs::remove_dir_all(&dir).expect("the scratch directory to go");
    }
}
