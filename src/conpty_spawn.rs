//! Windows PTY launch: the console first, then bash created suspended inside it and put in the
//! task's Job, and only then resumed.
//!
//! A process created without `CREATE_SUSPENDED` runs while the `CreateProcessW` call is still
//! returning; bash could start a child before it joined the Job, and that child would escape the
//! task's tree. Creating it suspended and resuming it after `Tree::adopt` closes that window: no
//! instruction of the root runs before the Job owns it, and every process it starts inherits it.
//!
//! The ConPTY export lookup and the command-line quoting follow portable-pty 0.9's Windows
//! implementation (https://github.com/wez/wezterm, `src/cmdbuilder.rs`), Copyright 2018 Wez
//! Furlong, licensed under the MIT license in `vendor/conpty/LICENSE`; see NOTICE.

use crate::process::{Child, Launch, Spawned, Tree, bash_args, environment};
use std::cmp::Ordering;
use std::ffi::{OsStr, OsString};
use std::io::{Read, Write};
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::time::{Duration, Instant};
use windows_sys::Win32::Foundation::{
    HANDLE, INVALID_HANDLE_VALUE, S_OK, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Globalization::{
    CSTR_EQUAL, CSTR_GREATER_THAN, CSTR_LESS_THAN, CompareStringOrdinal,
};
use windows_sys::Win32::System::Console::{COORD, HPCON};
use windows_sys::Win32::System::LibraryLoader::{GetModuleHandleW, GetProcAddress};
use windows_sys::Win32::System::Threading::{
    CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW, DeleteProcThreadAttributeList,
    EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, INFINITE, InitializeProcThreadAttributeList,
    LPPROC_THREAD_ATTRIBUTE_LIST, PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE, PROCESS_INFORMATION,
    ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW, TerminateProcess,
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
struct ConPty {
    create: CreatePseudoConsoleFn,
    resize: ResizePseudoConsoleFn,
    close: ClosePseudoConsoleFn,
}

impl ConPty {
    fn load() -> std::io::Result<Self> {
        let kernel = module("kernel32.dll")?;
        if resolve(kernel).is_none() {
            return Err(std::io::Error::other(
                "this Windows has no ConPTY; Windows 10 version 1809 or newer is required",
            ));
        }
        // Only the module loaded from the verified bundled image may override the system
        // API. A failed extraction must never search CWD or PATH for another conpty.dll.
        let bundled = crate::conpty::module().and_then(resolve);
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
    fn create(
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

/// A root bash created suspended inside its console and already in the task's Job.
struct Suspended {
    process: OwnedHandle,
    thread: OwnedHandle,
    tree: Tree,
    terminal: Terminal,
    output: Box<dyn Read + Send>,
    input: Box<dyn Write + Send>,
}

pub fn spawn_pty(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    let suspended = create_in_job(launch)?;
    if let Err(error) = resume(&suspended.thread) {
        let _ = suspended.tree.kill();
        end(&suspended.process);
        return Err(error);
    }
    let terminal = std::sync::Arc::new(suspended.terminal);
    Ok(Spawned {
        child: Child::Pty {
            process: suspended.process,
            terminal: std::sync::Arc::clone(&terminal),
            exit_status: launch.exit_status.map(std::path::Path::to_path_buf),
        },
        output: suspended.output,
        input: suspended.input,
        tree: suspended.tree,
        pty: Some(crate::process::PtyControl::new(terminal)),
    })
}

/// Creates bash suspended inside its console and puts it in the task's Job. The root runs no
/// instruction until [`resume`] is called on it.
fn create_in_job(launch: &Launch<'_>) -> std::io::Result<Suspended> {
    let api = ConPty::load()?;
    let (input_reader, input_writer) = std::io::pipe()?;
    let (output_reader, output_writer) = std::io::pipe()?;
    let terminal = api.create(
        input_reader.as_raw_handle(),
        output_writer.as_raw_handle(),
        launch.cols,
        launch.rows,
    )?;
    // The console keeps its own copies of the two ends it was handed.
    drop((input_reader, output_writer));
    let app = nul_terminated(launch.bash.as_os_str())?;
    let mut command = command_line(launch)?;
    let block = environment_block(launch)?;
    let cwd = nul_terminated(launch.cwd.as_os_str())?;
    let mut attributes = Attributes::new(1)?;
    attributes.set_console(terminal.con)?;
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
    // The console is the child's stdio. Handles inherited in their place would leak through as
    // if they were, which portable-pty guarded against the same way.
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
    startup.lpAttributeList = attributes.as_mut();
    let mut info = PROCESS_INFORMATION::default();
    // SAFETY: every pointer refers to storage that outlives the call: the NUL-terminated app and
    // cwd, the mutable command line, the environment block, and the attribute list, which keeps
    // the console live as well.
    let created = unsafe {
        CreateProcessW(
            app.as_ptr(),
            command.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_SUSPENDED,
            block.as_ptr().cast(),
            cwd.as_ptr(),
            &startup.StartupInfo,
            &mut info,
        )
    };
    if created == 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful CreateProcessW returns two handles, owned here from now on.
    let (process, thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(info.hProcess),
            OwnedHandle::from_raw_handle(info.hThread),
        )
    };
    let tree = match Tree::adopt(process.as_raw_handle()) {
        Ok(tree) => tree,
        Err(error) => {
            end(&process);
            return Err(error);
        }
    };
    Ok(Suspended {
        process,
        thread,
        tree,
        terminal,
        output: Box::new(output_reader),
        input: Box::new(input_writer),
    })
}

/// Ends a root that never became a task and waits for it, so a failed start leaves no process
/// terminating behind it after its handles are released.
fn end(process: &OwnedHandle) {
    // SAFETY: the handle is a live process of this call.
    unsafe {
        TerminateProcess(process.as_raw_handle(), 1);
        WaitForSingleObject(process.as_raw_handle(), INFINITE);
    }
}

fn resume(thread: &OwnedHandle) -> std::io::Result<()> {
    // SAFETY: the thread handle is the suspended root's own and alive.
    if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
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
struct Attributes {
    data: Vec<usize>,
}

impl Attributes {
    fn new(count: u32) -> std::io::Result<Self> {
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

    fn as_mut(&mut self) -> LPPROC_THREAD_ATTRIBUTE_LIST {
        self.data.as_mut_ptr().cast()
    }

    /// Hands the console to the process about to be created.
    fn set_console(&mut self, con: HPCON) -> std::io::Result<()> {
        // SAFETY: the list is initialized, and the value is the console itself, not its address.
        let set = unsafe {
            UpdateProcThreadAttribute(
                self.as_mut(),
                0,
                PROC_THREAD_ATTRIBUTE_PSEUDOCONSOLE as usize,
                con as *const std::ffi::c_void,
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

/// bash and its arguments as one `CreateProcessW` command line.
fn command_line(launch: &Launch<'_>) -> std::io::Result<Vec<u16>> {
    command_line_of(launch.bash.as_os_str(), &bash_args(launch))
}

pub(crate) fn command_line_of(program: &OsStr, args: &[OsString]) -> std::io::Result<Vec<u16>> {
    let mut line = Vec::new();
    append_quoted(program, &mut line)?;
    for arg in args {
        line.push(' ' as u16);
        append_quoted(arg, &mut line)?;
    }
    line.push(0);
    Ok(line)
}

/// Appends one argument quoted by the rules the Microsoft C runtime parses: a run of backslashes
/// is literal before an ordinary unit, doubled before a quote, and doubled before the closing
/// quote; a quote itself is escaped by one more backslash.
fn append_quoted(arg: &OsStr, line: &mut Vec<u16>) -> std::io::Result<()> {
    const BACKSLASH: u16 = b'\\' as u16;
    const QUOTE: u16 = b'"' as u16;
    let arg = units(arg)?;
    let plain = !arg.is_empty()
        && !arg
            .iter()
            .any(|&unit| matches!(unit, 32 | 9 | 10 | 11) || unit == QUOTE);
    if plain {
        line.extend_from_slice(&arg);
        return Ok(());
    }
    line.push(QUOTE);
    let mut index = 0;
    while index < arg.len() {
        let run = arg[index..]
            .iter()
            .take_while(|&&unit| unit == BACKSLASH)
            .count();
        index += run;
        if index == arg.len() {
            // Trailing backslashes sit before the closing quote, so they are doubled.
            line.extend(std::iter::repeat_n(BACKSLASH, run * 2));
            break;
        }
        let escape = arg[index] == QUOTE;
        let written = if escape { run * 2 + 1 } else { run };
        line.extend(std::iter::repeat_n(BACKSLASH, written));
        line.push(arg[index]);
        index += 1;
    }
    line.push(QUOTE);
    Ok(())
}

/// The session's environment, then this launch's values over it, as `CreateProcessW` wants them:
/// names sorted in ordinal ignore-case order, each `name=value` NUL-terminated, and the block
/// NUL-terminated.
fn environment_block(launch: &Launch<'_>) -> std::io::Result<Vec<u16>> {
    let overrides = environment(launch);
    let pairs = launch
        .env
        .env
        .iter()
        .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
        .chain(
            overrides
                .iter()
                .map(|(name, value)| (OsStr::new(*name), value.as_os_str())),
        );
    block(pairs)
}

/// Builds the sorted environment block from `(name, value)` pairs. Windows compares names without
/// regard to case, but msys bash imports the spelling it is given into its own case-sensitive
/// environment, so a later pair of the same name replaces the whole entry, spelling included: a
/// forced `LANG` reaches bash as `LANG`, never as the caller's `lang`.
fn block<'a>(pairs: impl Iterator<Item = (&'a OsStr, &'a OsStr)>) -> std::io::Result<Vec<u16>> {
    // (name, value)
    let mut entries: Vec<(Vec<u16>, Vec<u16>)> = Vec::new();
    for (name, value) in pairs {
        let name = units(name)?;
        check_name(&name)?;
        let value = units(value)?;
        match entries
            .iter_mut()
            .find(|(existing, _)| same_name(existing, &name))
        {
            Some(entry) => *entry = (name, value),
            None => entries.push((name, value)),
        }
    }
    entries.sort_by(|left, right| compare_names(&left.0, &right.0));
    let mut block = Vec::new();
    for (name, value) in entries {
        block.extend_from_slice(&name);
        block.push(b'=' as u16);
        block.extend_from_slice(&value);
        block.push(0);
    }
    // An empty block is two terminators, as a full one ends with one.
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

/// A name is `name=value`, so it cannot be empty and cannot hold `=` after the first unit. A
/// leading `=` is a name of its own, as in the per-drive current directory `=C:` other Windows
/// entries use, and `std`'s own environment reader keeps such a name.
fn check_name(name: &[u16]) -> std::io::Result<()> {
    // A lone `=` is no name at all; every other leading `=` starts one.
    let valid = !name.is_empty()
        && !name[1..].contains(&(b'=' as u16))
        && (name[0] != b'=' as u16 || name.len() > 1);
    if valid {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "an environment name cannot be {:?}",
        String::from_utf16_lossy(name)
    )))
}

/// Whether two names are the same name, which Windows decides without regard to case.
fn same_name(left: &[u16], right: &[u16]) -> bool {
    matches!(compare(left, right), Some(Ordering::Equal))
}

/// The order `CreateProcessW` wants environment names in: ordinal, without regard to case, on the
/// UTF-16 units themselves, so an unpaired surrogate is a unit like any other. Units the system
/// calls equal then fall back to their exact order, which keeps the sort total.
fn compare_names(left: &[u16], right: &[u16]) -> Ordering {
    match compare(left, right) {
        Some(order) => order.then_with(|| left.cmp(right)),
        None => left.cmp(right),
    }
}

/// The system's ordinal comparison of two names, or `None` when it gives no answer.
fn compare(left: &[u16], right: &[u16]) -> Option<Ordering> {
    let (left_len, right_len) = (
        i32::try_from(left.len()).ok()?,
        i32::try_from(right.len()).ok()?,
    );
    // SAFETY: both slices are alive for the call and their lengths are given explicitly, so
    // neither needs a terminator.
    let result =
        unsafe { CompareStringOrdinal(left.as_ptr(), left_len, right.as_ptr(), right_len, 1) };
    match result {
        CSTR_LESS_THAN => Some(Ordering::Less),
        CSTR_GREATER_THAN => Some(Ordering::Greater),
        CSTR_EQUAL => Some(Ordering::Equal),
        // A failure is not an equality: exact order keeps distinct names apart.
        _ => None,
    }
}

/// A value's UTF-16 units. An unpaired surrogate survives, which is why the wide form is used.
fn units(value: &OsStr) -> std::io::Result<Vec<u16>> {
    let units: Vec<u16> = value.encode_wide().collect();
    if units.contains(&0) {
        return Err(std::io::Error::other(format!(
            "a NUL cannot appear in {value:?}"
        )));
    }
    Ok(units)
}

fn nul_terminated(value: &OsStr) -> std::io::Result<Vec<u16>> {
    let mut units = units(value)?;
    units.push(0);
    Ok(units)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::LaunchContext;

    #[test]
    fn module_lookup_never_loads_an_unregistered_dll() {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-module-test-{}-{}",
            std::process::id(),
            crate::tasks::random_chars(12).unwrap()
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

    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::System::Threading::{ResumeThread, SuspendThread};
    use windows_sys::Win32::UI::Shell::CommandLineToArgvW;

    /// The arguments Windows' own splitter reads from a command line, the way a child's `argv`
    /// comes out of one. The first token is the program, which is dropped.
    fn argv(line: &[u16]) -> Vec<OsString> {
        let mut full = line.to_vec();
        full.push(0);
        let mut count = 0;
        // SAFETY: the line is NUL-terminated and outlives the call; the array is freed below.
        unsafe {
            let array = CommandLineToArgvW(full.as_ptr(), &mut count);
            assert!(!array.is_null(), "the command line cannot be parsed");
            let args = (0..count as usize)
                .skip(1)
                .map(|index| {
                    let entry = *array.add(index);
                    let mut length = 0;
                    while *entry.add(length) != 0 {
                        length += 1;
                    }
                    OsString::from_wide(std::slice::from_raw_parts(entry, length))
                })
                .collect();
            windows_sys::Win32::Foundation::LocalFree(array as *mut std::ffi::c_void);
            args
        }
    }

    /// Each argument reaches a child whole: the quoting the C runtime parses survives an empty
    /// argument, spaces, tabs, newlines, quotes, and backslashes that are ordinary, quoted, or
    /// trailing.
    #[test]
    fn every_argument_reaches_a_child_whole() {
        let cases = [
            "",
            "plain",
            "two words",
            "tab\there",
            "line\nbreak",
            "say \"hi\"",
            // Without quotes a backslash needs nothing; inside them an ordinary one stays one.
            "back\\slash",
            "a b\\c",
            "a\\b c\\d",
            // One backslash before a quote doubles, plus one: `\"` reads back as `"`.
            "a\\\"b",
            // Trailing backslashes sit before the closing quote, so each is doubled.
            "space then\\",
            "space then\\\\",
            "\\",
            "\\\\",
        ];
        for case in cases {
            let args: Vec<OsString> = vec![case.into()];
            let line = command_line_of(OsStr::new("bash"), &args).expect("a command line");
            // The trailing NUL is the terminator, not part of the line to parse.
            assert_eq!(argv(&line[..line.len() - 1]), args, "{case:?}");
        }
    }

    /// The same, for several arguments in one line, where a missing separator would show.
    #[test]
    fn several_arguments_keep_their_boundaries() {
        let args: Vec<OsString> = vec![
            "-c".into(),
            "printf '<%s>' \"$@\"".into(),
            "".into(),
            "two words".into(),
            "a\\\"b".into(),
            "c:\\work dir\\".into(),
        ];
        let line = command_line_of(OsStr::new("C:\\Program Files\\Git\\bin\\bash.exe"), &args)
            .expect("a command line");
        assert_eq!(argv(&line[..line.len() - 1]), args);
    }

    /// A name given twice, differing only in case, is one entry holding the later value.
    #[test]
    fn a_name_given_twice_in_different_case_is_one_entry() {
        let pairs = [
            (OsStr::new("Path"), OsStr::new("first")),
            (OsStr::new("PATH"), OsStr::new("second")),
            (OsStr::new("Path"), OsStr::new("third")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(block, expected(&["Path=third"]));
    }

    /// A forced value arrives under its own spelling: a caller's `lang` does not survive as the
    /// name bash would read, because Windows calls it the same name and msys bash is case-sensitive.
    #[test]
    fn a_forced_value_replaces_the_callers_spelling() {
        let pairs = [
            (OsStr::new("lang"), OsStr::new("caller")),
            (OsStr::new("LANG"), OsStr::new("C.UTF-8")),
        ];
        assert_eq!(
            block(pairs.iter().copied()).unwrap(),
            expected(&["LANG=C.UTF-8"])
        );

        // And the other order keeps the caller's spelling, which is what a caller's own value is.
        let pairs = [
            (OsStr::new("LANG"), OsStr::new("C.UTF-8")),
            (OsStr::new("lang"), OsStr::new("caller")),
        ];
        assert_eq!(
            block(pairs.iter().copied()).unwrap(),
            expected(&["lang=caller"])
        );
    }

    /// Names sort in ordinal ignore-case order, not in the order they were given.
    #[test]
    fn names_sort_without_regard_to_case() {
        let pairs = [
            (OsStr::new("beta"), OsStr::new("2")),
            (OsStr::new("Alpha"), OsStr::new("1")),
            (OsStr::new("Zulu"), OsStr::new("3")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(block, expected(&["Alpha=1", "beta=2", "Zulu=3"]));
    }

    /// Case folding is the system's: `é` and `É` are one name, and they sort where the folded
    /// `É` belongs, before `×`. A fold that only knew ASCII would place `é` after it.
    #[test]
    fn non_ascii_names_fold_together_and_sort_folded() {
        let pairs = [
            (OsStr::new("\u{e9}"), OsStr::new("lower")),
            (OsStr::new("\u{d7}"), OsStr::new("times")),
            (OsStr::new("\u{c9}"), OsStr::new("upper")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        // One entry, holding the later value under the later spelling, sorted where the folded
        // name belongs.
        assert_eq!(block, expected(&["\u{c9}=upper", "\u{d7}=times"]));
    }

    /// A value that is not valid UTF-16 reaches the child unchanged.
    #[test]
    fn an_unpaired_surrogate_survives() {
        let value = OsString::from_wide(&[0xD800, b'x' as u16]);
        let pairs = [(OsStr::new("KEEP"), value.as_os_str())];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(
            block,
            vec![
                b'K' as u16,
                b'E' as u16,
                b'E' as u16,
                b'P' as u16,
                b'=' as u16,
                0xD800,
                b'x' as u16,
                0,
                0
            ]
        );
    }

    /// No names means two terminators, an empty block and not a missing one.
    #[test]
    fn an_empty_environment_is_two_terminators() {
        let block = block(std::iter::empty()).expect("a block");
        assert_eq!(block, vec![0, 0]);
    }

    /// A value may hold `=`; a name that is empty or holds one past its first unit is refused
    /// rather than silently left out of the block.
    #[test]
    fn an_invalid_name_is_refused_and_a_value_may_hold_an_equals_sign() {
        let pairs = [(OsStr::new("EVEN"), OsStr::new("a=b=c"))];
        assert_eq!(
            block(pairs.iter().copied()).unwrap(),
            expected(&["EVEN=a=b=c"])
        );

        for name in ["o=dd", "a=b", "", "="] {
            let pairs = [(OsStr::new(name), OsStr::new("x"))];
            let error = block(pairs.iter().copied()).expect_err("the name is invalid");
            assert!(error.to_string().contains("environment name"), "{error}");
        }
    }

    /// The per-drive current directory `CreateProcessW` documents, `=C:`, is a name of its own.
    #[test]
    fn a_drive_current_directory_is_a_name() {
        let pairs = [
            (OsStr::new("=C:"), OsStr::new("C:\\work")),
            (OsStr::new("A"), OsStr::new("1")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(block, expected(&["=C:=C:\\work", "A=1"]));
    }

    /// The wide form of the `name=value` entries, then the block's terminator.
    fn expected(entries: &[&str]) -> Vec<u16> {
        let mut units = Vec::new();
        for entry in entries {
            units.extend(entry.encode_utf16());
            units.push(0);
        }
        units.push(0);
        units
    }

    fn pid_of(process: &OwnedHandle) -> u32 {
        // SAFETY: the handle is a live process handle owned by the caller.
        unsafe { windows_sys::Win32::System::Threading::GetProcessId(process.as_raw_handle()) }
    }

    /// The Job owns the root from before its first instruction: the root is a member while its
    /// only thread is still suspended, which a resumed or unsuspended creation cannot show.
    #[test]
    fn a_pty_root_is_in_its_job_before_it_runs() {
        let context = LaunchContext::current().expect("this process's context");
        let bash = context.bash().expect("bash for this context").to_path_buf();
        let cwd = std::env::current_dir().expect("a working directory");
        let launch = Launch {
            bash: &bash,
            command: "sleep 5",
            script: None,
            cwd: &cwd,
            env: &context,
            login: false,
            pty: true,
            cols: crate::process::PTY_COLS,
            rows: crate::process::PTY_ROWS,
            exit_status: None,
        };
        let suspended = create_in_job(&launch).expect("a suspended PTY root");
        let pid = pid_of(&suspended.process);
        assert!(
            suspended
                .tree
                .members()
                .unwrap_or_default()
                .iter()
                .any(|member| member.pid == pid),
            "the root is in the job before it runs"
        );
        // SAFETY: the handle is the suspended root's own thread, and the calls balance.
        let previous = unsafe { SuspendThread(suspended.thread.as_raw_handle()) };
        assert_eq!(previous, 1, "the root is still suspended");
        // SAFETY: undoes the count above, leaving the root suspended as it was.
        unsafe { ResumeThread(suspended.thread.as_raw_handle()) };
        let _ = suspended.tree.kill();
    }
}
