//! Windows pipe/ConPTY spawning and kill-on-close Job Object lifecycle.

use super::types::{
    Child, Launch, PtyControl, Spawned, Tree, bash_args, environment, lock, normalize_members,
    pipe_output,
};
use crate::env::var_in;
use crate::platform::conpty_native::{Attributes, ConPty, Terminal};
use crate::platform::members::{Member, MemberBudget};
use crate::platform::windows_command;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::os::windows::io::{AsRawHandle, BorrowedHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::process::{Command, Stdio};
use windows_sys::Win32::Foundation::{
    CloseHandle, ERROR_INVALID_PARAMETER, ERROR_MORE_DATA, GetLastError, INVALID_HANDLE_VALUE,
    STILL_ACTIVE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::System::Diagnostics::ToolHelp::{
    CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_BASIC_PROCESS_ID_LIST,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
    JobObjectBasicProcessIdList, JobObjectExtendedLimitInformation, QueryInformationJobObject,
    SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
    EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess, GetProcessId, INFINITE, OpenProcess,
    OpenThread, PROCESS_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    QueryFullProcessImageNameW, ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW,
    THREAD_SUSPEND_RESUME, TerminateProcess, WaitForSingleObject,
};

impl Child {
    /// Blocks until the root bash exits and returns its bash-convention exit code, and whether the
    /// end was forced because the script had finished while the root did not terminate.
    pub fn wait(&mut self, _tree: &Tree) -> (i32, bool) {
        let (code, forced) = match self {
            Child::Pipe(child) => (
                child.wait().map_or(1, |status| status.code().unwrap_or(1)),
                false,
            ),
            // The console's exit code carries the same signal convention the pipe's does.
            Child::Pty {
                process,
                exit_status,
                ..
            } => crate::platform::conpty_native::wait(process, exit_status.as_deref()),
        };
        (msys_signal_exit(code), forced)
    }

    /// A pipe probe needs a nonblocking root check while it drains a bounded banner.
    pub(super) fn try_wait(&mut self, _tree: &Tree) -> std::io::Result<Option<i32>> {
        match self {
            Child::Pipe(child) => child
                .try_wait()
                .map(|status| status.map(|status| msys_signal_exit(status.code().unwrap_or(1)))),
            Child::Pty { .. } => unreachable!("version probes use pipes"),
        }
    }

    /// Blocks until no process of the tree runs.
    pub fn wait_tree(&mut self, tree: &Tree) -> bool {
        let mut pause = std::time::Duration::from_millis(10);
        while tree.members().is_none_or(|members| !members.is_empty()) {
            std::thread::sleep(pause);
            pause = (pause * 2).min(std::time::Duration::from_millis(500));
        }
        // No more member queries need the original root identity once the tree ends.
        *lock(&tree.root) = None;
        true
    }
}

impl Tree {
    /// Terminates every process in the tree. Killing an empty tree succeeds.
    pub fn kill(&self) -> std::io::Result<()> {
        // SAFETY: the job handle is owned by this tree and alive.
        let ended = unsafe { TerminateJobObject(self.job.as_raw_handle(), 1) };
        if ended == 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }

    /// The processes of the tree other than its root: those in its job, apart from console hosts.
    pub fn members(&self) -> Option<Vec<Member>> {
        self.members_with_budget(&mut MemberBudget::unlimited())
    }

    pub(crate) fn members_with_budget(&self, budget: &mut MemberBudget<'_>) -> Option<Vec<Member>> {
        let mut result = {
            let held = lock(&self.root);
            let Some(process) = held.as_ref() else {
                return Some(Vec::new());
            };
            let handle = process.as_raw_handle();
            // SAFETY: the held handle keeps this root's PID reserved during the query.
            let pid = unsafe { GetProcessId(handle) };
            job_members_with_budget(self.job.as_raw_handle(), Some((pid, handle)), budget)
        };
        if let Some(list) = result.as_mut() {
            normalize_members(list);
        }
        result
    }

    pub(crate) fn adopt(process: RawHandle) -> std::io::Result<Tree> {
        // SAFETY: the caller owns the process handle for the duration of adoption.
        let root = unsafe { BorrowedHandle::borrow_raw(process) }.try_clone_to_owned()?;
        Ok(Tree {
            job: kill_on_close_job(process)?,
            root: std::sync::Mutex::new(Some(root)),
        })
    }

    /// A dummy process tree for unit tests that inject their own `members` callback.
    #[cfg(test)]
    pub(crate) fn synthetic_for_test() -> Tree {
        // SAFETY: null arguments request an unnamed empty job with default security.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        assert!(!raw.is_null(), "CreateJobObjectW failed");
        // SAFETY: CreateJobObjectW returned a fresh handle owned here.
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        Tree {
            job,
            root: std::sync::Mutex::new(None),
        }
    }
}

/// The msys runtime reports a process killed by signal N as exit status N << 8.
// ponytail: a native Windows program that exits with exactly 256..=16384 in steps of 256 is
// read as a signal exit too.
fn msys_signal_exit(code: i32) -> i32 {
    if code & 0xff == 0 && (1..=64).contains(&(code >> 8)) {
        128 + (code >> 8)
    } else {
        code
    }
}

/// Creates a command that never allocates a console window on Windows.
fn quiet_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    use std::os::windows::process::CommandExt;

    let mut command = Command::new(program);
    command.creation_flags(CREATE_NO_WINDOW);
    command
}

pub fn spawn(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    if launch.pty {
        spawn_pty(launch)
    } else {
        spawn_pipe(launch)
    }
}

fn probe_overrides(launch: &Launch<'_>) -> Vec<(&'static str, OsString)> {
    let mut env = Vec::new();
    if var_in(launch.base_env, "MSYS2_PATH_TYPE").is_none() {
        env.push(("MSYS2_PATH_TYPE", "inherit".into()));
    }
    env
}

fn spawn_pipe(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    spawn_pipe_args(launch, bash_args(launch), false, false)
}

pub(super) fn spawn_pipe_args(
    launch: &Launch<'_>,
    args: Vec<OsString>,
    probe: bool,
    raw_probe_env: bool,
) -> std::io::Result<Spawned> {
    use std::os::windows::process::CommandExt;
    let (reader, writer) = std::io::pipe()?;
    let output = pipe_output(reader, probe)?;
    let overrides = if raw_probe_env {
        probe_overrides(launch)
    } else {
        environment(launch)
    };
    let mut command = quiet_command(launch.bash);
    command
        .args(args)
        .env_clear()
        .envs(launch.base_env.iter().map(|(name, value)| (name, value)))
        .envs(overrides)
        .current_dir(launch.cwd)
        .stdin(if launch.pipe_stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(writer.try_clone()?)
        .stderr(writer)
        .creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
    if probe {
        command.stderr(Stdio::null());
    }
    let mut child = command.spawn()?;
    // The command still holds the pipe's write ends; dropping it lets the reader reach EOF.
    drop(command);
    let adopted = Tree::adopt(child.as_raw_handle()).and_then(|tree| {
        resume_threads(child.id())?;
        Ok(tree)
    });
    let tree = match adopted {
        Ok(tree) => tree,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let input = launch.pipe_stdin.then(|| -> Box<dyn Write + Send> {
        Box::new(child.stdin.take().expect("stdin is piped"))
    });
    Ok(Spawned {
        child: Child::Pipe(child),
        output,
        input,
        tree,
        pty: None,
    })
}

/// A root bash created suspended inside its console and already in the task's Job.
pub(super) struct Suspended {
    pub(super) process: OwnedHandle,
    pub(super) thread: OwnedHandle,
    pub(super) tree: Tree,
    terminal: Terminal,
    output: Box<dyn Read + Send>,
    input: Box<dyn Write + Send>,
}

fn spawn_pty(launch: &Launch<'_>) -> std::io::Result<Spawned> {
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
        input: Some(suspended.input),
        tree: suspended.tree,
        pty: Some(PtyControl::new(terminal)),
    })
}

/// Creates bash suspended inside its console and puts it in the task's Job. The root runs no
/// instruction until [`resume`] is called on it.
pub(super) fn create_in_job(launch: &Launch<'_>) -> std::io::Result<Suspended> {
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
    let app = windows_command::nul_terminated(launch.bash.as_os_str())?;
    let mut command =
        windows_command::command_line_of(launch.bash.as_os_str(), &bash_args(launch))?;
    let block = environment_block(launch)?;
    let cwd = windows_command::nul_terminated(launch.cwd.as_os_str())?;
    let mut attributes = Attributes::new(1)?;
    attributes.set_console(&terminal)?;
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

/// The session's environment, then this launch's values over it, as `CreateProcessW` wants them:
/// names sorted in ordinal ignore-case order, each `name=value` NUL-terminated, and the block
/// NUL-terminated.
fn environment_block(launch: &Launch<'_>) -> std::io::Result<Vec<u16>> {
    let overrides = environment(launch);
    let pairs = launch
        .base_env
        .iter()
        .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
        .chain(
            overrides
                .iter()
                .map(|(name, value)| (std::ffi::OsStr::new(*name), value.as_os_str())),
        );
    windows_command::block(pairs)
}

/// Creates a job that terminates its processes when its last handle closes, and assigns
/// `process` to it. Descendants join the job automatically.
pub(super) fn kill_on_close_job(process: RawHandle) -> std::io::Result<OwnedHandle> {
    // SAFETY: null arguments request an unnamed job with default security.
    let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if raw.is_null() {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: CreateJobObjectW returned a fresh handle owned here.
    let job = unsafe { OwnedHandle::from_raw_handle(raw) };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    // SAFETY: the buffer matches the information class and outlives the call.
    let configured = unsafe {
        SetInformationJobObject(
            job.as_raw_handle(),
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of_val(&limits) as u32,
        )
    };
    // SAFETY: both handles are alive for the call.
    if configured == 0 || unsafe { AssignProcessToJobObject(job.as_raw_handle(), process) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(job)
}

/// The live processes of a job, without console hosts or exited processes. A failed or
/// incomplete query stays unknown; a process this user may not query counts, named ?.
#[cfg(test)]
pub(super) fn job_members(job: RawHandle, root: Option<(u32, RawHandle)>) -> Option<Vec<Member>> {
    job_members_with_budget(job, root, &mut MemberBudget::unlimited())
}

pub(super) fn job_members_with_budget(
    job: RawHandle,
    root: Option<(u32, RawHandle)>,
    budget: &mut MemberBudget<'_>,
) -> Option<Vec<Member>> {
    let accounting = || {
        let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: info has the layout and size requested for this job query.
        let ok = unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicAccountingInformation,
                (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                std::mem::size_of_val(&info) as u32,
                std::ptr::null_mut(),
            )
        };
        (ok != 0).then_some(info)
    };
    let ids = || {
        job_pids(|buffer, bytes| {
            // SAFETY: job_pids supplies a usize-aligned buffer with room for the header and IDs.
            let queried = unsafe {
                QueryInformationJobObject(
                    job,
                    JobObjectBasicProcessIdList,
                    buffer.as_mut_ptr().cast(),
                    bytes,
                    std::ptr::null_mut(),
                )
            };
            if queried == 0 {
                Err(unsafe { GetLastError() })
            } else {
                Ok(())
            }
        })
    };
    collect_job_members(
        accounting,
        ids,
        |pid, budget| {
            if let Some((root_pid, handle)) = root
                && root_pid == pid
            {
                inspect_member(job, handle, true, budget)
            } else {
                job_process(job, pid, budget)
            }
        },
        budget,
    )
}

pub(super) fn collect_job_members(
    mut accounting: impl FnMut() -> Option<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>,
    mut ids: impl FnMut() -> Option<Vec<u32>>,
    mut inspect: impl FnMut(u32, &mut MemberBudget<'_>) -> JobProcess,
    budget: &mut MemberBudget<'_>,
) -> Option<Vec<Member>> {
    let before = accounting()?;
    if before.ActiveProcesses == 0 {
        return Some(Vec::new());
    }
    let mut live = Vec::new();
    let mut unknown = Vec::new();
    let initial = ids()?;
    let mut uncovered = false;
    for &pid in &initial {
        let inspected = inspect(pid, budget);
        if budget.refused() {
            return None;
        }
        match inspected {
            JobProcess::Named(name) => {
                budget.retain(name.capacity());
                live.push(Member { pid, name });
            }
            JobProcess::Unknown => unknown.push(pid),
            JobProcess::Gone => {}
        }
    }
    if !unknown.is_empty() {
        let mut current: std::collections::HashSet<u32> = ids()?.into_iter().collect();
        for pid in unknown.into_iter().filter(|pid| current.contains(pid)) {
            if !budget.temporary(1) {
                return None;
            }
            live.push(Member {
                pid,
                name: "?".into(),
            });
            budget.retain(1);
        }
        for pid in initial {
            current.remove(&pid);
        }
        uncovered = !current.is_empty();
    }
    // A process may exit while we read its image, or a parent may fork after the PID list.
    // Zero active members is authoritative even when an earlier PID could not be inspected.
    let after = accounting()?;
    if after.ActiveProcesses == 0 {
        return Some(Vec::new());
    }
    if live.is_empty() && (uncovered || after.TotalProcesses != before.TotalProcesses) {
        return None;
    }
    Some(live)
}

pub(super) const JOB_LIST_HEADER_WORDS: usize =
    std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList)
        / std::mem::size_of::<usize>();

/// A growing job may need another snapshot; bounded retries keep that uncertainty explicit.
pub(super) fn job_pids(
    mut query: impl FnMut(&mut [usize], u32) -> Result<(), u32>,
) -> Option<Vec<u32>> {
    let mut capacity = 1024_usize;
    for _ in 0..8 {
        let words = JOB_LIST_HEADER_WORDS.checked_add(capacity)?;
        let bytes = u32::try_from(words.checked_mul(std::mem::size_of::<usize>())?).ok()?;
        let mut buffer = Vec::new();
        buffer.try_reserve_exact(words).ok()?;
        buffer.resize(words, 0_usize);
        let complete = match query(&mut buffer, bytes) {
            Ok(()) => true,
            Err(ERROR_MORE_DATA) => false,
            Err(_) => return None,
        };
        // SAFETY: usize storage has the header's alignment and at least one PID slot.
        let list = unsafe { &*buffer.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
        let (assigned, listed) = (
            list.NumberOfAssignedProcesses as usize,
            list.NumberOfProcessIdsInList as usize,
        );
        if listed > capacity || listed > assigned {
            return None;
        }
        if complete && listed == assigned {
            return buffer[JOB_LIST_HEADER_WORDS..JOB_LIST_HEADER_WORDS + listed]
                .iter()
                .map(|&pid| u32::try_from(pid).ok())
                .collect();
        }
        capacity = capacity.checked_mul(2)?.max(assigned);
    }
    None
}

pub(super) enum JobProcess {
    Gone,
    Named(String),
    Unknown,
}

pub(super) fn job_process(job: RawHandle, pid: u32, budget: &mut MemberBudget<'_>) -> JobProcess {
    // SAFETY: every successful OpenProcess handle is owned and closed in this function.
    unsafe {
        let mut raw = OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            pid,
        );
        let can_wait = !raw.is_null();
        if !can_wait && GetLastError() != ERROR_INVALID_PARAMETER {
            // Query rights alone may still expose the exit code and job membership.
            raw = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        }
        if raw.is_null() {
            if GetLastError() == ERROR_INVALID_PARAMETER {
                return JobProcess::Gone;
            }
            // A process may allow waiting without granting image or exit-code queries.
            let raw = OpenProcess(PROCESS_SYNCHRONIZE, 0, pid);
            if raw.is_null() {
                return if GetLastError() == ERROR_INVALID_PARAMETER {
                    JobProcess::Gone
                } else {
                    JobProcess::Unknown
                };
            }
            let process = OwnedHandle::from_raw_handle(raw);
            return if WaitForSingleObject(process.as_raw_handle(), 0) == WAIT_OBJECT_0 {
                JobProcess::Gone
            } else {
                JobProcess::Unknown
            };
        }
        let process = OwnedHandle::from_raw_handle(raw);
        inspect_member(job, process.as_raw_handle(), can_wait, budget)
    }
}

pub(super) fn inspect_member(
    job: RawHandle,
    process: RawHandle,
    can_wait: bool,
    budget: &mut MemberBudget<'_>,
) -> JobProcess {
    // SAFETY: both handles remain open and each buffer lives through its call.
    unsafe {
        if can_wait && WaitForSingleObject(process, 0) == WAIT_OBJECT_0 {
            return JobProcess::Gone;
        }
        let mut in_job = 0;
        let membership_known = IsProcessInJob(process, job, &mut in_job) != 0;
        if membership_known && in_job == 0 {
            return JobProcess::Gone;
        }
        if !budget.temporary(32_768 * 2) {
            return JobProcess::Unknown;
        }
        let mut buffer = vec![0_u16; 32_768];
        let mut size = buffer.len() as u32;
        let named = membership_known
            && QueryFullProcessImageNameW(process, 0, buffer.as_mut_ptr(), &mut size) != 0;
        // Inspect termination after the image lookup. A normal exit code can be available
        // during teardown; 259 is also a valid final code, so only a wait resolves it.
        let mut code = 0;
        if GetExitCodeProcess(process, &mut code) != 0 && code != STILL_ACTIVE as u32 {
            return JobProcess::Gone;
        }
        let waited = can_wait.then(|| WaitForSingleObject(process, 0));
        if waited == Some(WAIT_OBJECT_0) {
            return JobProcess::Gone;
        }
        if named {
            if !budget.temporary(buffer.capacity() * 2 + size as usize * 9) {
                return JobProcess::Unknown;
            }
            let path = String::from_utf16_lossy(&buffer[..size as usize]);
            let name = path.rsplit('\\').next().unwrap_or(&path);
            if name.eq_ignore_ascii_case("conhost.exe") {
                return JobProcess::Gone;
            }
            if waited == Some(WAIT_TIMEOUT) {
                return JobProcess::Named(name.to_owned());
            }
        }
        JobProcess::Unknown
    }
}

/// Resumes every thread of a process created with `CREATE_SUSPENDED`.
pub(super) fn resume_threads(pid: u32) -> std::io::Result<()> {
    // SAFETY: the snapshot handle is closed below on every path.
    let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    if snapshot == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    let mut entry = THREADENTRY32 {
        dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
        ..Default::default()
    };
    let mut result = Err(std::io::Error::new(
        std::io::ErrorKind::NotFound,
        "no suspended thread found",
    ));
    // SAFETY: entry has the documented size and lives across the enumeration.
    let mut more = unsafe { Thread32First(snapshot, &mut entry) } != 0;
    while more {
        if entry.th32OwnerProcessID == pid {
            // SAFETY: the thread id comes from the live snapshot; the handle is closed below.
            let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
            if thread.is_null() {
                result = Err(std::io::Error::last_os_error());
                break;
            }
            // SAFETY: the handle has THREAD_SUSPEND_RESUME access.
            let resumed = unsafe { ResumeThread(thread) };
            // SAFETY: this function owns the thread handle.
            unsafe { CloseHandle(thread) };
            if resumed == u32::MAX {
                result = Err(std::io::Error::last_os_error());
                break;
            }
            result = Ok(());
        }
        // SAFETY: as for Thread32First.
        more = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
    }
    // SAFETY: this function owns the snapshot handle.
    unsafe { CloseHandle(snapshot) };
    result
}
