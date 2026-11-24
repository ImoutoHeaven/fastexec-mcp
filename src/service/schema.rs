//! Request schema, normalization, and argument validation for the exec service.

use crate::files;
use crate::platform::process;
use crate::term::output::Truncate;
use crate::wait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const DEFAULT_WAIT_MS: u64 = 30_000;
const DEFAULT_INTERACT_WAIT_MS: u64 = 2_000;
const MAX_WAIT_MS: u64 = 240_000;
pub(super) const MAX_INPUT_BYTES: usize = 16 * 1024;
const MAX_START_INPUT_BYTES: usize = 1 << 20;
pub(super) const DEFAULT_KEY_DELAY_MS: u64 = 150;
const MAX_KEY_DELAY_MS: u64 = 2_000;
/// Longest pause the keys of one poll add up to before its wait begins.
const MAX_KEY_PAUSE_MS: u64 = 30_000;
const DEFAULT_MAX_BYTES: usize = 16_384;
const DEFAULT_PROGRAM_MAX_BYTES: usize = 4 << 20;
const MAX_BYTES_RANGE: std::ops::RangeInclusive<u64> = 100..=4 << 20;
const LIST_LIMIT_RANGE: std::ops::RangeInclusive<u64> = 1..=500;

#[derive(Clone, Copy, Debug)]
pub struct ExecPolicy {
    /// When true, waitMs omits a default deadline unless returnWhen (30s) or input/keys/resize (2s) applies.
    pub unbounded_default_wait: bool,
    /// Upper bound on waitMs (Some(240_000) for Agent, None for Program).
    pub max_wait_ms: Option<u64>,
    pub default_truncate: Truncate,
    pub default_max_bytes: usize,
    /// When true, injects omission/cut markers and permits `head_tail`.
    /// When false, output is a single contiguous raw slice (rejects `head_tail` and list `maxBytes`/`truncate`).
    pub inline_markers: bool,
    /// Preserves NUL bytes during non-raw cleaning (true for Program, false for Agent).
    pub keep_nul: bool,
}

impl ExecPolicy {
    pub const AGENT: Self = Self {
        unbounded_default_wait: false,
        max_wait_ms: Some(MAX_WAIT_MS),
        default_truncate: Truncate::HeadTail,
        default_max_bytes: DEFAULT_MAX_BYTES,
        inline_markers: true,
        keep_nul: false,
    };

    pub const PROGRAM: Self = Self {
        unbounded_default_wait: true,
        max_wait_ms: None,
        default_truncate: Truncate::Head,
        default_max_bytes: DEFAULT_PROGRAM_MAX_BYTES,
        inline_markers: false,
        keep_nul: true,
    };

    /// Resolves `waitMs` to an effective timeout in milliseconds (`None` waits until exit).
    pub(crate) fn resolve_wait_ms(
        self,
        wait_ms: Option<u64>,
        has_return_when: bool,
        interacts: bool,
    ) -> Option<u64> {
        match wait_ms {
            Some(ms) => Some(ms),
            None if has_return_when => Some(DEFAULT_WAIT_MS),
            None if interacts => Some(DEFAULT_INTERACT_WAIT_MS),
            None if self.unbounded_default_wait => None,
            None => Some(DEFAULT_WAIT_MS),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Start,
    Poll,
    Kill,
    List,
    Transcript,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListScope {
    #[default]
    Session,
    All,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListState {
    Active,
    Finished,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// start runs bash. poll accepts input, waits and returns unseen output. kill ends the tree and returns unseen output. list shows tasks. transcript returns new PTY scrollback and the current screen.
    pub action: Action,
    /// start: the bash command line. NUL bytes (U+0000) are rejected before execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) command: Option<String>,
    /// start: working directory. Omit for the working directory the session started in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cwd: Option<String>,
    /// start: run inside a pseudo-terminal (default false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) pty: Option<bool>,
    /// start: run bash -lc to source login profiles (default false: bash --noprofile --norc -c with the session's login-environment snapshot).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) login_shell: Option<bool>,
    /// start: kill the entire tree, including kept processes, this many ms after launch. Zero or omitted disables it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) kill_after_ms: Option<u64>,
    /// start: keep descendants after bash exits until kill, killAfterMs or owner shutdown (session or daemon). By default they end with bash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keep_tree: Option<bool>,
    /// poll, kill, transcript: a reader handle (<taskId>.<reader>) to continue, or a bare task ID to attach a new reader from the retained log's beginning.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) task_id: Option<String>,
    /// start, pipe only: text written to stdin, up to 1 MiB; stdin closes after it unless eof is false. poll: at most 16 KiB written to stdin before keys and waiting. Write failures are reported; terminal line discipline can transform or discard PTY input, which is warned separately.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) input: Option<String>,
    /// poll, PTY only: keys pressed after `input` is written, in array order, e.g. ["Enter"] or ["Escape", ":", "q", "Enter"]. One key per item, named as in tmux send-keys: Enter, Tab, BTab, Escape, Space, BSpace, Up, Down, Left, Right, Home, End, PageUp/PgUp/PPage, PageDown/PgDn/NPage, Insert/IC, Delete/DC, F1-F12, KP0-KP9, KP/, KP*, KP-, KP+, KP., KPEnter, [NUL]-[US] for C0 controls, 0xHH for a code point, or one character; prefixes C- (Ctrl), M- (Meta/Alt), S- (Shift) combine, as in C-c, M-x, C-M-a, S-Up, and ^c means C-c. Named keys ignore case; single characters retain their case. Arrow and keypad keys follow the program's cursor and keypad modes. An unknown name, or a modifier the key cannot carry (C-Enter, C-S-a) in the program's mode when the poll starts, is an error before anything is sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) keys: Option<Vec<String>>,
    /// poll: delay before each key, including the first after input, 0-2000 ms (default 150). Zero sends input and keys together. To separate Escape from the next key, exceed the program's escape timeout (often 500 ms).
    #[schemars(range(min = 0, max = 2000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) key_delay_ms: Option<u64>,
    /// start, pipe only: false keeps stdin open for poll input (default: stdin closes after input, and is empty without it). poll: true closes stdin after input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) eof: Option<bool>,
    /// start, poll: wait up to 240000 ms per call. Poll again for longer. Ignored by kill and transcript.
    #[schemars(range(min = 0, max = 240000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) wait_ms: Option<u64>,
    /// start, poll: any match returns; the task continues. Text conditions take 1-16 case-sensitive literals, 1-4096 bytes each, without line breaks. waitMs defaults to 30000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) return_when: Option<wait::ReturnWhen>,
    /// Window placement for all actions. Default head_tail, tail for transcript, head for nested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) truncate: Option<Truncate>,
    /// Body budget in UTF-8 bytes, 100-4194304 (default 16384; nested 4194304). Status, footer, errors, and warnings stay outside it.
    #[schemars(range(min = 100, max = 4194304))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) max_bytes: Option<u64>,
    /// start, poll: return output without cleaning (default false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) raw: Option<bool>,
    /// start, poll: WHATWG label of the output encoding, e.g. "big5" or "gbk". On start it becomes the task default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) encoding: Option<String>,
    /// start, poll, PTY only: return the rendered terminal screen and cursor position instead of the output stream; for full-screen and TUI programs. Marks the stream read. Not with truncate, raw, or encoding.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) screen: Option<bool>,
    /// start, poll, PTY only: terminal columns, 20-500. start defaults 120; poll keeps the current value when omitted.
    #[schemars(range(min = 20, max = 500))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cols: Option<u16>,
    /// start, poll, PTY only: terminal screen rows, 5-200. start defaults 30; poll keeps the current value when omitted.
    #[schemars(range(min = 5, max = 200))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) rows: Option<u16>,
    /// list: "session" for this session's tasks (default without parentTaskId), or "all" for every task (default with parentTaskId).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) scope: Option<ListScope>,
    /// list: "active" for running or kept-process tasks, or "finished" for the rest; omit for both. Children whose parent is filtered out promote to top level.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) state: Option<ListState>,
    /// list: max top-level tasks (or children with parentTaskId), newest first, 1-500 (default 20). Folded codemode children do not count.
    #[schemars(range(min = 1, max = 500))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) limit: Option<u64>,
    /// list: codemode bare task ID or reader handle to list its children flat; defaults scope to "all".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) parent_task_id: Option<String>,
}

/// Output options shared by start and poll.
pub(crate) struct View {
    pub(super) inline_markers: bool,
    pub(super) keep_nul: bool,
    pub(super) owned_by_script: bool,
    pub(super) truncate: Truncate,
    pub(crate) max_bytes: usize,
    pub(super) raw: bool,
    pub(super) encoding: Option<&'static encoding_rs::Encoding>,
    pub(super) screen: bool,
    /// The call set `returnWhen`.
    pub(super) conditions: bool,
}

/// Effective presence of an optional boolean flag in applicability and conflict checks:
/// `false` and `None` count as absent where the parameter is inapplicable.
pub(super) fn flag(value: Option<bool>) -> bool {
    value == Some(true)
}

pub(crate) fn normalize_request(request: &mut Request) {
    if request.keys.as_ref().is_some_and(Vec::is_empty) {
        request.keys = None;
        request.key_delay_ms = None;
    }
}

pub(crate) fn validate(request: &Request, policy: ExecPolicy) -> Result<View, String> {
    let present = [
        ("command", request.command.is_some()),
        ("cwd", request.cwd.is_some()),
        ("pty", flag(request.pty)),
        ("loginShell", flag(request.login_shell)),
        ("killAfterMs", request.kill_after_ms.is_some()),
        ("keepTree", flag(request.keep_tree)),
        ("taskId", request.task_id.is_some()),
        ("input", request.input.is_some()),
        ("eof", flag(request.eof)),
        ("waitMs", request.wait_ms.is_some()),
        ("truncate", request.truncate.is_some()),
        ("maxBytes", request.max_bytes.is_some()),
        ("raw", flag(request.raw)),
        ("encoding", request.encoding.is_some()),
        ("keys", request.keys.is_some()),
        ("keyDelayMs", request.key_delay_ms.is_some()),
        ("screen", flag(request.screen)),
        ("cols", request.cols.is_some()),
        ("rows", request.rows.is_some()),
        ("returnWhen", request.return_when.is_some()),
        ("scope", request.scope.is_some()),
        ("state", request.state.is_some()),
        ("limit", request.limit.is_some()),
        ("parentTaskId", request.parent_task_id.is_some()),
    ];
    const VIEW: [&str; 7] = [
        "waitMs",
        "returnWhen",
        "truncate",
        "maxBytes",
        "raw",
        "encoding",
        "screen",
    ];
    let allowed: Vec<&str> = match request.action {
        Action::Start => [
            "command",
            "cwd",
            "pty",
            "loginShell",
            "killAfterMs",
            "keepTree",
            "input",
            "eof",
            "cols",
            "rows",
        ]
        .into_iter()
        .chain(VIEW)
        .collect(),
        Action::Poll => [
            "taskId",
            "input",
            "eof",
            "keys",
            "keyDelayMs",
            "cols",
            "rows",
        ]
        .into_iter()
        .chain(VIEW)
        .collect(),
        Action::Kill => vec!["taskId", "waitMs", "truncate", "maxBytes"],
        Action::List => {
            let mut fields = vec!["scope", "state", "limit", "parentTaskId"];
            if policy.inline_markers {
                fields.extend(["truncate", "maxBytes"]);
            }
            fields
        }
        Action::Transcript => vec!["taskId", "waitMs", "truncate", "maxBytes"],
    };
    for (name, set) in present {
        if set && !allowed.contains(&name) {
            return Err(format!(
                "`{name}` does not apply to action {}; remove it.",
                action_name(request.action)
            ));
        }
    }
    match request.action {
        Action::Start
            if request
                .command
                .as_deref()
                .is_none_or(|c| c.trim().is_empty()) =>
        {
            return Err("start needs a non-empty `command`.".into());
        }
        Action::Start
            if request
                .command
                .as_deref()
                .is_some_and(|command| command.contains('\0')) =>
        {
            return Err("`command` contains a NUL byte (U+0000); no command was started.".into());
        }
        Action::Poll | Action::Kill | Action::Transcript if request.task_id.is_none() => {
            return Err(format!(
                "{} needs `taskId`; use action list to find it.",
                action_name(request.action)
            ));
        }
        _ => {}
    }
    if request.action == Action::Start {
        if flag(request.pty) {
            if request.input.is_some() {
                return Err(
                    "`input` on start applies to pipe tasks only; for PTY tasks, send input with poll."
                        .into(),
                );
            }
            if flag(request.eof) {
                return Err(
                    "`eof` does not apply to PTY tasks; a PTY has no stdin half-close.".into(),
                );
            }
        } else if request
            .input
            .as_ref()
            .is_some_and(|input| input.len() > MAX_START_INPUT_BYTES)
        {
            return Err(
                "input on start exceeds 1 MiB; write larger data to a file and redirect stdin."
                    .into(),
            );
        }
    }
    let screen = flag(request.screen);
    if screen {
        if let Some(name) = [
            ("truncate", request.truncate.is_some()),
            ("raw", flag(request.raw)),
            ("encoding", request.encoding.is_some()),
        ]
        .into_iter()
        .find_map(|(name, set)| set.then_some(name))
        {
            return Err(format!(
                "`{name}` does not apply with screen; remove one of them."
            ));
        }
        if request.action == Action::Start && !flag(request.pty) {
            return Err("screen needs a PTY task; start with pty: true.".into());
        }
    }
    if let Some(when) = &request.return_when {
        when.validate()?;
        if let Some(name) = when.screen_condition()
            && request.action == Action::Start
            && !flag(request.pty)
        {
            return Err(format!("{name} needs a PTY task; start with pty: true."));
        }
    }
    if let Some(delay) = request.key_delay_ms {
        if request.keys.is_none() {
            return Err("keyDelayMs applies only with keys; remove it.".into());
        }
        if delay > MAX_KEY_DELAY_MS {
            return Err(format!(
                "keyDelayMs {delay} is out of range 0-{MAX_KEY_DELAY_MS}."
            ));
        }
    }
    if let Some(keys) = &request.keys {
        let delay = request.key_delay_ms.unwrap_or(DEFAULT_KEY_DELAY_MS);
        let pause = keys.len() as u64 * delay;
        if pause > MAX_KEY_PAUSE_MS {
            return Err(format!(
                "The keys would pause {pause} ms in all at keyDelayMs {delay}, over the {MAX_KEY_PAUSE_MS} ms limit; send fewer keys per poll or lower keyDelayMs."
            ));
        }
    }
    if let Some(max_wait) = policy.max_wait_ms
        && let Some(wait) = request.wait_ms.filter(|&wait| wait > max_wait)
    {
        return Err(format!("waitMs {wait} is out of range 0-{max_wait}."));
    }
    if !policy.inline_markers && request.truncate == Some(Truncate::HeadTail) {
        return Err(
            "`truncate: \"head_tail\"` is not supported without inline markers; use \"head\" or \"tail\"."
                .into(),
        );
    }
    // A size belongs to a PTY task's terminal; a pipe task's output is a byte stream. `start`
    // reads an omitted `pty` as false, so a size there needs `pty: true`.
    if let Some(name) = [("cols", request.cols), ("rows", request.rows)]
        .into_iter()
        .find_map(|(name, set)| set.is_some().then_some(name))
    {
        let applies = match request.action {
            Action::Start => flag(request.pty),
            _ => true,
        };
        if !applies {
            return Err(format!(
                "`{name}` applies to PTY tasks only; start with pty: true or remove it."
            ));
        }
    }
    if let Some((name, value, range)) = [
        (
            "cols",
            request.cols,
            process::PTY_COLS_MIN..=process::PTY_COLS_MAX,
        ),
        (
            "rows",
            request.rows,
            process::PTY_ROWS_MIN..=process::PTY_ROWS_MAX,
        ),
    ]
    .into_iter()
    .find_map(|(name, set, range)| {
        set.filter(|value| !range.contains(value))
            .map(|value| (name, value, range))
    }) {
        return Err(format!(
            "{name} {value} is out of range {}-{}.",
            range.start(),
            range.end()
        ));
    }
    if let Some(bytes) = request.max_bytes
        && !MAX_BYTES_RANGE.contains(&bytes)
    {
        return Err(format!("maxBytes {bytes} is out of range 100-4194304."));
    }
    if let Some(limit) = request.limit
        && !LIST_LIMIT_RANGE.contains(&limit)
    {
        return Err(format!("limit {limit} is out of range 1-500."));
    }
    if request.action == Action::Poll
        && request
            .input
            .as_ref()
            .is_some_and(|input| input.len() > MAX_INPUT_BYTES)
    {
        return Err(
            "input exceeds 16 KiB; send it in smaller pieces or write it to a file.".into(),
        );
    }
    let encoding = match request.encoding.as_deref() {
        None => None,
        Some(label) => Some(encoding_rs::Encoding::for_label(label.trim().as_bytes())
            .filter(|encoding| encoding.is_ascii_compatible())
            .ok_or_else(|| {
            format!("Unknown or unsupported encoding {label:?}; use an ASCII-compatible WHATWG label such as \"utf-8\", \"big5\", or \"gbk\" (UTF-16 output is not supported).")
        })?),
    };
    if request.action == Action::Start
        && let Some(name) = request
            .return_when
            .as_ref()
            .and_then(|when| when.screen_condition())
        && encoding.is_some_and(|encoding| encoding != encoding_rs::UTF_8)
    {
        return Err(format!("{name} needs UTF-8 output; remove encoding."));
    }
    let default_truncate = if request.action == Action::Transcript {
        Truncate::Tail
    } else {
        policy.default_truncate
    };
    Ok(View {
        inline_markers: policy.inline_markers,
        keep_nul: policy.keep_nul,
        owned_by_script: false,
        truncate: request.truncate.unwrap_or(default_truncate),
        max_bytes: request
            .max_bytes
            .map_or(policy.default_max_bytes, |bytes| bytes as usize),
        raw: flag(request.raw),
        encoding,
        screen,
        conditions: request.return_when.is_some(),
    })
}

pub(super) fn action_name(action: Action) -> &'static str {
    match action {
        Action::Start => "start",
        Action::Poll => "poll",
        Action::Kill => "kill",
        Action::List => "list",
        Action::Transcript => "transcript",
    }
}

pub(crate) fn validate_cwd(
    dir: &str,
    launch: &crate::session::LaunchContext,
) -> Result<PathBuf, String> {
    let resolved = files::path::resolve(dir, launch)?;
    if !resolved.is_dir() {
        return Err(format!(
            "cwd {} is not an existing directory.",
            files::path::display_path(&resolved)
        ));
    }
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(value: serde_json::Value) -> Request {
        serde_json::from_value(value).expect("request deserializes")
    }

    #[test]
    fn parent_task_id_allowed_only_on_list() {
        let list = parse(json!({
            "action": "list",
            "parentTaskId": "p1-1"
        }));
        assert_eq!(list.parent_task_id.as_deref(), Some("p1-1"));
        assert!(validate(&list, ExecPolicy::AGENT).is_ok());
        assert!(validate(&list, ExecPolicy::PROGRAM).is_ok());

        for req in [
            json!({"action": "start", "command": "true", "parentTaskId": "p1-1"}),
            json!({"action": "poll", "taskId": "p1-1", "parentTaskId": "p1-1"}),
            json!({"action": "kill", "taskId": "p1-1", "parentTaskId": "p1-1"}),
            json!({"action": "transcript", "taskId": "p1-1", "parentTaskId": "p1-1"}),
        ] {
            let parsed = parse(req);
            let err = validate(&parsed, ExecPolicy::AGENT)
                .err()
                .expect("non-list action must reject parentTaskId");
            assert!(err.contains("`parentTaskId` does not apply"), "{err}");
        }
    }

    #[test]
    fn policy_constants_match_spec() {
        const {
            assert!(!ExecPolicy::AGENT.unbounded_default_wait);
            assert!(ExecPolicy::AGENT.inline_markers);
            assert!(!ExecPolicy::AGENT.keep_nul);
            assert!(ExecPolicy::PROGRAM.unbounded_default_wait);
            assert!(!ExecPolicy::PROGRAM.inline_markers);
            assert!(ExecPolicy::PROGRAM.keep_nul);
        }
        assert_eq!(ExecPolicy::AGENT.max_wait_ms, Some(240_000));
        assert_eq!(ExecPolicy::AGENT.default_truncate, Truncate::HeadTail);
        assert_eq!(ExecPolicy::AGENT.default_max_bytes, 16_384);

        assert_eq!(ExecPolicy::PROGRAM.max_wait_ms, None);
        assert_eq!(ExecPolicy::PROGRAM.default_truncate, Truncate::Head);
        assert_eq!(ExecPolicy::PROGRAM.default_max_bytes, 4 << 20);
    }

    #[test]
    fn defaults_follow_policy_and_transcript_uses_tail() {
        let start = parse(json!({"action": "start", "command": "true"}));
        let agent_start = validate(&start, ExecPolicy::AGENT).expect("agent start valid");
        assert_eq!(agent_start.truncate, Truncate::HeadTail);
        assert_eq!(agent_start.max_bytes, 16_384);

        let program_start = validate(&start, ExecPolicy::PROGRAM).expect("program start valid");
        assert_eq!(program_start.truncate, Truncate::Head);
        assert_eq!(program_start.max_bytes, 4 << 20);

        let transcript = parse(json!({"action": "transcript", "taskId": "t1"}));
        let agent_tr = validate(&transcript, ExecPolicy::AGENT).expect("agent transcript valid");
        assert_eq!(agent_tr.truncate, Truncate::Tail);
        assert_eq!(agent_tr.max_bytes, 16_384);

        let program_tr =
            validate(&transcript, ExecPolicy::PROGRAM).expect("program transcript valid");
        assert_eq!(program_tr.truncate, Truncate::Tail);
        assert_eq!(program_tr.max_bytes, 4 << 20);

        let cap_ok = parse(json!({"action": "start", "command": "true", "maxBytes": 4 << 20}));
        assert!(validate(&cap_ok, ExecPolicy::AGENT).is_ok());
        assert!(validate(&cap_ok, ExecPolicy::PROGRAM).is_ok());

        let cap_over =
            parse(json!({"action": "start", "command": "true", "maxBytes": (4 << 20) + 1}));
        assert!(validate(&cap_over, ExecPolicy::AGENT).is_err());
        assert!(validate(&cap_over, ExecPolicy::PROGRAM).is_err());
    }

    #[test]
    fn wait_ms_bounds_and_defaults_follow_policy() {
        let at_cap = parse(json!({"action": "start", "command": "true", "waitMs": 240_000}));
        assert!(validate(&at_cap, ExecPolicy::AGENT).is_ok());
        assert!(validate(&at_cap, ExecPolicy::PROGRAM).is_ok());

        let over_cap = parse(json!({"action": "start", "command": "true", "waitMs": 240_001}));
        let err = validate(&over_cap, ExecPolicy::AGENT)
            .err()
            .expect("agent must reject waitMs > 240_000");
        assert!(err.contains("waitMs 240001 is out of range 0-240000"), "{err}");
        assert!(validate(&over_cap, ExecPolicy::PROGRAM).is_ok());

        assert_eq!(ExecPolicy::AGENT.resolve_wait_ms(None, false, false), Some(30_000));
        assert_eq!(ExecPolicy::AGENT.resolve_wait_ms(None, false, true), Some(2_000));
        assert_eq!(ExecPolicy::AGENT.resolve_wait_ms(None, true, true), Some(30_000));
        assert_eq!(ExecPolicy::AGENT.resolve_wait_ms(Some(500), false, false), Some(500));

        assert_eq!(ExecPolicy::PROGRAM.resolve_wait_ms(None, false, false), None);
        assert_eq!(ExecPolicy::PROGRAM.resolve_wait_ms(None, false, true), Some(2_000));
        assert_eq!(ExecPolicy::PROGRAM.resolve_wait_ms(None, true, false), Some(30_000));
        assert_eq!(ExecPolicy::PROGRAM.resolve_wait_ms(None, true, true), Some(30_000));
        assert_eq!(ExecPolicy::PROGRAM.resolve_wait_ms(Some(500), false, false), Some(500));
    }

    #[test]
    fn head_tail_rejected_when_inline_markers_disabled() {
        for req in [
            json!({"action": "start", "command": "true", "truncate": "head_tail"}),
            json!({"action": "poll", "taskId": "t1", "truncate": "head_tail"}),
            json!({"action": "kill", "taskId": "t1", "truncate": "head_tail"}),
            json!({"action": "transcript", "taskId": "t1", "truncate": "head_tail"}),
        ] {
            let parsed = parse(req);
            assert!(validate(&parsed, ExecPolicy::AGENT).is_ok());
            let err = validate(&parsed, ExecPolicy::PROGRAM)
                .err()
                .expect("program policy must reject head_tail");
            assert!(err.contains("head") && err.contains("tail"), "{err}");
        }

        for mode in ["head", "tail"] {
            for req in [
                json!({"action": "start", "command": "true", "truncate": mode}),
                json!({"action": "poll", "taskId": "t1", "truncate": mode}),
                json!({"action": "kill", "taskId": "t1", "truncate": mode}),
                json!({"action": "transcript", "taskId": "t1", "truncate": mode}),
            ] {
                assert!(validate(&parse(req), ExecPolicy::PROGRAM).is_ok());
            }
        }
    }

    #[test]
    fn list_max_bytes_and_truncate_rejected_when_inline_markers_disabled() {
        for (field, req) in [
            ("maxBytes", json!({"action": "list", "maxBytes": 1024})),
            ("truncate", json!({"action": "list", "truncate": "head"})),
            ("truncate", json!({"action": "list", "truncate": "tail"})),
            ("truncate", json!({"action": "list", "truncate": "head_tail"})),
        ] {
            let parsed = parse(req);
            assert!(validate(&parsed, ExecPolicy::AGENT).is_ok());
            let err = validate(&parsed, ExecPolicy::PROGRAM)
                .err()
                .expect("program policy must reject list maxBytes/truncate");
            assert!(err.contains(field) && err.contains("list"), "{err}");
        }
    }
}
