# fastexec reference and architecture

Authoritative runtime usage rules live in
[`src/description.md`](../src/description.md), service wiring in
[`src/service.rs`](../src/service.rs), and request parameter validation in
[`src/service/schema.rs`](../src/service/schema.rs).

## 0. Build and deployment

### MCP compatibility boundary

Broad compatibility with MCP clients is a core product contract. `fastexec`
supports multiple protocol revisions over stdio and Streamable HTTP across both
the `initialize` / `notifications/initialized` session lifecycle and the
stateless `server/discover` / inline-metadata lifecycle, plus `2026-07-28` MCP
Events (`events/list`, `events/poll`, `events/stream`; section 7). Their
protocol paths, client fixtures, and release smoke coverage are part of the
supported external interface.

Internal refactors avoid migration layers, legacy compatibility shims, and
unexplained lint suppressions while keeping these external MCP interfaces intact.

### Platform notes

Linux release binaries link musl statically and run on Alpine without a glibc
compatibility package. Install GNU bash with `apk add bash` on Alpine or
`pkg install bash` on FreeBSD. The Linux kernel and process requirements are in
[process-tree ownership](#5-bash-launch-and-process-tree-ownership).

Builds require Rust 1.92 or newer (Rust 2024 edition) and a C compiler.
Linux musl builds need `musl-tools`. FreeBSD source builds need FreeBSD 15.1 or
newer, `llvm`, and `LIBCLANG_PATH` set to its library directory, such as
`/usr/local/llvm21/lib`. The FreeBSD archive is built and tested on 15.1.
Shared runtime directories need atomic no-replace directory renames; UFS and
ZFS are tested.

The macOS binaries are unsigned. After verifying the archive checksum, use
`xattr -d com.apple.quarantine fastexec` if macOS blocks the extracted binary
because the browser marked it as quarantined.

### Several servers in Pi

One config can hold a local stdio server and a remote HTTP one under different
keys. Pi treats `-` and `_` in server names as the same character, so names that
differ only in those collide.

```json
{
  "mcpServers": {
    "fastexec_local": {
      "command": "/absolute/path/to/fastexec",
      "exposure": "direct"
    },
    "remote_linux": {
      "url": "https://build.example.com/mcp",
      "headers": {"Authorization": "Bearer ${FASTEXEC_TOKEN}"},
      "exposure": "deferred"
    }
  }
}
```

`${NAME}` reads a variable from Pi's environment. Use Pi's `codemode` or
`deferred` exposure for a remote server. If a `direct` server misses the first
prompt's 10-second wait, Pi adds its tools mid-session.

### Run HTTP under a service manager

A systemd user unit shares `XDG_RUNTIME_DIR` with the user's shells, so
`fastexec daemon status` and `stop` reach the daemon it started:

```ini
# ~/.config/systemd/user/fastexec.service
[Unit]
Description=fastexec daemon

[Service]
Type=exec
ExecStart=%h/.local/bin/fastexec daemon run --http 127.0.0.1:8765
EnvironmentFile=%h/.config/fastexec.env
WorkingDirectory=%h
Restart=on-failure

[Install]
WantedBy=default.target
```

`~/.config/fastexec.env` holds `FASTEXEC_HTTP_TOKEN=<token>` and
`FASTEXEC_HTTP_ALLOW_REMOTE=1`. Then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now fastexec
loginctl enable-linger <user>
```

`loginctl enable-linger` starts the unit without a login session.

On Windows, [nssm](https://nssm.cc/usage) runs the daemon under the user's own
account so tasks use that user's profile and permissions:

```powershell
nssm install fastexec "C:\path\to\fastexec.exe" "daemon run --http 127.0.0.1:8765"
nssm set fastexec AppDirectory "C:\Users\<user>"
nssm set fastexec AppEnvironmentExtra "FASTEXEC_HTTP_TOKEN=<token>" "FASTEXEC_HTTP_ALLOW_REMOTE=1" "LOCALAPPDATA=C:\Users\<user>\AppData\Local"
nssm edit fastexec
nssm start fastexec
```

`nssm edit` sets the Log on account without putting a password on the command
line. Every environment entry goes in one `AppEnvironmentExtra` command, because
a later one replaces the block. The runtime root comes from `LOCALAPPDATA`, so
the service and the user's shell must see the same value: set it explicitly so
the service does not inherit an administrator's profile. A service runs in Session 0
without a desktop, so an app that needs a visible window belongs in Task
Scheduler "at logon" instead.

### Behind a proxy

If the proxy forwards the public `Host`, set `FASTEXEC_HTTP_ALLOW_REMOTE=1`
even on a loopback bind. Strip the external path prefix so `/mcp` and
`/t/<id>` reach the daemon.

To serve several machines on one domain, give each daemon a path prefix.
Set `FASTEXEC_HTTP_TOKEN` in the daemon's environment before running this
example. `FASTEXEC_PUBLIC_URL` sets the public root for transfer links:

```bash
FASTEXEC_TRANSFER=true \
FASTEXEC_PUBLIC_URL=https://example.com/mac/ \
FASTEXEC_HTTP_ALLOW_REMOTE=1 \
fastexec daemon start --http 127.0.0.1:8765
```

```nginx
location /mac/ {
    proxy_pass http://127.0.0.1:8765/;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto $scheme;
    client_max_body_size 4m;
}
```

Use a separate prefix and port for each daemon. MCP clients use `<root>mcp`,
for example `https://example.com/mac/mcp`.

Cloudflare reaches the daemon through a named tunnel; Quick Tunnels do not
support server-sent events. Plan limits are in
[Cloudflare's connection limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/).

## 1. Tool actions and parameter limits

Each tool accepts a single JSON object (`deny_unknown_fields`, `camelCase`),
using a portable JSON Schema without root combinators and with flat scalar or enum
types. `null` values are treated as absent everywhere, and `false` booleans
count as absent in inapplicable-parameter and `screen` combination checks while
retaining their meaning where applicable (`eof: false`, `loginShell: false`).
Other inapplicable values or out-of-range parameters return an error before
execution, naming `exec` actions in lowercase.

| Tool | Parameters | Summary |
| --- | --- | --- |
| `exec` | `action` plus action-specific fields | Runs bash commands as tasks. |
| `codemode` | `code`, `cwd`, `waitMs`, `returnWhen`, `killAfterMs`, `truncate`, `maxBytes` | Runs async JavaScript as a task when `FASTEXEC_CODEMODE=true`. |
| `read` | `path`, `offset`, `limit` | Reads a text file, or attaches an image, audio, PDF, or video file. |
| `write` | `path`, `content` | Saves a complete replacement of a regular file. |
| `edit` | `path`, `edits` | Applies exact text replacements to a UTF-8 file. |
| `transfer` | `path` | Publishes one regular file at a `/t/<id>` URL when `FASTEXEC_TRANSFER=true`, returning the capability URL, path, and current byte size. |

### `exec` actions

| Action | Allowed Parameters | Summary |
| --- | --- | --- |
| `start` | `command`, `cwd`, `pty`, `loginShell`, `input`, `eof`, `killAfterMs`, `keepTree`, `cols`, `rows`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Rejects NUL bytes in `command` before execution, then spawns it in GNU bash (`loginShell: false` default with the session's login-environment snapshot; `loginShell: true` runs `bash -l -c`) and waits up to `waitMs` (default `30000`, range `0-240000` on direct calls; unbounded by default without `returnWhen` in nested `codemode` calls). Pipe mode defaults stdin to `/dev/null`, writes `input` (`<= 1` MiB UTF-8) and closes stdin unless `eof: false`, or keeps stdin open with `eof: false`; PTY mode rejects `input` and `eof: true` on `start` (`eof: false` is a no-op) and defaults size to `120x30`. |
| `poll` | `taskId`, `cols`, `rows`, `input`, `keys`, `keyDelayMs`, `eof`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Normalizes `keys: []` (and any accompanying `keyDelayMs`) to absent before validation, resizes the terminal (PTY only), writes `input` (`<= 16` KiB), presses `keys` (PTY only), applies `eof` (pipe only, idempotent when already closed), waits (`2000` ms with a resize, `input`, or `keys` and no `returnWhen`; `30000` ms on direct calls or with `returnWhen`; unbounded by default on nested `codemode` waits without `returnWhen` or input), and returns unseen output. |
| `kill` | `taskId`, `truncate`, `maxBytes`, `waitMs` (ignored) | Terminates the task's process tree (waits up to `5` s) and returns unseen output through the reader gate without `waitEndedBy` (`head_tail` / `16384` B default on direct calls; `head` / `4194304` B default on nested calls). On a finished `keepTree` task, ends held processes while keeping the root exit result. |
| `list` | `scope`, `state`, `limit`, `parentTaskId`, `truncate`, `maxBytes` | Lists bare task IDs newest first filtered by `scope` (`"session"` default, or `"all"` by default when `parentTaskId` is set), `state` (`"active"` or `"finished"`; omitted lists both), and `limit` (`1-500`, default `20`), reporting `totalCount` before `limit`. `truncate` and `maxBytes` apply to direct MCP calls only and fail validation in nested `codemode` calls. Tree markers follow `[<state>]` before elapsed time and command. Without `parentTaskId`, folds `codemode` children into their parent's summary (including `queued`/`cancelled` start counts and `kept ended` when kept child processes exited) and indents up to `8` active children below it; with `parentTaskId`, lists that script's children flat. Creates no readers; direct calls mark only delivered fragments (and folded finished children when the parent summary fits), while nested calls mark only tasks started by the current script. |
| `transcript` | `taskId`, `truncate` (default `"tail"`), `maxBytes`, `waitMs` (ignored) | Reads a UTF-8 PTY task's terminal transcript from the first unseen final line, followed by pending lines. Does not wait or advance the `poll` cursor. |

### Parameter rules

#### `taskId` and readers

Bare task IDs are `<8-char epoch>-<1..=13-char seq>`
in Crockford base32; in daemon mode a well-formed ID with a foreign epoch
reports that it came from an earlier daemon, whereas malformed IDs report
`Unknown taskId`. `start`, `poll`, `kill`, and `transcript` return
a reader handle, `<taskId>.<reader>` (four Crockford base32 characters), in
the status line (and as `taskId` in nested `codemode` results). Reusing a handle in the same session
preserves its cursor; passing a bare task ID creates a new reader from the
beginning of the retained log. An unknown valid reader key is recreated under
that key, including after eviction or a session ending. Each task entry holds
at most `16` readers, evicting the least recently used idle reader when
needed; an active reader, a waiter, or a cancelled call's still-running
blocking read cannot be evicted. If every reader is busy, attachment is
refused. A new attachment's first delivered output (including on `kill`),
transcript, or screen result says `new reader`; cancelled calls leave that
notice pending. Log and transcript range reads reject premature EOF. Reader
cursors and decoding state commit only after complete rendering succeeds;
failed or cancelled reads leave unseen output available for the next call.
Retention and output limits apply to each reader.

#### `list` grouping and `parentTaskId`

Each bash task started from a `codemode` script records `Task.parent_task_id`
(immutable after creation; `None` for direct bash starts and `codemode`
scripts). Without `parentTaskId`, `Tasks::listed` runs one pass over the scoped
tasks (`registry.tasks` for `scope: "all"`, `session.touched` for
`scope: "session"`):

- Every `list` row (top-level, indented active child, promoted orphan, and
  `parentTaskId` flat row) keeps `[running]`, `[exited N]`, and `[killed]`
  for the root process and places any tree marker immediately after
  `[<state>]` before elapsed time, `pty`, summary, `owner`, `via`, and
  `command`: ` · keepTree` while running or when `keepTree: true` finished
  without entering a held state, ` · keeps <members>[ (kill failed)]` (PID
  sorted), ` · keeps processes[ (kill failed)]` when held with no enumerated
  non-root members, ` · keeps processes (unconfirmed[, kill failed])` when
  members could not be confirmed, or ` · kept processes ended` once a held
  tree ends.
- Finished children of an eligible parent fold into the parent's inline
  summary (`total`, `done`, `failed`, `warned`, plus `tree_ended` rendered as
  `<n> kept ended` when non-zero; `done + failed + active == total`), paired
  with lock-free `queued` (while running) or `cancelled` (after exit or
  `kill`) start counts. Active children (`running` or `keptRunning`) render
  indented (`  └─`) beneath the parent, capped at `8` per parent (matching
  `START_PERMITS`) with an overflow hint pointing to `parentTaskId` when more
  are active; under `state: "finished"`, `0` children are indented.
- When `state` is not `"finished"`, an eligible parent sorts at
  `max(parent_pos, newest_active_child_pos)` so an older running `codemode`
  with a fresh active child stays at the top of the list without post-`limit`
  re-slicing. `totalCount` is the top-level item count before `limit`.
- A child whose parent is absent from the current view (evicted, outside
  `scope: "session"`, finished/killed under `state: "active"`, or running
  under `state: "finished"`) is promoted to a top-level row tagged `via <pid>`
  when the child matches `state`.
- Passing `parentTaskId` (bare task ID or reader handle, normalized via
  `parse_handle`) lists that script's children flat without folding. Omit
  `state: "active"` when inspecting `kept ended` children. When
  `scope` is omitted, drill-down searches all sessions; explicit
  `scope: "session"` filters to the caller's session. Drill-down keeps
  working after parent eviction while any child with that `parent_task_id`
  remains in `registry.tasks`. A bash task ID returns `"No tasks."`; an
  unknown ID with no remaining children errors naming `parentTaskId`.

#### `waitMs` and `killAfterMs`

`waitMs` (`0-240000`) bounds only the current call's wait. A call that hits
`waitMs`, matches `returnWhen`, or is cancelled leaves the task running (use
`list` to recover the `taskId` of a cancelled `start`). `killAfterMs` on
`start` arms an independent timer that kills the owned tree, including kept
processes. Expiry while the root runs reports `state: "killed"` and
`lifetimeExpired: true`; after root exit, held processes end and the root's
original result stays.

#### `returnWhen`

Ends the wait early when any condition holds (`waitEndedBy` reports
`{ condition, text? }`):

- `outputContains`: `1-16` literal case-sensitive strings (`1-4096` bytes each,
  without line breaks) matched against unseen output lines: cleaned for bash
  (including text before a carriage return overwrites it), unchanged for
  codemode.
- `screenContains`, `screenAppears`, `screenGone` (UTF-8 PTY only): match
  against the live screen at the task's size (`screenAppears` and `screenGone`
  compare against the screen captured before the call's resize and `input`).
- `quietMs`: `10-60000` ms without output after `input` and `keys` finish.

#### Window and encoding options

- `maxBytes`: `100-4194304` (`4` MiB; default `16384` on direct `exec` and
  outer `codemode`, `4194304` on nested `tools.exec`). Bounds the output body
  only; the status line, background footer (`<= 512` bytes), metadata,
  errors, and correctness warnings stay outside that budget. Rejected on
  nested `list` calls.
- `truncate`: Direct MCP calls (`ExecPolicy::AGENT`) default to `"head_tail"`
  (`"tail"` for `transcript`), inject inline omission and cut markers
  (`... [<k> lines omitted: log lines <a>-<b>] ...` and
  `<prefix> … [line cut at <B>; <n> more bytes]`, plus
  `... [<n> top rows omitted] ...` and ` … [cut]` for `screen: true`), and
  drop NUL (`0x00`) bytes when cleaning (`keep_nul: false`). Multi-line
  `head_tail` overflow reserves the inline marker and `B / 10` for the head,
  fills the tail from the last line backward (cutting the last line if
  needed), and returns unused tail bytes to the head. Nested `codemode` calls
  (`ExecPolicy::PROGRAM`) default to `"head"` (`"tail"` for `transcript`),
  preserve NUL bytes in clean output (`keep_nul: true`, so `find -print0` and
  `rg -0` parse cleanly), reject
  `truncate: "head_tail"` and `list` `truncate`/`maxBytes` with
  `ToolError` (`code: "VALIDATION"`), and omit all inline markers
  (`inline_markers: false`). When a single line or screen row exceeds the
  budget in Program mode, it slices cleanly at a UTF-8 boundary and reports
  `truncated`, `omittedLines`, `omittedRange`, `cutLines`, and `omittedRows`
  in the control channel. Neither path enforces a fixed per-line length cap.
  Omitted ranges on direct calls also appear in the status line, outside
  the body budget.
- `raw`: Skips terminal-output cleaning. Nested `raw: true` output preserves
  ANSI/control sequences and line endings, while decoding bytes using `encoding`
  (UTF-8 by default). Invalid sequences become replacement characters;
  `encodingErrors` counts affected lines. The task log retains captured bytes
  under its normal storage limits.
- `encoding`: Any ASCII-compatible WHATWG label (such as `"utf-8"`, `"big5"`,
  `"gbk"`; UTF-16 is rejected). On `start`, sets the task default; on `poll`,
  overrides for that call.
- `screen`: Boolean (`false` default, UTF-8 PTY only). Returns the rendered
  screen at the task's size and 1-based cursor `[row, col]`, marking the output
  stream read up to that point. Cannot combine with `truncate`, `raw`, or
  `encoding`.
- `cols`, `rows` (PTY only): the terminal's size. `start` takes them,
  defaulting `120x30`; `poll` takes them too, because a resize is
  terminal-side input: a poll applies the resize, then `input`, then `keys`,
  then waits. Either axis may come alone and an omitted axis keeps its value;
  bounds are `cols` `20-500` and `rows` `5-200`; an unchanged size is a no-op.
  A pipe task refuses both.

### Codemode

`FASTEXEC_CODEMODE=true` adds `codemode` and includes JavaScript orchestration
guidance in server `instructions`. Its required `code` is an async
function body with top-level `await` and `return`, up to 256 KiB of UTF-8,
nonempty after trimming and without NUL bytes. Unknown parameters are refused.
It shares `exec start`'s directory, wait, and output options except `raw`:
`cwd`, `waitMs`, `killAfterMs`, `truncate` (default `head_tail`, keeping lines
uncut up to `maxBytes`), `maxBytes` (`100-4194304`, default `16384`), and
`returnWhen` (`outputContains`, `quietMs`). Schema-decoding,
parameter-validation, and admission failures return a text tool error
(`isError: true`, no `structuredContent`) without creating a task.

A script is a task with `kind: "codemode"`. The start result has
`action: "codemode"`; later `exec poll`, `kill`, and `list` use the same task
and reader handles (`kill` keeps short status receipts within the `8` MiB
control reserve and defers larger pending attachments to inputless `poll`). A list command is `[codemode] <code>`, flattened and
truncated like bash commands. A script poll rejects `input` (including an empty
string), `keys`, `keyDelayMs`, resize, `encoding`, and true values for `eof`,
`screen`, or `raw`. Optional fields accept `null`. Scripts have no terminal
or process tree and reject `transcript` requests.

The VM uses synchronous rquickjs 0.14.0 on a dedicated thread, with 256 MiB
of VM memory, a 512 KiB JavaScript stack, and a 2 MiB thread stack. A local
server admits four scripts and a daemon eight, within the existing total task
limit. The VM, output reader and capture workers are prepared before admission
is published; a failed preparation returns its capacity and files.

The script globals are `tools.exec/read/write/edit`, `sleep`, `sh`, `text`,
`console`, `attach`, `exit`, and UTF-8 `TextEncoder`/`TextDecoder` (alongside
QuickJS's built-in `atob`/`btoa`). `sleep(ms)` returns a `Promise<void>` without
spawning a process (yielding once when `ms <= 0` and throwing `TypeError` for
non-finite numbers). `sh`, `sh.quote`, and `sh.join` are frozen, non-constructible
functions with no own `prototype` property. `sh.quote(value)` formats a string or
finite number as one POSIX single-quoted word and throws `TypeError` for `NaN`,
infinities, NUL bytes (`U+0000`), or other types. `sh.join(values)` takes a dense
array of own elements (validating a single `length` read as a uint32 and checking
each index as an own property), formats each element with `sh.quote`, and joins
them with spaces (returning `""` for `[]`). Tagged `` sh`...` `` validates
`strings` as a non-null object with an own `raw` array read once whose uint32
length matches the call arguments, checks each raw segment is an own NUL-free
string, preserves raw static text verbatim, expands array interpolations with
`sh.join`, and quotes scalar interpolations with `sh.quote`, throwing `TypeError`
on any invalid input. There are no modules, Node
APIs, callback timers (`setTimeout`/`setInterval`), store/load, network, or direct
filesystem APIs. `text` appends strings as-is and other values as JSON
(preserved verbatim without ANSI or trailing-space stripping). Console methods
join formatted arguments with spaces and show Error stacks. A non-undefined
return value must be a string or JSON-serializable and is appended last (values
for which `JSON.stringify` throws or returns `undefined`, such as `Symbol`,
functions, `BigInt`, or circular objects, end the script with exit `1` and
`Script error: TypeError: the return value is not JSON-serializable`). `exit()`
ends successfully even inside a try/catch. Each script owns its globals; bridge
and codec functions bind the built-ins they depend on before user code runs.

Nested calls bypass MCP formatting and `CallToolResult` completely, resolving
directly to typed objects (no `ok` wrapper). Their data channel (`output` on
`exec`, `text` on `read`) is raw with zero status lines, footers, line-number
prefixes, or inline omission/cut markers; truncation and diagnostics appear in
explicit control fields. Wrong argument shapes (such as non-object arguments or
non-finite `sleep` values) reject with `TypeError`. Operational, validation,
quota, overload, cancellation, edit, and I/O failures reject with `ToolError`
(`name: "ToolError"`, `tool`, `code`, `message`, `result`):

- `"VALIDATION"`: argument or schema validation failures, including
  `truncate: "head_tail"`, `list` `maxBytes`/`truncate` in Program mode, and
  keys that the current keyboard mode cannot encode.
- `"NOT_FOUND"`: unknown `taskId` / `parentTaskId` or missing file.
- `"OVERLOAD"`: task or reader capacity, input-queue backpressure, or active
  byte-pool or bridge-input memory overload (`err.result` carries
  `{ taskId, logPath }` when the failure belongs to an already-created task).
- `"LIMIT"`: pending-call cap (`1024`), `256` MiB pending bridge budget, `4096`
  start cap, oversized result, or strict whole-file `read` size limit.
- `"CANCELLED"`: cancelled call.
- `"EDIT_FAILED"`: `tools.edit` match, ambiguity, overlap, or no-op failure
  (`err.result` carries `{ path, failures }`).
- `"IO"`: file I/O errors and task I/O failures (`task_io_ok == false`, i.e.,
  `inputError`, `captureError`, or `logError`), where `err.result` preserves
  the full structured exec report (`output`, `exitCode`, `state`, `taskId`,
  `logPath`, error fields, `warnings`).

Earlier input may already have been delivered when a poll hits input-queue
backpressure or a keyboard-mode change. Delayed-key errors report how many keys
were sent. Input queued before a cancellation or timeout remains an `"IO"`
failure when delivery is unconfirmed. It can still take effect later, so do not
replay it blindly.

Non-zero command `exitCode`s resolve normally. Async rejections from `tools.*`
and `sleep` carry the caller's script stack frame (`codemode.js:<line>:<col>`).
`exec` returns `{action, taskId, kind, state, exitCode, pty, elapsedMs, output,
truncated, logPath, ...}` (defaulting to `truncate: "head"`, or `"tail"` for
`transcript`, and `maxBytes: 4194304`, keeping NUL bytes in clean output and
waiting with no `waitMs` ceiling unless `waitMs`, `returnWhen`, or
input/resize/keys supplies one); text `read` uses `strict_whole_file: true`
when both `offset` and `limit` are omitted (returning the whole UTF-8 file up
to `4` MiB or rejecting with `ToolError` when larger or non-UTF-8), and with
`offset` or `limit` reads a slice up to `4` MiB (no line cap, marking invalid
UTF-8 with `lossy: true`), always returning `{kind: "text", path, text,
startLine, endLine, shownLines, totalLines, truncated, nextOffset?,
lineTooLong?, lossy?}`; media `read` (image, audio, PDF, video) uses
`MediaReadMode::DescriptorOnly`, returning `{kind, path, mimeType, bytes,
attachable: true}` or `{kind, path, mimeType, sourceBytes?, attachable: false,
note}` without sending base64 into QuickJS (`attach()` throws `TypeError` with
`note` when `attachable` is `false`); `write` returns `{path, bytes}`; and
`edit` returns `{path, replacements, edits}` (`edits: [{ line, fuzzy }]`).
Each script has `8` `exec start` slots and `8` file/transcript I/O slots
(`read`, `write`, and `edit` hold their slot inside the blocking worker until
disk I/O finishes; `poll`, `kill`, `list`, and `sleep` take no slot), within
`1024` pending calls. Starts waiting for a slot are admitted in script
submission order. Additional calls past the pending limit, the `256` MiB
pending bridge budget, or the `4096` start cap reject with `ToolError`
(`code: "LIMIT"` or `"OVERLOAD"`). A completed result that cannot fit rejects
after the operation ran; its error identifies this case, and the operation
remains in effect. A script awaiting a promise with no pending tool call,
`sleep`, or JavaScript job fails because it has no way to settle.

Calls use the script's session and launch context (with `cwd` overridden when
the script sets `cwd`). A nested task's reader can be polled by the caller, and
IPC children retain the outer call ID. Nested calls advance their reader state.
Nested poll, kill, and transcript leave background notifications pending for
tasks started outside the script; completion observed for a script's own tasks
marks their session entries reported. Nested `tools.exec({ action: "list" })` uses the same grouping and
`parentTaskId` contract as direct `exec list`, folding `codemode` children
under their parent unless `parentTaskId` is passed. It follows the same
ownership boundary as other nested `exec` calls: it marks only existing session
entries for tasks started by the current script (checking top-level entries,
active children, and folded finished children independently), never writes
`keeps_reported`, and leaves outer session notifications pending.
When a script
ends, in-flight nested calls are cancelled and drained before publishing the
final script report, so any `exec start` already inside process creation is
recorded in `startedTasks`. Each script accepts at most 4096 validated exec
start calls, counting calls that later fail or are cancelled. Further starts
reject with `ToolError` (`code: "LIMIT"`), so the complete `startedTasks` history stays bounded.

Script output streams through the existing pipe capture and task log. The
64 MiB per-script text limit reserves 4 KiB for final diagnostics, while stored
attachments count separately under the 4 GiB server task quota and 1024-entry
per-task cap. Normal completion and `exit()` have exit code 0;
syntax errors, uncaught exceptions, unhandled rejection, stalled promises
and resource failures have exit code 1 with a `Script error:` line. These are
normal task results (`isError: false`). Stop has state `killed`, null exitCode,
and a line naming cancelled calls; deadline stops also set lifetimeExpired.

Error descriptions borrow QuickJS strings and copy at most 4 KiB into Rust,
ending with `...` when truncated. Before the VM starts, the worker reserves
12 KiB from the control pool for the description and pending-call summary;
it holds that workspace until the VM has stopped and its final diagnostic has
been written. The final diagnostic streams its segments without a second
assembled copy and stays within the reserved 4 KiB of task output. If that
workspace is unavailable, the script returns a fixed error without running.

The script's cancellation token is independent of a request's waiting token.
Kill, deadline, session end in local mode, or daemon stop interrupt bytecode
and the job loop, cancel pending nested calls and wake an idle VM. The final
state is published after capture drains. Any script ending cancels calls it
did not await. Completed operations remain in effect; bash tasks it started
remain independent tasks. `Control` tracks unspawned `exec start` calls with
two lock-free `AtomicUsize` counters (`queued_starts` and `cancelled_starts`)
managed by `QueuedStartGuard`. `Host::call` constructs an armed guard on the VM
thread right after admitting a validated `Action::Start` (incrementing
`queued_starts`) and moves it through `Call` and `StartArgs` into
`Tasks::start`, which disarms it under `lock(&self.registry)` right after
pushing the new `Task`. If the guard drops while still armed (because script
cancellation fired while waiting for `START_PERMITS`, or spawn failed), `Drop`
decrements `queued_starts` and increments `cancelled_starts` when the script is
cancelled. `ScriptHandle::list_counts()` reads both atomics without acquiring
`Control.report`, keeping queued and cancelled counts disjoint from spawned
`Task` entries with no spawn gap or double-counting. `codemode.cancelledCalls`
identifies calls cancelled on termination, and `codemode.startedTasks` lists
all registered handles with states queried at each response. Status lines show
up to three still-running handles, append `; list active with exec list
parentTaskId="<bare_id>" state="active"` on both `killed` and non-killed exits
when more than three children are still running, and report when a kill has
waited five seconds without completion.

`attach(value)` accepts an opaque task attachment handle from nested `exec poll`
or `kill`, a file path string or `{path}` media descriptor, an inline MCP
`image`/`audio`/`resource(blob)` block, or a base64 `data:` URL, and returns
`{index, kind, mimeType, bytes}` with a monotonic 1-based task-local `index`.
Path inputs reopen the regular file from byte 0; inline inputs verify 8 KiB
container evidence and allow a declared MIME only as a controlled container hint,
rejecting conflicts. When `FASTEXEC_IMAGE_COMPRESS=true` for the session,
compressible images scale to at most 2000 pixels on the long side within 4.5 MiB
of base64; otherwise every supported media type is stored verbatim up to
`FASTEXEC_MAX_MEDIA_BYTES` (`100` MiB default, `1` MiB to `360` MiB). Each task
stores up to `1024` attachments within the `4` GiB total task output quota.

Outer responses pack unseen attachments greedily in contiguous `index` order
under the `512` MiB outbound frame ceiling (`8` MiB control reserve on `kill`)
and the `1` GiB process active-byte pool, committing the text cursor and
`attachments_seen` watermark atomically before transport delivery. Remaining
attachments stay queued for subsequent inputless `exec poll` calls even after
text reaches EOF. A single attachment that exceeds the outbound frame ceiling on
its own advances the watermark only alongside a visible warning naming its
`index` and retained path. Outer responses always append
`Attached: #<index> (<mime>, <bytes> B)` to the text summary alongside the
media `content[]` blocks (with no `structuredContent`). Nested `exec poll` and `kill` return unforgeable
script-scoped handles in `result.codemode.attachments`, holding short-term read
leases across source-task eviction until the calling script exits or cancels.
Repeated reads of the same source attachment reuse its handle within that script.
The active byte pool charges each retained attachment's metadata and the handle
table's allocated capacity until script cleanup; failed delivery rolls back new
registrations and keeps earlier handles valid. If a transport write is interrupted,
attaching a fresh reader via the bare `taskId`
re-reads retained output and attachments from index `1` for deduplication by
`index`.

### File tools

#### `read`

Direct calls show up to `2000` lines and `50` KiB of a text file
(nested `codemode` calls without `offset` or `limit` return the whole UTF-8
file up to `4` MiB or reject with `ToolError` when larger or non-UTF-8, and
with `offset` or `limit` read up to `4` MiB without a line cap). `offset` is a
  non-zero integer (`1`-indexed from the start when positive, or counting from
  the end when negative with `-1` naming the last line; `0` is rejected). If the file
  changes the starting line of a negative offset during the scan, the reader
  retries once and reports an error if the starting line changes again. The
  `limit` (`>= 1`) bounds the number of lines. Directories report `Cannot read <path>: it is a directory; list it with exec.` Line counts
  follow real `\n` / `\r\n` lines (`""` has `totalLines: 0`, `"a\n"` and `"a"`
  have `totalLines: 1`, and `offset: 1` on an empty file returns `""` with
  `startLine: 1`, `endLine: null`, `shownLines: 0`, `truncated: false`), preserving original line
  terminators in the returned slice and emitting truncation or lossy notices
  without leading blank lines when the body is empty. Recognized media files
  (PDF, PNG, JPEG, GIF, WebP, BMP, TIFF, HEIC/HEIF, WAV, FLAC, AIFF, Ogg
  Vorbis/Opus/FLAC/Speex, MP3, ADTS AAC, M4A, MP4, QuickTime MOV, 3GP, WebM, and
  AVI, classified from bounded `8` KiB container/brand headers) attach directly
  up to `FASTEXEC_MAX_MEDIA_BYTES` (`100` MiB default, `1` MiB to `360` MiB),
  returning verbatim bytes by default (`FASTEXEC_IMAGE_COMPRESS=false`) or
  scaling compressible images when `FASTEXEC_IMAGE_COMPRESS=true`. Files over
  `FASTEXEC_MAX_MEDIA_BYTES` or rejected during opt-in compression return a text
  notice on direct calls (and `{ kind, path, mimeType, sourceBytes?, attachable:
  false, note }` on nested `codemode` calls), while genuine I/O failures return
  `isError: true` (or `ToolError` in `codemode`). Media reads ignore valid
  `offset` and `limit` values. A non-regular file, and one
  that is not a recognized media format and has a NUL byte in its first `8` KiB, are
  refused (binary files suggest `exec`, and also `transfer` when
  `FASTEXEC_TRANSFER=true` for that session). Slices report invalid UTF-8 with
  replacements and a note. A nested whole-file read requires valid UTF-8
   throughout.

#### `write`

Saves `content`, creating missing parent directories of the final
  target, including a dangling symlink's resolved target. Relative, absolute,
  and chained symlinks are preserved; `edit` still requires an existing file.
  Content is staged in a private sibling file and synced before it replaces the
  destination, so a failed content write leaves the original intact. The parent directory
  must be writable. Existing owner, permissions, and access-control metadata
  are preserved; symlinks keep their normal target semantics. Non-regular files
  and files with multiple hard links are refused: use `exec` for a deliberate
  in-place update of those files. On Windows, a caller unable to retain a
  different owner or group gets an error before replacement. If native file
  replacement and its rollback both fail, the original stays at the recovery
  path named in the error so the last copy is not deleted.
  macOS saves use native ACL APIs: private staging suppresses inherited grants
  during creation, then owner, group, mode, and the complete original ACL are
  restored together. A volume that ignores ownership or cannot provide the
  required private staging and permission preservation is refused before
  publication; `exec` remains available for an explicit in-place operation.
  FreeBSD stages inside a new random sibling directory, removes inherited ACLs
  while that directory is still empty, checks its owner, `0700` mode and trivial
  ACL, then creates a `0600` file inside it. Completed saves preserve owner,
  group, mode, native POSIX.1e or NFSv4 ACLs, and USER extended attributes;
  chmod precedes ACL restoration so ZFS cannot discard the restored grants.
  SYSTEM attributes are copied when the caller can inspect them, excluding
  native ACL storage that is copied through the public ACL APIs. When the kernel
  reports active vnode MAC labeling, existing files are not atomically replaced,
  even by root: copying persistent attributes does not relabel an in-memory
  vnode. New files retain the kernel-created SYSTEM attributes while MAC labeling
  is active. `exec` remains available for an explicit in-place update.
  Administrator-only application metadata in the SYSTEM namespace requires a
  sufficiently privileged server to preserve it. Attribute
  lengths are checked again after reading so a concurrent growth cannot be
  saved as a truncated value.

#### `edit`

Each `edits[]` entry is `{oldText, newText, startLine?, endLine?}`,
  matched against the original file, not against the result of an earlier entry.
  When `startLine` and/or `endLine` (`1`-based inclusive LF line bounds;
  `startLine > endLine` is rejected) are given, only exact or fuzzy candidates
  whose start line falls within that range match, and `not_found` lists up to
  `20` out-of-range candidate start lines in `lines`. Every `oldText`
  must match at least once (`not_found`), be unique over every start position in
  range including overlapping ones (`ambiguous`, reporting `lines` in match order with
  repeats kept), and not overlap any other matched span (`overlap`, checking
  every pair of overlapping spans and listing
  partner indices in `with`). `lines` and `with` cap at `20` items (`more`
  counts any remainder), and a batch that matches uniquely without overlap while
  leaving the file unchanged fails with `failures: []`. `edit` evaluates every
  entry in the batch and reports all failures together
  (as a text error on direct calls, or `ToolError` with `code: "EDIT_FAILED"`
  and `result: { path, failures }` in `codemode`), counting line numbers on
  real `\n` breaks so they match `read`. The file must be UTF-8; a byte-order mark
  and every unmatched byte are kept, including mixed CRLF, LF, and CR line
  endings, and inserted text follows the file's first line ending style. The
  success result names each replaced block's first line in the original file,
  so earlier insertions or deletions do not shift later reported lines, and marks
  fuzzy matches. Saving uses the same staged replacement and file
  restrictions as `write`. Both `read` and `edit` validate the opened file as
  regular; an edit verifies the target's identity, contents, and path again
  before publishing.

#### `transfer`

Resolves `path` with the unified path rules, refuses
directories and non-regular files (hinting `tar` via `exec`) and non-missing
metadata errors, and returns plain text with the capability URL, path, and
byte size snapshot at mint time (or noting the file does not exist yet).

#### Path resolution and serialization

Every tool `cwd` and `path` resolves through one path helper (`src/files/path.rs`):
expands `~` / `~/...`, resolves relative paths against the effective `cwd`
(a `codemode` call's `cwd` or the session's working directory), strips `.` and
repeated separators while leaving `..` to the OS kernel so symlinks resolve
as the filesystem sees them, accepts Windows `C:/...`, `C:\...`,
`//server/share/...`, and Git Bash `/c` or `/c/...`, refuses Windows
driveless rooted paths (`/tmp/...`, hinting `cygpath -m`) and drive-relative
paths (`C:foo`), and formats every reported path with forward slashes
(stripping `\\?\` verbatim prefixes and rendering UNC paths as
`//server/share/...`).

`write` and `edit` serialize with each other inside one server
process. They do not coordinate with `exec`, with a `transfer` upload, or with
another process.

## 2. Host configuration and response format

### Progress notifications and timeouts

When a request carries a `progressToken`, the server emits progress every
`20` seconds during waits. Clients that refresh their timeout on progress,
including Pi's built-in MCP client, can keep long waits alive.

Other hosts need a tool timeout of at least `300` seconds. In the default mode,
keep the MCP server connected for the task's lifetime: an idle disconnect ends
its tasks. In `on` mode a disconnect ends nothing; the daemon keeps the tasks
(section 6).

### Direct MCP responses and Codemode programmatic results

Direct MCP tool calls (`exec`, `codemode`, `read`, `write`, `edit`, and
`transfer`) return `content` text and media blocks with `structuredContent`
omitted (`None`). Status lines, inline omission and cut markers, `Attached:`
summaries, warnings, and background footers appear in the text body.

Nested `codemode` calls (`tools.exec`, `tools.read`, `tools.write`,
`tools.edit`) skip MCP presentation and `CallToolResult` completely. They
encode typed DTOs directly in `src/codemode/encode.rs` so JavaScript callers
receive raw data (`output` or `text`) and explicit control fields without
Fastexec-injected prose.

`src/service.rs` prepares domain results for both frontends. Shared snapshots,
resource leases, task reports, and diagnostics live in `src/service/core.rs`;
Agent text and MCP media packing live in `src/service/render.rs`. The Program
encoder imports the shared core directly. An explicit `Consumer` selects startup
cancellation and session-reporting rules independently of `ExecPolicy`; `View`
contains output options. Task failures retain their overload, cancellation, or
I/O kind from the source through the Service and Program encoder.

### `FASTEXEC_DAEMON`

`"off"` (default) keeps tasks in this server's own session. `"on"` forwards
every tool call to the namespace's daemon (section 6). The value is read at
startup; an invalid value exits with status `1`.

The daemon builds up to two ordinary IPC results at once. `kill` and `list`
each have their own slot. Starting and waiting for tasks take no slot. If no
ordinary slot is free when output is ready, the result gives the reader handle
and log path without advancing the reader. Poll that handle for output only or read
the log. Do not replay a start, script, or poll input. You can request a
transcript again with the same handle. File and transfer calls can be cancelled
while they wait for a slot, before they start.

A slot stays occupied through encoding, or until a cancelled blocking read
releases its output. The task text window still tops out at 4 MiB, with up to
64 MiB of pending transcript data before windowing. Ordinary outbound buffers and
queued frames share the 1 GiB process active-byte pool alongside an independent
8 MiB control reserve for `list`, short `kill` receipts, progress, and overload
errors. Each IPC result payload can be up to 512 MiB, with a 60 s no-progress
idle write timeout separated from the endpoint's 60 s request send deadline.

### `FASTEXEC_CODEMODE`

`"false"` (default) omits the `codemode` route. `"true"` registers it and adds
script-specific poll and kill rules to the exec description, before its final
Host line. Local stdio and daemon endpoints use their own startup environment;
HTTP uses the daemon's. The daemon accepts forwarded codemode calls regardless
of its advertised setting, within the same session and task permissions as exec.
Invalid values fail stdio startup, daemon start, and daemon run with exit 1,
without blocking `daemon status` or `daemon stop`.

### `FASTEXEC_TRANSFER`, `FASTEXEC_PUBLIC_URL`, and `FASTEXEC_TRANSFER_ADDR`

`FASTEXEC_TRANSFER` defaults to `"false"` across local stdio, daemon endpoints,
and Streamable HTTP, leaving `transfer` unregistered. `"true"` registers
`transfer` and enables binary-file `read` hints naming `transfer`. Invalid
values fail stdio startup, `daemon start`, and `daemon run` with exit `1`
without blocking `daemon status` or `daemon stop`.

- Local stdio (`FASTEXEC_DAEMON=off`): when `FASTEXEC_TRANSFER=false`,
  `FASTEXEC_PUBLIC_URL` and `FASTEXEC_TRANSFER_ADDR` stay unread and no port is
  bound. When `true`, startup validates both (refusing an unspecified
  `0.0.0.0` or `[::]` listener address when `FASTEXEC_PUBLIC_URL` is unset),
  binds a `/t/` listener on `FASTEXEC_TRANSFER_ADDR` (default `127.0.0.1:0`),
  and mints `{base}t/{id}` from `FASTEXEC_PUBLIC_URL` or `http://<bound>/`.
- Daemon stdio (`FASTEXEC_DAEMON=on`): each endpoint advertises `transfer`
  from its own `FASTEXEC_TRANSFER` setting (validating `FASTEXEC_PUBLIC_URL` and
  `FASTEXEC_TRANSFER_ADDR` at startup when `true`) and forwards `"transfer"`
  over IPC. A non-HTTP daemon started with
  `FASTEXEC_TRANSFER=false` leaves `FASTEXEC_PUBLIC_URL` and
  `FASTEXEC_TRANSFER_ADDR` unread and binds no `/t/` port; it binds one shared
  `/t/` listener at startup when both `FASTEXEC_TRANSFER=true` and
  `FASTEXEC_TRANSFER_ADDR` are set in the daemon's environment, or on the first
  valid `"transfer"` call using that session's `FASTEXEC_TRANSFER_ADDR` or
  `127.0.0.1:0` (including when `FASTEXEC_PUBLIC_URL` is set). URL prefix
  precedence is session `FASTEXEC_PUBLIC_URL`, then daemon startup
  `FASTEXEC_PUBLIC_URL`, then `http://<http_addr>/` when `--http` is active,
  then `http://<transfer_listener_addr>/`; an unspecified `--http` or
  `FASTEXEC_TRANSFER_ADDR` address without `FASTEXEC_PUBLIC_URL` is refused.
- Streamable HTTP (`--http <addr>`): uses the daemon's startup
  `FASTEXEC_TRANSFER` setting to decide whether HTTP sessions register
  `transfer`. Behind a reverse proxy, set `FASTEXEC_PUBLIC_URL` to the external
  base URL (such as `https://example.com/mac/`) so minted capability URLs use
  the proxy's public origin and path prefix. The `/t/` route stays mounted on
  the HTTP listener backed by the daemon's shared `Transfers` table, returning
  `404` when no capability exists and serving capabilities minted by
  `FASTEXEC_TRANSFER=true` stdio endpoints on the same daemon. Non-loopback
  `FASTEXEC_TRANSFER_ADDR` values require `FASTEXEC_HTTP_ALLOW_REMOTE=1`, and
  setting `FASTEXEC_TRANSFER_ADDR` alongside `--http` fails startup with exit `1`.

### `FASTEXEC_IMAGE_COMPRESS` and `FASTEXEC_MAX_MEDIA_BYTES`

Both settings validate at MCP serving startup (and per endpoint session on a
shared daemon, where HTTP uses the daemon's environment and each stdio endpoint
sends its own values in `Hello`; `daemon status` and `daemon stop` ignore them).

- `FASTEXEC_IMAGE_COMPRESS`: `"false"` (default) sends recognized images,
  audio, PDF, and video verbatim. `"true"` decodes PNG, JPEG, GIF, WebP, and
  BMP (excluding APNG and JPEG-LS), scales dimensions over `2000` pixels down
  to at most `2000` on the long side, and searches PNG or JPEG quality until
  the encoded payload fits `4,718,592` base64 bytes (`3.5` MiB raw), while
  rejecting images past `32768` pixels per side or `64` megapixels and leaving
  APNG, JPEG-LS, TIFF, HEIC/HEIF, audio, PDF, and video verbatim.
- `FASTEXEC_MAX_MEDIA_BYTES`: maximum raw byte size per media file or
  `codemode` attachment (`104857600` default, accepted range `1048576` to
  `377487360`, or `1` MiB to `360` MiB so `4/3` base64 plus JSON framing fits a
  `512` MiB outbound frame). Live media reads and outbound frames share a
  `1` GiB process active-byte pool (`src/lease.rs`) with non-blocking peak
  reservations covering concurrent raw, base64, and serialized frame buffers,
  leaving an independent `8` MiB control reserve so `list`, short `kill`
  receipts, progress, and overload errors never stall behind slow readers.

### Active buffer ownership

The process byte pool covers application-owned live payload buffers by allocated
capacity, including replacement buffers while the old allocation is still alive.
Official SDK allocations after handoff are outside this accounting. It is not a total RSS
limit: Rust object headers, allocator bookkeeping, task state and terminal history
have separate lifetimes. An admission failure leaves output cursors and attachment
watermarks unchanged. A receipt for an operation that already ran includes its task
handle and log path so the caller can retrieve output without repeating the action.

Task lists reserve snapshot diagnostics, owner/session text, display paths and
member names before copying them, covering every selected top-level task and
attached indented active child (up to `8` per parent). Folded finished children
are summarized from lightweight non-nested lock reads without cloning `Final`,
`leftovers`, `held`, or command strings, keeping list memory O(`limit`). Held-process
`members(tree)` queries for both top-level tasks and indented active children
run after dropping `lock(&self.registry)`. Their rendering workspace covers 32 times the
source capacity plus fixed row/member keys, including simultaneous snapshot, warning,
JSON and growing text buffers. Short lists use the independent control pool;
larger candidates can move to the main pool after acquiring its replacement charge.
Native member queries admit names and conversion buffers before allocation, then
release their temporary scan allowance before rendering the selected names. Repeated
queries reuse a live-byte high-water allowance. On macOS, a budget refusal drains
the current native reply so subsequent queries remain aligned.

Window, transcript and screen responses reserve the same metadata workspace before
copying diagnostics or leftover process names under their state locks. Held-process
queries use the same name/conversion admission callback as lists. The snapshot and
its rendering allowance stay together until the final response payload takes over;
only kill responses may use control space when the main pool is full. After reading
a window, a smaller refresh copies only the four current I/O diagnostics and reads
the line/drop/eviction scalars. It admits the new diagnostics before replacing the
old copies, without cloning final state or process lists again. Cancellation drops
the snapshot before its metadata lease; a queued body worker keeps its separate
workspace until that worker finishes. Refusal leaves reader and attachment marks
unchanged and retains the task handle and recovery instructions in the error.

Background footers classify and order tasks from scalar state without copying their
unused diagnostics or leftover lists. Held-process queries admit their names before
allocation; the notice also reserves its handle and formatting workspace. Comparisons
borrow the previous reported PID list under its lock. Only snapshots actually named
in the footer move into the pending report, with their workspace held through the
report commit. Refusal or a dropped candidate leaves those notices and the outside-task
claim due; the same kill-only control fallback and task recovery errors apply.

A list candidate checks the final frame limit and reserves its payload and
application-owned wire before marking entries reported. `fit_lines` operates on
atomic top-level display blocks (`ListBlock`, pairing a parent header with its
indented active child lines) so `head_tail` and `tail` never separate `  └─`
rows from their parent. If one block alone exceeds `maxBytes`, `fit_lines`
trims indented child lines from the bottom and drops those omitted children
from `delivered_tasks`; if the single header row still exceeds `maxBytes`,
`fit_single_block` degrades it through `fit_row` (keeping ` … [cut]`) and
records which fragments survived (`RowAck`: `root`, `tree_ended`, `keeps`,
`summary`). Omission markers count omitted top-level blocks. HTTP reserves its
payload until handler return; the SDK creates its own encoding afterward.
Cancellation or refusal leaves those marks unchanged. On commit, direct calls
apply each surviving row's `RowAck` (`mark_root_reported`,
`mark_tree_end_reported`, `mark_keeps_reported` only when held members are
known with no `kill_error`, and `folded_finished` children only when
`ack.summary` is true), while nested calls mark only tasks started by the
current script (without writing `keeps_reported`). Unrendered blocks, trimmed
children, and dropped fragments stay unreported. The
final result owns its payload, and daemon callers reserve only their IPC
encoding while checking the eventual MCP envelope ceiling.

Text windows reserve their working peak before entering a blocking reader. Retained
line prefixes release unused capacity after truncation and terminal padding removal;
legacy encoding boundary checks stream through fixed buffers. The text workspace
travels with blocking read and attachment workers, including after cancellation.
The completed result retains a smaller owner for its text payload.

Output matching reserves its whole application workspace in `wait::mark`, before
a poll resizes the terminal or sends input. With scan size `C = 64 KiB`, longest
needle length `L`, retained UTF-8 tail `T = L + 3`, and cloned needle bytes plus
`Vec<String>` slots `N`, the reservation is
`N + C + G(C) + G(T + D)`, where `G(n) = 4 * (n + 8)` and `D = 3 * (C + 8)`.
All size arithmetic is checked. The `G` terms cover old and replacement
capacities during Vec/String growth, including concurrent pending bytes and
decoded text; the separate `C` covers the log reader's buffer. The retained tail
may start up to three bytes before its requested UTF-8 boundary. The matcher
also splits larger feeds at `C` while keeping the same decoder and cleaner.

The decoder bound is checked against encoding_rs 0.8.42, upstream commit
[`a155adc`](https://github.com/hsivonen/encoding_rs/tree/a155adc7271c9e507556d053c95b459186671d2a/src).
Its `Decoder` uses fixed state and starts in `Converting` with BOM handling off.
The `max_utf8_buffer_length` formulas in `utf_8.rs` need at most
`3 * (C + 3) + 3`; `utf_16.rs` needs at most `1 + 3 * ((C + 4) / 2)`.
`gb18030.rs` has at most seven counted pending bytes and uses `1 + 3 * (C + 7)`.
`iso_2022_jp.rs` adds at most four state units before its factor of three;
`big5.rs` uses `3 + 3 * (C + 1)`. EUC-JP, EUC-KR and Shift-JIS add at most one
lead byte, single-byte and x-user-defined decoders use `3 * C`, and the
replacement decoder needs three bytes. Each bound fits `D`, including at EOF.
The matcher owns the lease through queued and running blocking scans, returned
scan tuples and error cleanup. Cancelling a waiter does not release a worker's
live buffers. On a match, the existing needle String moves into `WaitEnd` with
a split lease for its actual capacity; the other workspace is released.
The wait's JSON fields, notes, match precedence and reader commit remain unchanged.

File operations first reserve a shared path workspace before resolving, displaying,
opening, or sniffing a path. The allowance is 32 times the borrowed request, cwd,
and selected home source bytes, plus 4 KiB for fixed notices; reads also reserve
the sniff buffer. Native and display conversions, path copies, and error/result
formatting share this owner. Media configuration diagnostics reserve their source
text before formatting. Missing paths, binary files, media results, write/edit
results, and transfer inspection retain the owner through their returned result.
Media and path failures do not require the text decoder allowance.

The native path bounds match the Rust `1.92` and `1.100` standard-library
implementations. `rust-version = "1.92"` sets the minimum supported Rust
version; normal CI runs on stable Rust alongside a pinned Rust `1.92` job on
Alpine.

Native target resolution has a separate budget because a short symlink can name
a much longer target. Unix `canonical_target` supplies `realpath` with a
`PATH_MAX` caller buffer, including the terminating NUL; it finds the NUL inside
that buffer and moves the same byte Vec into an OsString without a UTF-8 copy.
Rust's standard library passes a null output buffer to `realpath`, whereas the
caller-buffer route returns the native
`ENAMETOOLONG` error when the resolved output cannot fit that platform limit.
On Linux the caller buffer is 4096 bytes; macOS and FreeBSD use 1024. Native path
bytes, including non-UTF-8 Unix names, are preserved; native filesystem naming
restrictions still apply. The raw-name filesystem round trip runs on Linux and
FreeBSD. The macOS CI filesystem rejects its `0xFF` filename with `EILSEQ`, so
all Unix hosts also test the native argument bytes and their lease directly,
without creating that name. The Unicode canonical-path budget case runs on all
platforms.

Windows canonicalization reopens the current name with the same access mode and
backup-semantics flag as Rust's standard library. It queries
`GetFinalPathNameByHandleW(VOLUME_NAME_DOS)` for its actual UTF-16 size. The query
includes NUL and a successful read excludes it. Each caller buffer is admitted
before allocation; if a later query needs more space, the old buffer is dropped
before its replacement. Conversion reserves three WTF-8 bytes per UTF-16 unit
plus String growth while the wide buffer is still alive, preserving unpaired
surrogates. No approximate Windows maximum path length is used as a memory bound.

Unix final-link reads admit each caller-owned `readlink` buffer before use and
grow from 256 bytes only when the result fills it. Windows preserves the
relative-link and junction semantics of Rust's `std::fs::read_link`. Its native
reparse buffer is 16 KiB. Eight times that size covers the fixed reparse buffer,
the substitute-name UTF-16 Vec, WTF-8 conversion growth and the short (at most
260 UTF-16 units) prefix check, matching `library/std/src/sys/fs/windows.rs`,
`sys/args/windows.rs`, and `sys/path/windows.rs` in the Rust standard library.
Native Win32 internal allocations remain outside this allowance. The forty-link
walk and directory checks still apply.

Canonical and followed targets carry their actual PathBuf-capacity lease through
the source object or blocking upload-preparation result. Link concatenation also
admits the old target, new result and temporary cleaned paths before copying.
The common native-argument allowance is `16 * (source_bytes + 8) + 1024` for
terminated strings, UTF-16 argument/normalized copies and their growth. After a
target is known, write/edit and transfer inspection add
`64 * (target_bytes + 64) + 4096` to the existing result owner. This covers up to
four staging/default/backup paths (including FreeBSD's temporary directory),
native arguments, lossy display expansion and error copies while these overlap.
The 64-byte suffix allowance covers the generated staging name and child name;
the larger multiplier includes geometric old/new capacities. All arithmetic is
checked. A failed reservation happens before publication; target-derived error
text keeps the outer owner. Save borrows the canonical target directly, and edit
verification checks both the current path identity and the opened handle identity.

**WONT FIX: opaque native-library scratch.** glibc `realpath` allocates its own
dynamic rname, extra, and link scratch buffers even with a caller buffer; a
supplied `PATH_MAX` buffer does not bound those internal allocations. libc and
WinAPI internals remain outside the application byte pool, which bounds
caller-owned buffers, owned targets, and application-derived paths without
claiming a whole-process RSS cap.

Native implementation references: [Rust 1.92 Unix filesystem code](https://github.com/rust-lang/rust/blob/1.92.0/library/std/src/sys/fs/unix.rs), [Rust 1.92 Windows filesystem code](https://github.com/rust-lang/rust/blob/1.92.0/library/std/src/sys/fs/windows.rs), [Rust 1.100 Unix filesystem code](https://github.com/rust-lang/rust/blob/1.100.0/library/std/src/sys/fs/unix.rs), [Rust 1.100 Windows filesystem code](https://github.com/rust-lang/rust/blob/1.100.0/library/std/src/sys/fs/windows.rs), [Rust 1.100 path conversion](https://github.com/rust-lang/rust/blob/1.100.0/library/std/src/sys/args/windows.rs), [Rust 1.100 path normalization](https://github.com/rust-lang/rust/blob/1.100.0/library/std/src/sys/path/windows.rs), [glibc 2.39 canonicalize](https://github.com/bminor/glibc/blob/glibc-2.39/stdlib/canonicalize.c), and [Microsoft GetFinalPathNameByHandleW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew).

File edits reserve the old file independently of the request arguments. The
blocking reader uses an 8 KiB block and admits each replacement `Vec` before
allocating it, while the old capacity is still charged. If geometric growth
cannot fit, it tries the exact required capacity. `EditSource` keeps the final
String's capacity lease in its last field, after the content. The fixed reader
allowance is returned when the read helper ends. There is no separate file-size
ceiling: the shared active budget decides whether the actual operation fits.

The matching workspace uses checked counts and arithmetic throughout. A growing
buffer with at most `n` elements of size `s` reserves `4 * (n + 8) * s`: two
capacities of up to twice the logical length may coexist during growth, and the
small floor covers initial capacities. This is a conservative payload and index
buffer bound, not a measurement of allocator bookkeeping. Before matching, the
reservation covers the LF strings, CRLF and original-line indexes, normalized
old/new strings, restored insertion text, per-edit records, current search needle,
KMP tables and bounded occurrence lists. The cached fuzzy text has its own space.

Unicode bounds come from the locked `unicode-normalization 0.1.26` crate,
not a fixed expansion assumption. `char::decompose_compatible` in the
`unicode-normalization` crate's `src/normalize.rs` emits fully decomposed
characters from static tables or the Hangul calculation without building a
string. The preflight counts those characters, line breaks, the longest
combining sequence, and the largest expansion of one input character.
Composition cannot increase the decomposed character count, so four UTF-8 bytes
per character bounds a non-ASCII NFKC result; ASCII retains its original byte
length. The longest sequence plus one expansion bounds the decomposition and
recomposition buffers in `unicode-normalization`'s `src/decompose.rs` and
`src/recompose.rs`. Their element sizes, geometric growth, and stable-sort
scratch are reserved before running the normalizer. The fuzzy-form allowance
also covers its NFKC String, line-slice vector, joined String, and
character-mapped String.

The heavier source map is admitted only before its first use. It includes the
source-unit vector, the four-offset unit records, untrimmed and trimmed forms,
and the per-byte origin index. Checks of a match crossing a composition boundary
separately preflight the actual borrowed context before `exact_survives_fuzzy`:
its normalized tagged vector, both `tagged_nfkd` vectors, stable-sort scratch and
accumulated text are all included. The largest temporary allowance remains
reserved through matching, so clearing a temporary does not leave a later call
using an uncharged retained capacity.

Overlap entries are counted before their vectors are built. Replacement text is
admitted against the original length plus every positive replacement increment,
plus three bytes for a restored UTF-8 BOM. Summing only the final length would
miss a reverse-order replacement that grows before another one shrinks it.
The same capacity rule covers the old and new String during each replacement.

Diagnostic construction is admitted before formatting or publication. Each error
shows at most 20 line numbers or edit labels; 16 KiB per edit covers the bullets,
joined error, JSON copy and temporary labels. A success reserves 512 bytes per
edit and the fixed 8 KiB source-verification block used during save. Both paths
add a path allowance and 4 KiB for fixed text. The successful response is built
before `save` can stage or rename the replacement. Any failed admission therefore
leaves the original file unchanged.

Saving also reserves caller-owned native metadata buffers. On Linux and macOS,
each extended-attribute size query precedes admission and allocation. The original
and inherited name lists keep their capacity leases while the current value is
copied and written; a value lease ends only after that write. FreeBSD also admits
raw length-prefixed name lists, the converted CString arrays, each value and the
final name-list recheck. Conversion covers the current Vec and boxed CString
while both can exist, and each returned group keeps its own owner through use.
The original query/read/query size check still rejects changed or truncated data.
Windows queries `GetKernelObjectSecurity` on the same file handle, then admits a caller-owned
buffer rounded to `usize` alignment. Owner, group and DACL pointers borrow that
buffer. Both source and staging descriptors stay charged through
`SetSecurityInfo`; no filesystem-specific descriptor size limit is assumed.
Every buffer is destroyed before its lease. Refusal discards the staging file
before publication and leaves the original unchanged. Internal allocations made
by native APIs remain an opaque upstream boundary (WONT FIX); this budget covers
our buffers and does not claim to bound total process RSS.

Both the initial edit reader and the prepublication content verifier retry
`Interrupted` reads, matching `Read::read_to_end`. Other I/O errors still return
immediately.

The workspace stays with the real blocking worker after its waiter is dropped.
Once source and matching buffers have been destroyed, the returned outcome
retains the larger of its path allowance and final text/JSON string payload.
The display String uses its actual capacity; freshly constructed JSON copies are
bounded by their encoded length. This owner covers successful replies and errors
until the application passes the result onward.

File text readers then extend the fixed read workspace to 512 KiB and reserve
16 times the largest retained text span requested so far.
The slice decoder reads explicit 8 KiB blocks; the fixed allowance covers its
decoded text and replacement-position vectors, including old allocations during
growth. Whole-file reads use the same allowance for their fixed read chunk. Each
retained line, accumulated window, or whole-file buffer grows only after
non-blocking admission. The workspace follows the blocking worker into its
Outcome and tracked result, with payload fields dropped before the lease.
File workers also return derived metadata and errors with their input owner,
including after their waiter is cancelled, and move it into the final result.
Transfer inspection likewise captures the input and owner together before the
future is polled. Its blocking job and completed path or error retain that owner
until the published result takes it; storing a path in the transfer table does
not extend this accounting to persistent table state.

The transfer table moves each published PathBuf into one Arc allocation. Admitted
requests and their GET, HEAD and PUT blocking workers clone that Arc, so those
application paths share the published PathBuf allocation.
Metadata checks pass the Arc to an application blocking worker to avoid the
PathBuf copy inside Tokio's generic metadata wrapper. A worker can retain that
same allocation after its waiter is cancelled and the idle table entry is swept.
The Admitted guard still controls active-request pins and idle timestamps; sharing
the path does not extend that guard or the per-file upload lock.

HTTP metadata checks and GET/PUT file opens also reserve
`native_path_bytes` from the shared process pool inside their actual blocking
worker, before calling the filesystem. The same allowance covers Rust's terminated
Unix path argument or Windows UTF-16/normalization buffers. The worker keeps that
lease through the call, including after its waiting request is cancelled; normal
returns, I/O errors and unwinding release it after the native argument buffers.
Admission failure uses the existing HTTP failure mapping. Parent creation already
reserves its own native target workspace. Native-library internals retain the
separate opaque boundary described above.

File downloads admit each application-created chunk before copying its bytes.
The returned Bytes owner retains the chunk's actual capacity lease through the
response frame and every cloned slice. Dropping the body does not release a
retained frame's charge. A failed admission ends the body with an I/O error;
the existing range, idle-entry and file-handle behavior remains unchanged.

The file-path branch of `attach` uses the same path workspace as file tools.
Admission includes the borrowed path, cwd and applicable home-directory sources
before resolution, display conversion or file I/O. Its owner stays in the actual
VM callback through media reading and error construction; the media byte lease
remains separate.

Nested callbacks borrow QuickJS-owned UTF-8 strings. Before parsing tool arguments,
the Rust bridge reserves four times their UTF-8 length, including the tool name,
for decoded values, escape scratch, typed requests, diagnostics and worker copies.
Short control arguments may use the control pool while being classified; ordinary
calls, including sleep, must move to the main pool before dispatch. Only nested
list/kill replies may also use that bounded reserve for their encoded wire String;
the queue keeps the wire owner until the VM consumes or drops the reply. Workers and pending-call metadata
keep the input owner through cancellation and final report construction. This
does not change the separate QuickJS heap limit.

Nested tool results admit memory in a single peak (`admit_bridge_result` in
`src/codemode/encode.rs`) without constructing `CallToolResult`. Before
committing a reader cursor, attachment watermark, or reported state, the
encoder measures `wire_len = ipc::json_len(&dto)` without allocating, checks
`wire_len <= BRIDGE_LIMIT`, resizes the held workspace or `TextSlice.lease` in
place to cover retained data plus `wire_len` (falling back to
`control_memory()` for `kill` and `list`), and reserves `wire_len` once against
the script's pending-result quota. It then serializes directly into a pre-sized
buffer, commits state and disarms `NestedAttachments`, drops the source data,
and shrinks the lease to the serialized buffer's capacity. Any earlier failure
unregisters temporary opaque attachment handles, drops uncommitted state in
safe lock/lease order, and preserves the prior quota charge. Wake queue items
keep the wire owner through synchronous VM consumption or queue disposal.

A refusal before commit leaves reader, attachment and reported state available for
a later poll. Already-executed start/input operations are not rolled back or replayed.
When the detailed error fits, it includes the task handle, log path and recovery
instructions. If the remaining script quota cannot hold even that error, a fixed
`ToolError` (`code: "LIMIT"` or `"OVERLOAD"`) explains that the operation was
not undone and must not be repeated. Accepted starts are recorded before waiting;
their handles remain in the outer codemode report even in this small-quota case.
This does not reserve an extra unbudgeted error frame.
Result payload sizing includes resource URIs, MIME strings, annotations and
metadata strings as well as text and Base64, and subtracts an existing payload
lease when sizing additional delivery storage.

Before committing a reader cursor or attachment watermark, the service includes
the encoded outer JSON-RPC ID length in the frame ceiling. A daemon also checks
its actual IPC envelope with the numeric call ID. Packing must fit both envelopes;
the daemon reserves only its own IPC buffer, and the endpoint reserves its own
later RPC encoding.

Terminal snapshots reserve their payload workspace while holding the terminal
lock, before copying rows or pending transcript history. Screen waits keep that
owner with their comparison rows. Transcript reads carry both the snapshot owner
and text-window workspace through each blocking worker; cancellation cannot free
them while the worker still uses the snapshot. The final text owner is acquired
before committing the reader cursor.
Media packing moves Base64 blocks between candidates without cloning them.
Payload and wire owners are split only after their corresponding buffers are gone.

Fastexec uses the official registry `rmcp = 3.9.0` with the checked-in lockfile.
There is no rmcp patch or local fork. The existing Windows ConPTY package remains
under `vendor/conpty`.

Application results carry a separate `TrackedResult.payload_owner`; SDK model
structs have no added fields. File and transfer workers, terminal results, task
lists, nested bridge envelopes and IPC encoders keep that owner with their actual
payload. Payload fields precede their owner, including in unstarted worker closures
and completed workers awaiting a consumer. Returning a bare `CallToolResult` to
rmcp ends this application ownership boundary.

Stdio records a conservative staging and wire reservation for a delivered request
ID, carrying any existing application payload owner alongside it. Payload bytes
already charged to that owner are not admitted a second time after reader state
is committed. The transport takes these owners before returning its send future, so the
application-owned send payload and encoded frame stay charged even if the future
is never polled. Serialization consumes the model before releasing its staging
reservation; the encoded frame keeps its own charge until the write ends. The
request-ID table is a prepayment mechanism, not an observer of SDK clone/drop
lifetimes. A reservation for a response the SDK never sends can remain until that
session closes. No extra guard is leaked or retained indefinitely to infer SDK
ownership.

Daemon replies retain both their application model and frame charges during
encoding; returned frames keep the wire charge while queued or partially written.
Short control replies account for both buffers in the independent control reserve.
The endpoint reserves five times the frame length plus a small fixed allowance
before reading: raw bytes, decoded strings, escape scratch and the extra owned
resource-content copy. After parsing, raw and scratch storage is dropped before
the decoded payload owner is handed to its application consumer.

The custom stdio input accumulator (`src/ingress.rs`, `src/cli/stdio.rs`)
reserves old and replacement capacity together. Raw bytes, decoded JSON strings,
and escape scratch are admitted before parsing; four times the input length
remains for model strings, metadata, and application parameter conversion.
Official public Request/Notification Extensions carry the application owner to
handlers and their workers. Recent cancellation IDs retain only their
transferred ID allowance. Short inputs up to `64` KiB may use the control
reserve for classification. Ordinary calls and `initialize` metadata move to
the main pool before dispatch, while short `list`/`kill`, `ping`, discovery,
and cancellation or lifecycle control messages can remain in the reserve.
Oversized lines (including partial chunks that cross the limit before `\n`),
malformed JSON, or schema-invalid requests return correlated JSON-RPC errors
(preserving the request `id` whenever parseable and discarding the remainder of
an oversized line through its newline) without closing an established stdio
session or killing active local tasks; notifications arriving before
`initialize` are ignored without triggering `exit(1)`.

At each public tool-handler entry, an existing application input owner is reused.
When the official SDK supplied no owner, the handler counts the borrowed typed
parameters and request ID, then reserves four times that encoded length before
creating application copies or dispatching workers. Ordinary calls use the main
pool; only short exec list/kill calls may fall back to control space. A refusal
returns a fixed short receipt before dispatch. This allowance covers subsequent
application copies and follows their real workers; it does not retroactively
account for the SDK's earlier parsing or parameter conversion.

Initialization, discovery, tool listings and the instructions tool reserve their
application response-copy workspace before copying startup configuration, then
release it at handler return. Tool listings count owned descriptions and metadata
while sharing schema Arcs. Progress forwarding owns its input token and admits
its application message String before copying. Stdio admits its separate encoded
frame when the official SDK calls the transport.

**WONT FIX: official SDK internal ownership.** Public `rmcp` APIs do not expose
ownership hooks for the following paths:

- HTTP body collection, JSON parsing, header decoding, and validation scratch.
  `fastexec` enforces authentication, route, and body-limit checks ahead of the
  SDK while keeping `rmcp`'s HTTP validation order and error behavior.
- Independently cloned request parameters, initialization peer information,
  response models, handler cancellation, and SDK queues after application handoff.
- HTTP JSON/SSE encoding, response/replay caches, keepalive frames, and returned
  body slices, including errors and progress notifications.

Whole-HTTP RSS accounting, post-handoff SSE frame tracking, and SDK internal
clone or replay lifetimes sit outside the application byte pools because
upstream `rmcp` owns those allocations. `fastexec` does not fork or duplicate
SDK internals; the process pool bounds the file, media, IPC, terminal, event,
and nested Codemode payload buffers owned by `fastexec`.

The HTTP integration case
`slow_sse_client_holds_active_memory_lease_until_response_completes_or_drops`
is marked `#[ignore]` as an upstream-dependent check because its assertion
requires `rmcp`'s slow response body to hold a lease after handler return. CI
reports this case as ignored while running ordinary HTTP media delivery,
application admission, cancellation, and control checks.

IPC request senders count JSON without a temporary encoding, reserve the measured
frame capacity, and encode directly into that allocation. The frame owns its
reservation while waiting for the writer and during partial writes. A `Hello` also
reserves its temporary environment and identity copies before constructing them.
Daemon request readers reserve six times the declared body length: the raw body,
a parsed `Value`, and four times the body length for borrowed-`Value` decoding and
subsequent argument copies. The earlier escape-decoding scratch fits within that
peak. After parsing, the raw body and `Value` are dropped before retaining the
four-times allowance. This covers payload strings and native OS-string units,
not JSON object metadata, persistent launch environments, task state, or VM heap.
Short `Hello`, `HelloEmit`, `Cancel`, `EventsCancel`, `Bye`, `Status`, `Stop`,
and `list`/`kill` requests may use the control reserve; ordinary tool calls,
`EmitEvent`, and `EventsList`/`EventsPoll`/`EventsStream` move to the main pool
before dispatch. A refused call returns a short correlated receipt without
running the operation. Handshake and CLI response readers use the same budgeted
endpoint parser.

The IPC input owner travels with the dispatched future and the call's worker
arguments. File workers retain it after their waiter is dropped; stdin queue items
keep it through actual writes, and script code keeps it through the VM invocation.
Payload fields precede their owner, including in unstarted worker closures. Hello
conversion releases its temporary owner after the handshake; the installed launch
context retains its existing state lifetime. Reader and encoder boundaries are
covered by small ordinary-request tests, including cancellation, native-unit
conversion, and control operations with an unavailable main pool.

Opt-in image compression reserves each codec stage before construction: retained
input and metadata, decoded pixels, orientation copies, resize intermediates and
one bounded encoder candidate. PNG candidates and JPEG quality attempts run
sequentially. Header accounting includes PNG Exif copies and JPEG decoder-owned
input; WebP uses the declared RIFF extent for both inspection and decoding. The
locked WebP decoder needs a conservative workspace bound, so some otherwise valid
lossless or alpha images can be refused when compression is enabled. Byte
passthrough remains available with the default `FASTEXEC_IMAGE_COMPRESS=false`.
These bounds must be reviewed when the locked image codec versions change.

### `FASTEXEC_YOLO`

`"false"` (default) keeps each tool's default MCP `annotations` hints.
`"true"` sets `readOnlyHint: true`, `destructiveHint: false`,
`idempotentHint: false`, and `openWorldHint: false` on every exposed tool and
keeps each tool's `title`. The MCP server reads this setting at startup in all
modes and exits with status `1` if the value is invalid.

### `FASTEXEC_INSTRUCTIONS_FILE`

When unset, MCP `initialize` and `server/discover` return the built-in server
instructions unchanged. When set, the path is resolved with `files::resolve`
(relative to the launch working directory, with `~` expansion and Windows Git
Bash drive paths) and opened non-blockingly as a regular file with
`files::open_regular`, rejecting directories, FIFOs, sockets, device nodes, and
Windows named pipes or reserved DOS device names.

The server reads at most `16,385` raw bytes without pre-checking file metadata
and validates in order: size cap (`16,384` raw bytes), absence of NUL bytes,
and valid UTF-8. It normalizes CRLF and lone CR to LF and strips leading UTF-8
BOM characters. Whitespace-only or BOM-only files supply no extra text.
Nonempty normalized text retains leading indentation and trailing whitespace.
The default protocol composition appends it after a blank line (`\n\n`);
`FASTEXEC_INSTRUCTIONS_TOOL=move` instead puts it in the tool, as described below.
Empty, non-UTF-8, unreadable, or invalid values exit with status `1`.

Each serving process captures one immutable `Arc<str>` snapshot at startup;
later edits or deletions do not change `initialize` or `server/discover`
responses. Local stdio (`FASTEXEC_DAEMON=off`), daemon stdio endpoints
(`FASTEXEC_DAEMON=on`), and HTTP daemons (`--http`) read it at startup.
`daemon start --http` probes for an already-running daemon first and skips
reading the file when reusing one, pre-validating it only on a cold HTTP start.
Non-HTTP `daemon start` and `daemon run`, plus `daemon status` and
`daemon stop`, leave the variable unread.

### `FASTEXEC_INSTRUCTIONS_TOOL`

This optional serving-process setting takes exact lowercase `off` (default),
`move`, or `copy`:

| Mode | Protocol `instructions` | Tool description and result |
| --- | --- | --- |
| `off` | Built-in lines plus extra text, if any | Tool absent |
| `move` | Built-in lines plus a pointer when there is extra text | Extra text only; built-in lines if no extra text |
| `copy` | Built-in lines plus extra text, if any | Same composed string as the protocol field |

The zero-argument tool returns one text block verbatim, independent of output
formatting and without task or file operations. Its discovery description is
already populated before the call; client display or truncation is up to the
client. On HTTP both snapshots and the router are shared across sessions; a
stdio endpoint has its own snapshot. The flag is validated only at MCP serving
startup and during cold HTTP preflight; healthy HTTP reuse and non-serving
daemon commands ignore the caller's flag. Changes take effect when the serving
process restarts. Do not include secrets: clients allowed to discover tools
can receive the description as part of discovery.

A nonzero command `exitCode` is a normal result (`isError: false` on direct
calls; resolves normally in `codemode`). Operational failures set
`isError: true` on direct MCP calls (with no `structuredContent`) and reject
with `ToolError` in `codemode`.

### Programmatic result fields (`codemode`)

These fields describe typed objects returned by nested `tools.*` calls in
`codemode` (direct MCP calls return text and media blocks without
`structuredContent`). Default-valued flags, zero counters, empty arrays, and
null diagnostics are omitted via `skip_serializing_if` unless noted as
always-present.

- `start`, `poll`, and `kill` always include `action`, `kind` (`"bash"` or
  `"codemode"`), `taskId` (reader handle), `state` (`"running"`, `"exited"`,
  `"killed"`), `exitCode`, `pty`, `elapsedMs`, `output` (raw string without
  inline markers or footers), `truncated`, and `logPath`. On direct MCP calls,
  status lines omit ` · log <path>` when a finished task has neither omitted/cut
  output nor log-loss warnings, open output, or kept processes. Fields omitted
  when false, zero, empty, or null: `waitEndedBy` (`start` and `poll` only;
  `kill` omits it), `keepTree`, `keptRunning`, `treeEnded`, `treeUnconfirmed`,
  `forcedEnd`, `lifetimeExpired`, `logEvicted`, `logError`, `captureError`,
  `outputIncomplete`, `outputOpen`, `droppedBytes`, `inputWarning`,
  `inputError`, `warnings`, `leftovers` (`[{ pid, name }]`), `held` (`[{ pid,
  name }]`), `killError`, `encodingErrors`, `omittedLines`, `omittedRange`
  (`[first, last]`), `cutLines`, and `newReader`. With `screen: true`, `cols`,
  `rows`, and `cursor` (`[row, col]`) are present and `omittedRows` is included
  when non-zero.
- `transcript` always includes `action`, `kind`, `taskId`, `state`,
  `exitCode`, `pty`, `elapsedMs`, `output`, `truncated`, `logPath`,
  `transcriptPath`, `transcriptLines`, `finalLines`, and `firstLine`, plus
  non-default window and task diagnostics when set.
- `codemode` report is included when any array is non-empty (nested
  `attachments` sealed handles, `cancelledCalls`, `startedTasks`).
- `read` text reads return `{ kind: "text", path, text, startLine,
  endLine, shownLines, totalLines, truncated, nextOffset?, lineTooLong?,
  lossy? }`. Media reads return `{ kind, path, mimeType, bytes, sourceBytes?,
  attachable, note? }` without base64.
- `list` returns `{ action: "list", totalCount, tasks }` (built after
  `limit`, rejecting `maxBytes` and `truncate`). Each entry carries `taskId` as
  a bare task ID, `kind`, `state`, `exitCode`, `pty`, `elapsedMs`, `command` up
  to 120 chars, `owner`, and `logPath`, omitting `cols`, `rows`, `session`,
  `callId`, `parentTaskId`, `treeEnded`, `treeUnconfirmed`, and task
  diagnostics when null/false/zero/empty.
  Flat child entries (in `parentTaskId` drill-down or as promoted orphans) set
  `parentTaskId` to the bare parent ID. `codemode` entries include `children`
  (`{ total, done, failed, warned, treeEnded, active, queued, cancelled,
  activeTasks }`, omitting zero counters and empty `activeTasks`) only when
  `total > 0`, `running && queued > 0`, or `!running && cancelled > 0`. Up to `8` active
  children appear in `children.activeTasks` (without redundant `parentTaskId`).
  Each task entry carries its own `warnings` array; nested `list` omits the
  redundant top-level formatted warning string list.

#### Integrity and initial-input diagnostics

On pipe `start`, an early child close (`BrokenPipe` or Windows pipe-closed codes
`109`/`232`/`233`) sets `inputWarning: "stdin closed after N of M bytes"` as a
non-fatal warning, whereas other initial write failures, any `poll` write
failure, or output capture and log write failures (`task_io_ok == false`) return
`isError: true` on direct MCP calls and reject with `ToolError` (`code: "IO"`)
in `codemode` while preserving `exitCode`, captured `output`, error fields, and
`warnings` on `err.result`. Warnings stay whole regardless of `maxBytes`.

#### Background footer (direct MCP calls only)

Appended to direct `start`, `poll`, `kill`, `transcript`, and `codemode` results
(up to `512` bytes, outside `maxBytes`), naming up to three other tasks this
session has addressed (using bare task IDs) plus any unseen `external.event`
summary for this session (`External events: <n> new (...)`; section 7). Nested
`codemode` calls never emit background footers, never advance the session's
external-event background cursor, and never suppress outer session task
reporting when polling, killing, reading, or listing outer tasks. HTTP clients
sharing a token share this session scope. While scanning `session.touched`,
`Tasks::background` collects running `codemode` task IDs before excluding the
current call's task (`exclude`), and skips running children (`parent_task_id:
Some(pid)`) whose parent `pid` is also running in `touched` so an active batch
does not flood the three-slot footer (including when polling the parent itself).
Once the parent exits or is killed, surviving running or unreported finished
children surface normally until reported by `poll`, `kill`, or a delivered
`list` (including via a delivered parent summary). Running tasks (or tasks
holding processes) outside this session appear at most once per session as `1
task running outside this session; use list with scope "all".` or `<n> tasks
running outside this session; use list with scope "all".`; the session flag is
consumed atomically when a delivered response shows that clause or when the
session's first committed footer-bearing result finds no outside tasks (never on
nested or cancelled calls). A footer marks only session tasks it names, and
commits the session's `external.event` cursor only after the response is
prepared for delivery; direct `list` marks only delivered fragments of shown
session entries (plus folded finished children when the parent summary fits),
while nested `list` marks only tasks started by the current script (without
writing `keeps_reported`).

## 3. Logs, terminal emulator, and transcript spool

### Logs and retention

File names use the bare task ID, without the reader
  suffix; readers share the same retained log and transcript files, and status
  lines omit ` · log <path>` when a finished task has neither omitted/cut output
  nor active processes or log warnings. In the default mode each task logs raw
  output to
  `<temp>/fastexec-<uid>-<pid>-<random>/<taskId>.log` on Unix (`0700` dir, `0600` file)
  or `<temp>\fastexec-<pid>-<random>\<taskId>.log` on Windows (owner-only DACL).
  The directory is reserved atomically and existing shared directories or links
  are refused. On macOS, creation also suppresses inherited ACL entries; existing
  runtime directories with ACL entries, or on volumes that ignore ownership, are
  refused. FreeBSD makes each new directory private and empty before creating
  children. Shared runtime names are published only after those checks, from a
  random sibling with FreeBSD 15.1 `renameat2(RENAME_NOREPLACE)`, so concurrent
  starters cannot use a partially initialized directory. Existing destinations
  are never replaced; unsupported filesystems are refused without falling back
  to an overwrite-capable rename. UFS and ZFS are tested. Existing directories with
  non-trivial access ACLs or inheritable POSIX default ACLs are refused without
  changing those ACLs. Its executable cache
  and short-path sockets each sit inside an additional private directory created
  after the outer directory's ACLs are removed, so an earlier `O_SEARCH` handle
  cannot reach them. Windows validates
  an existing directory's owner, DACL, and reparse
  status. `on` mode logs under
  `<runtime root>/ns/<namespace>/logs/` (section 6). Each task log caps at `64`
  MiB, and all logs, transcripts, and stored attachments on a server cap at `4`
  GiB total (evicting oldest finished task files first outside
  `lock(&self.registry)` so filesystem unlink I/O never blocks task-registry
  operations, and refunding quota only when file deletion succeeds or reports
  `NotFound`). After a partial write,
  the new reservation is refunded only if truncation to the confirmed prefix
  succeeds. Otherwise the full reservation stays charged until file reclamation;
  the uncertain tail never advances readable bytes or transcript lines. Past a
  cap or write error, capture keeps draining output and the status line reports
  dropped bytes. The default mode allows at most
  `16` running tasks (including `keepTree` tasks still holding processes) and
  retains the `64` most recently finished tasks without expiry. `on` mode allows
  `64` running and `256` finished tasks, drops a finished task after `24` hours,
  and keeps task logs and transcripts under
  `<runtime root>/ns/<namespace>/logs/` (section 6). The default mode removes its
  log directory on server shutdown. On Linux it cleans up stale private log
  directories for dead PIDs only when the effective owner, permissions, and
  resolved ancestor paths are safe. It retains unverifiable entries. On macOS
  and Windows, and on FreeBSD, stale default-mode directories are retained;
  automatic stale-directory cleanup currently runs only on Linux. `on` mode
  empties the namespace's
  `logs/` directory at daemon start while holding its `daemon.lock`.
### Live PTY emulator and continuous transcript spool

Each UTF-8 PTY task
  feeds output bytes into an `alacritty_terminal` (`0.26.0`) instance at the
  task's size (`src/term/terminal.rs`, `src/term/frames.rs`) that answers terminal queries
  (`ESC[6n`, `ESC[c`), tracks keyboard modes, serves `screen: true`, and
  continuously spools `<taskId>.transcript.txt` (up to `64` MiB, counted under
  the `4` GiB total limit). A resize settles the transcript at the old size,
  reflows at the new one, and starts the recorder fresh, so transcript text is
  never lost on either screen, and rows still visible after the resize may appear
  twice; arbitrary application redraws keep today's best effort. Lines scrolled off the normal screen and merged frames from
  alternate-screen sessions (`--- alternate screen ---` /
  `--- normal screen ---`) are written as final lines once settled. A
  `transcript` call returns unseen final lines followed by current pending
  lines; pending lines may change on the next call.
### Bounded terminal state

A rendered cell retains at most `32` user combining
  characters, and transcript row markers are replaced and retired once the
  grid no longer references them. OSC control strings are bounded at `8` KiB;
  an oversized string is discarded through its terminator without becoming
  visible text. Unused window titles, hyperlinks, and clipboard contents are
  not retained. The raw task log still receives the original bytes under its
  normal limits. Filtered bytes are mapped back to raw positions so a screen
  read cannot consume output still held by a synchronized update. Kitty keyboard
  modes have a separate `128`-entry saved stack for each screen; an overflow
  drops the oldest saved mode while current-mode queries and restores continue
  to work.

## 4. PTY keys and bundled ConPTY

### Key names and modes (`src/term/keys.rs`)

`keys` uses case-insensitive tmux
  `send-keys` names (`Enter`, `Tab`, `BTab`, `Escape`, `Space`, `BSpace`,
  arrows, `Home`, `End`, `PageUp`/`PageDown`, `Insert`, `Delete`, `F1` to `F12`,
  keypad keys, `[NUL]` to `[US]`, `0xHH`, or single characters) with `C-`, `M-`,
  `S-`, and `^` modifiers. Keys encode according to the program's active
  keyboard mode: standard xterm/tmux sequences (respecting DECCKM cursor and
  DECKPAM keypad modes) or the kitty keyboard protocol when enabled. Initial
  validation rejects unknown or unencodable keys before input delivery; a later
  keyboard-mode change can fail a key after earlier input has been sent.
### Key pacing

`keyDelayMs` (`0-2000`, default `150`; requested pauses per
  poll capped at `30000` ms) waits for stdin to accept prior bytes (up to `5` s).
  When input or another key precedes a key, the delay separates it from a paste.
  Zero sends input and keys together. Each write waits for the writer's completion
  acknowledgement; failed writes return an error. A write still pending after
  `5` s, or cancelled while pending, returns an unconfirmed-delivery error and
  can still take effect later; it must not be retried blindly. On Unix, canonical
  terminal input emits a warning that is never shortened: its line discipline
  can discard long lines, including a line accumulated over several polls.
  Use a pipe or file when exact long-text delivery is required. Windows PTY input
  always warns that its mode could not be verified: MSYS console readers may
  discard a leading byte after a resize, even for short keys ([#5](https://github.com/ImoutoHeaven/fastexec-mcp/issues/5)).
  Writer acknowledgement does not confirm delivery to the application.
### Bundled ConPTY (`src/platform/conpty.rs`)

Windows x64 and arm64 builds embed Microsoft
  ConPTY `1.24.260710001` (`conpty.dll` and `OpenConsole.exe`, MIT). Each process
  extracts into a newly reserved owner-only directory named
  `%TEMP%\fastexec-conpty-<version>-<arch>-<pid>-<random>` and pins the verified files
  read-only. The next process retires private copies whose owning process ended.
  The exact verified DLL handle supplies the console API, and dependencies are searched
  only in its private directory and System32. Loading happens before the first PTY so
  terminal output passes through without legacy Windows console re-rendering. If loading
  fails, the already loaded system kernel supplies ConPTY; CWD and PATH are never searched
  for a replacement DLL. Transcript results warn that lines may be missing on that fallback.

## 5. Bash launch and process-tree ownership

### Launch (`src/platform/bash.rs`, `src/platform/process.rs`, `src/session.rs`)

Runs
  `bash --noprofile --norc -c` (`loginShell: false`, default) on top of a
  once-per-session login-environment snapshot probed via `bash -l -c 'exec "$0" --fastexec-dump-env' <fastexec>`
  (`5` s deadline, `1` MiB cap, stripping `PWD`, `OLDPWD`, `SHLVL`, `_`,
  `SHELLOPTS`, `BASHOPTS`, and session-removed variables; `with_cwd` shares both
  the bash path and snapshot cache, and probe failure falls back to the raw
  session environment with a one-time warning), or `bash -l -c`
  (`loginShell: true`) on the raw session environment to re-source profiles.
  Commands over `8000` bytes run from a temporary script
  file in the log directory. Both modes set `LANG`/`LC_ALL` (`C.UTF-8`, or
  `en_US.UTF-8` on macOS), `PAGER=cat`, `GIT_PAGER=cat`, `PYTHONUNBUFFERED=1`,
  and `PYTHONIOENCODING=utf-8`; pipe mode also sets `TERM=dumb`, `NO_COLOR=1`,
  `CLICOLOR=0`, `FORCE_COLOR=0`, `EDITOR=true`, `VISUAL=true`,
  `GIT_EDITOR=true`, and `GIT_TERMINAL_PROMPT=0`, whereas PTY mode sets
  `TERM=xterm-256color`. Bash discovery's `--version` probe uses the same
  process-tree ownership as a task, with nonblocking output, a `4` KiB banner
  limit, a `5` second deadline, and cleanup of descendants on every outcome.
  Task workers and startup reapers are reserved before launching processes;
  allocation failures return errors without admitting an untracked running task.
### Tree ownership (`src/platform/process.rs`, `src/platform/host.rs`)

Unix entry points restore `SIGCHLD` to its default disposition with automatic
reaping disabled before spawning children. Each task host's PID stays reserved
until its owning server explicitly waits for it. Readiness follows the bash
spawn attempt; if the host dies before readiness, startup cleanup ends its
reserved process group before reaping the host. Unix PTY descriptors are
created with `O_CLOEXEC`, so an unrelated spawned command cannot retain them
across exec. Linux opens the matching slave with `TIOCGPTPEER`, so another
devpts mount cannot redirect a pathname lookup. FreeBSD and macOS use a
separate buffer per allocation, with `ptsname_r` and `TIOCPTYGNAME` respectively.

- Windows: runs each task inside a Job Object with
  `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` and breakaway disabled, because Git
  Bash starts `&` children with breakaway whenever a Job allows it.
- Linux: a separate task host leads the session and adopts daemonized
  descendants as a subreaper. Cleanup pins candidate processes with pidfds
  before reading their authoritative ancestry, verifies membership again,
  and signals the pinned processes. An empty scan alone cannot prove that
  cleanup is complete: the host exits only after `waitpid` reports `ECHILD`,
  and its pidfd confirms that exit before cleanup succeeds. This requires kernel
  `5.3+`, readable `/proc`, and allowed `pidfd_open`/`pidfd_send_signal`
  syscalls. The host checks pidfd availability before running bash; a failure
  to read procfs during cleanup is reported as an error. A lifeline pipe
  triggers cleanup if the server dies. A host
  retains ownership and retries when a privileged descendant cannot be killed.
- FreeBSD: the task host acquires a kernel reaper with `PROC_REAP_ACQUIRE`
  before starting bash. `PROC_REAP_GETPIDS` identifies descendants, including
  nested reapers; `PROC_REAP_KILL` lets the kernel select signal targets
  without a numeric PID-reuse race. Detached sessions and double forks stay
  owned, without procfs. Partial kills and native permission errors keep the
  host alive and report the remaining tree. The
  lifeline cleanup preserves reaper ownership while an unkillable descendant
  remains. PTY allocation uses `posix_openpt` and `ptsname_r` with a separate
  path buffer for each call; FreeBSD's `openpty` uses shared `ptsname` storage
  and can open another task's slave during concurrent starts. Both descriptors
  are created with `O_CLOEXEC`. The host retains a private PTY slave. After every
  descendant is reaped, it disables terminal input controls and echo, resumes any stopped
  output, and drains queued bytes before its session closes. Late input cannot
  stop or discard the finished command's output. The root's exit status is
  reported before this final drain,
  so a kept background writer does not delay that result. An empty live host
  completes the drain itself; an already dead host's reserved process group
  is still cleaned up.
- macOS: the same task host owns membership, because nothing adopts an
  orphan there: it scans the process table every 100 ms and tracks the
  processes it started by pid and start time, so a descendant seen once stays
  a member after it leaves the session, group, and parent chain. The host
  stays alive while it holds a member, which is what a `keepTree` wait, a
  deadline, and the server's end-of-tree wait read. The server keeps no
  membership set: it asks the host for members and for a kill over a control
  socket, so no signal ever names a group by a number the server read
  elsewhere. Identity comes from libproc, or from the kernel's `KERN_PROC_PID`
  row for another user's process, which libproc refuses. `SIGKILL` goes to
  each member by identity, then to the host's own group. A member this user
  may not signal fails the kill and keeps the host holding the tree, as on
  Linux. A descendant that leaves the session and group and loses its
  traceable parent before a scan sees it escapes, whichever order `setsid`
  and the parent's exit happen in: the tree is best-effort tracking of
  detached descendants, and fastexec is not a sandbox.
- `keepTree: true`: keeps leftover processes running after root bash exits
  until `kill`, `killAfterMs`, or the end of the tasks' owner: the server's
  session in the default mode, the daemon's stop or exit in `on` mode. Once the root exit has been
  reported, subsequent `poll` calls wait on the held tree
  (`waitEndedBy.condition: "tree_ended"`), and a `start`/`poll` with
  `returnWhen` waits past a root exit `0` for output from held processes.
- Non-terminating Windows root: a ConPTY root records its
  script's exit status as that script ends, in a file the task's directory
  holds. A root that has not terminated five seconds after its script finished
  is ended with that status, so the task publishes `exited` with `forcedEnd:
  true` and ` · root forced after 5 s exit grace` in the result line; its tree
  is then handled as on any root exit. The marker is installed only when the
  shell has no `EXIT` trap yet, so a startup file (`BASH_ENV`, a profile) that
  sets one keeps its cleanup; such a task, a command that sets its own trap,
  and a command that `exec`s all keep the plain wait.
- Cleanup limits: native permission failures appear in `killError`; unkillable
  processes remain held. Processes launched outside the owned tree, such as
  through a system service manager, are outside its cleanup scope.

## 6. Daemon mode (`FASTEXEC_DAEMON=on`)

### Endpoint and daemon

The stdio process answers `initialize`, `server/discover`, and `tools/list`
itself and forwards every tool call and MCP Events request to the namespace's
daemon, holding no tasks of its own and killing none. The first call works to
one `10` s deadline that covers connecting, starting a daemon, and its `Hello`;
only an absent daemon is started, and one that is present yet unresponsive in
time is reported as an error. A timeout in the writer queue leaves the
connection usable and identifies the frame that never went out. Interrupting a
frame write closes the connection. A call whose connection fails after sending
began returns an error naming the call, request ID, session, and daemon epoch:
the action for `exec`, and the tool for `codemode`, `read`, `write`, `edit`, and
`transfer`. Failed calls are not replayed. `list` shows each task's owner and,
for a call through this endpoint, the session and call ID that started it; both
are null for a task started in the default mode or over HTTP.

### Identity and runtime root

The build ID is the SHA-256 of the running image. The namespace is the first
`16` hex characters of `SHA-256(build ID, scope)`, where the scope is a
non-empty `FASTEXEC_NAMESPACE`, else the canonical path of the executable, so
different builds never share a daemon.

The runtime root is `%LOCALAPPDATA%\fastexec` on Windows (created with an
owner-only DACL) and `$XDG_RUNTIME_DIR/fastexec` or `<temp>/fastexec-<uid>` on
Unix (mode `0700` and verified). `ns/<namespace>/` holds `startup.lock`,
`daemon.lock`, `daemon.log`, `logs/`, and the socket; `bin/<build>[.exe]` holds
the cached copy of the executable a daemon starts from. A daemon start sweeps
the payload of every namespace whose locks it can acquire.

### IPC (`src/ipc.rs`)

Endpoints and `daemon emit` connect over a named pipe
`\\.\pipe\fastexec-<user hash>-<namespace>` with an owner-only DACL that
rejects remote clients, or a Unix socket at `ns/<namespace>/sock` when that path
fits in `100` bytes, else at `/tmp/fastexec-<uid>/<namespace>.sock`. Frames are
length-prefixed JSON speaking protocol version `6` (`ipc::PROTOCOL = 6`). Tool
calls name their tool (`exec`, `codemode`, `read`, `write`, `edit`, `transfer`),
while MCP Events and external ingestion use `EventsList`, `EventsPoll`,
`EventsStream`, `EventsCancel`, `HelloEmit`, `EmitEvent`, `EventFrame`, and
`EmitAck` (section 7). The endpoint refuses a tool call whose serialized
arguments exceed `3` MiB before writing it (sizing for up to `1` MiB of
JSON-escaped `start` `input`), a request frame is at most `4` MiB (a `Hello`
carries the endpoint's environment), and a response frame is at most `512` MiB.
One endpoint connection carries at most `32` concurrent tool calls and `16`
active event streams (`MAX_EVENT_STREAMS_PER_CONN`), and the daemon admits at
most `128` endpoint connections plus `128` concurrent `daemon emit` connections
(`MAX_EMIT_CONNECTIONS`).

### Sessions, lifetimes, and host confinement

Each endpoint process holds one random session ID with its own task entries and
readers, and a reconnect keeps them. An endpoint session with no live connection
is dropped after `10` minutes, and the table holds at most `256` endpoint
sessions. The endpoint sends `Bye` when its stdin closes, dropping that session
immediately. Sessionless HTTP has one principal session per configured token for
the listener's entire lifetime; these sessions do not expire. A session's
entries retire with their tasks.

The daemon does not exit idle by default. Starting it with `--idle-exit on`
enables exit after `10` minutes with no connections, an empty task registry
(finished tasks stay for up to `24` hours), and no live or in-flight `/t/`
transfers. `--idle-exit off` explicitly disables it. HTTP daemons never exit
idle, even with `--idle-exit on`. `fastexec daemon stop` kills every task and
exits. A daemon crash ends its tasks through the Windows Job or the Unix task
host, and a task ID from a dead daemon answers as coming from an earlier daemon.

A host that keeps its children in a Job without breakaway, such as a `fastexec`
task, cannot start a detached daemon, and the call fails naming
`fastexec daemon start` from a terminal outside the host. `fastexec daemon run`
is the supervisor's own process and serves inside such a host. A host that
confines its children to a cgroup, such as a systemd scope or a container, ends
the daemon with it.

### CLI (`src/main.rs`, `src/cli/daemon.rs`)

`fastexec` with no arguments serves MCP over stdio (exiting `1` after cleanup if
MCP initialization fails); `--version` (`-V`) and `--help` (`-h`) print version
or usage and exit `0`, and unknown or extra arguments print usage to stderr and
exit `2`.

- `fastexec daemon start [--http <addr>] [--idle-exit on|off]`: starts the
  daemon detached from the calling terminal and prints its epoch, PID,
  namespace, and HTTP address. Start flags apply only to a new daemon; stop an
  existing daemon before changing its startup settings or idle-exit policy.
- `fastexec daemon run [--http <addr>]`: serves the daemon in the foreground in
  the supervisor's process, acquires the namespace lock (refusing a namespace
  that already has a daemon), prints its readiness line to stderr once bound,
  keeps logs on stderr, runs with idle exit off, and rejects `--idle-exit`.
  `SIGTERM` and `SIGINT` on Unix, and `Ctrl+C`, `Ctrl+Break`, console close, and
  shutdown on Windows, stop all tasks as `daemon stop` does and exit `0`.
- `fastexec daemon status`: prints epoch, build, PID, sessions, tasks, and the
  HTTP address or `off` without starting a daemon.
- `fastexec daemon stop`: stops the running daemon and prints the stopped epoch
  (exiting `0` without starting a daemon if none is running).
- `fastexec daemon emit [--namespace <ns>] [--source <src>] [--topic <topic>] [--stream]`:
  injects SEP-3415 `external.event` occurrences into an already-running daemon
  from `stdin` (section 7).

### Streamable HTTP (`src/http.rs`)

`--http <addr>` with `FASTEXEC_HTTP_TOKEN` (one or more unique tokens separated
by ASCII whitespace, each at least `16` visible ASCII characters) serves the
same toolset and MCP Events over Streamable HTTP. Empty lists, invalid tokens,
and duplicates fail startup naming token positions without exposing token
values. Sessionless requests use the authenticated token's principal session
(`t1`, `t2`, etc., with owner labels `http-t1`, `http-t2`, etc.); the inline
`_meta` lifecycle selects this path regardless of the named protocol version.
Stateful HTTP `initialize` creates a session after negotiation. The bind address
must be loopback unless `FASTEXEC_HTTP_ALLOW_REMOTE=1`, which belongs behind a
TLS-terminating reverse proxy. Every `/mcp` request requires a configured bearer
token checked before session creation or resumption, and `FASTEXEC_HTTP_TOKEN`
is stripped from the environment HTTP sessions launch tasks with. `/mcp` bodies
over `4` MiB are refused. `Origin` is restricted to the bound address's origin,
loopback names, and `FASTEXEC_HTTP_ALLOWED_ORIGINS` (where a host entry without
a port matches any port); this is request `Origin` validation, without CORS
preflight or response headers. HTTP sessions run with the daemon's environment.
The listener retains up to `256` stateful HTTP sessions (expiring after `60`
minutes idle or on `DELETE`); principal sessions sit outside this count and live
until the listener stops.

The HTTP listener admits at most `128` concurrent connections and enforces a
`10` second header-read deadline without imposing a total timeout on uploads,
SSE streams, or long tool calls. Within those `128` connections, `/t/` transfer
requests are bounded by a dedicated sub-quota (returning `503 Service
Unavailable` when transfer slots are full) so concurrent file transfers cannot
starve `/mcp` control traffic. Extra connections past `128` close without
queueing; local daemon socket management keeps its own capacity. Streamed
responses include `X-Accel-Buffering: no`.

### Transfer route (`src/transfer.rs`)

`/t/<capability id>` serves one file without the bearer token so tools such as
`curl` can download or upload directly. It runs on `--http` (sharing the
daemon's table with any `FASTEXEC_TRANSFER=true` stdio endpoints; set
`FASTEXEC_PUBLIC_URL` behind a reverse proxy so minted URLs carry the public
origin and path prefix) or on the standalone `/t/` listener in local stdio and
non-HTTP daemon modes when `FASTEXEC_TRANSFER=true`, returning `404` on any
non-`/t/` path there. A capability ID expires after `10` minutes unused, and an
unknown or expired ID returns `404`. `GET` returns the file, honoring one
`bytes=` range with `206` and `416` and ignoring other forms so the full file
returns with `200`. `HEAD` returns the current byte size. `PUT` writes the body
at `?offset=` (default `0`), truncating the file to that offset before reading,
refusing an offset past EOF with `409`, and returning the new size on success.
Truncated or malformed bodies return `400`, and an upload that stalls `30`
seconds waiting for network bytes returns `408` while keeping the written prefix
so `HEAD` and a resumed `PUT` can continue from the confirmed size. Uploads to
the same open file identity serialize across distinct capability URLs, symlinks,
and hard links; disconnected waiters release their file handle and lock waiter.
Any other HTTP method returns `405` with `Allow`. Replies set `Cache-Control:
no-store`, `Content-Type: application/octet-stream`, and `X-Accel-Buffering:
no`. `/t/` runs ahead of the bearer check and the `4` MiB `/mcp` body cap.
Behind the bearer check, `/mcp` is the sole route and any other path returns
`404`, so an unstripped proxy prefix fails immediately on the first `/mcp`
request.

## 7. MCP Events and daemon emit

### Capability and protocol methods (`src/mcp.rs`, `src/events.rs`)

On MCP protocol revision `2026-07-28`, `server/discover` and `initialize`
advertise `io.modelcontextprotocol/events` with `{ "listChanged": false }`.
`fastexec` implements `events/list`, `events/poll`, and `events/stream` in both
local mode (`FASTEXEC_DAEMON=off`) and shared daemon mode (`FASTEXEC_DAEMON=on`,
stdio and Streamable HTTP). Webhook methods `events/subscribe` and
`events/unsubscribe` return JSON-RPC `-32601` (`Method not found`).

HTTP header routing enforces strict `Mcp-Name` validation (`src/mcp.rs`), using
the same MCP header value encoding as `Mcp-Param-*`:

- `events/list` accepts `null` or `{}` params and rejects any request carrying
  an `Mcp-Name` header with JSON-RPC `-32020`.
- `events/poll` and `events/stream` require `params.name` and validate
  `Mcp-Name` whenever the header is present: duplicate `Mcp-Name` headers are
  rejected; non-empty printable ASCII (`0x20..=0x7E`) values without leading or
  trailing space (`' '`) or horizontal tab (`'\t'`) and not starting with
  `=?base64?` pass verbatim, while all other values use standard `=`-padded
  Base64 wrapped as `=?base64?<standard-base64>?=` decoding to valid UTF-8. Any
  malformed header or mismatch against `params.name` fails with JSON-RPC
  `-32020`.

### Event catalog and schemas (`src/events.rs`)

`events/list` returns `{ resultType: "complete", events: [...] }` describing two
event types, each supporting `delivery: ["push", "poll"]`:

| Event `name` | `inputSchema` properties (`additionalProperties: false`) | `payloadSchema` properties (`additionalProperties: false`) | Summary |
| --- | --- | --- | --- |
| `task.exited` | `taskId?: string`, `scope?: "all" \| "session"` (`"all"` default; `"session"` requires an active session and rejects sessionless calls with `-32602`) | Required: `taskId: string`, `state: string`, `expired: boolean`, `keptProcesses: integer` (`>= 0`), `treeEnded: boolean`; optional: `exitCode: integer`, `logPath: string` | Published when a bash or `codemode` task exits, is killed, or expires via `killAfterMs`. |
| `external.event` | `source?: string`, `topic?: string`, `name?: string` | Required: `source: string`, `name: string`, `eventId: string`, `data: any`; optional: `topic: string`, `timestamp: string` | Published by `fastexec daemon emit` using pure SEP-3415 `eventId` naming (no `upstreamEventId` or `upstreamTruncated`), deduplicated in the ring buffer by `(source, eventId)`. |

### Ring buffer, polling, and streaming

Each `Service` owns a bounded in-memory `EventBus` (`src/events.rs`) capped at
`DEFAULT_MAX_EVENTS = 4096` records and `DEFAULT_MAX_BYTES = 8 MiB`
(`8,388,608` bytes). Enqueuing a new event evicts the oldest records until both
bounds hold and removes evicted `(source, eventId)` keys from the deduplication
map.

- `events/poll`: accepts `{ name, arguments?, cursor?, maxAgeMs?, maxEvents? }`,
  caps returned items at `MAX_POLL_EVENTS = 64`, and returns `{ resultType:
  "complete", events: [{ eventId, name, timestamp, data }], cursor, truncated,
  hasMore, nextPollMs }`. Items in `events[]` omit per-item `cursor`;
  `nextPollMs` is `2000` ms (`POLL_NEXT_ACTIVE_MS`) when `hasMore` is true and
  `5000` ms (`POLL_NEXT_IDLE_MS`) when caught up, and `truncated: true` marks
  evicted history before the caller's `cursor`.
- `events/stream`: accepts `{ name, arguments?, cursor?, maxAgeMs?,
  heartbeatMs? }`, clamping `heartbeatMs` to `5000..=30000` ms (default `30000`
  ms). Every stream notification and the final `{ resultType: "complete" }`
  result attach `_meta["io.modelcontextprotocol/subscriptionId"] = <request
  id>`. The stream sends an initial `notifications/events/active` (`{ cursor,
  truncated, heartbeatMs }`), followed by `notifications/events/event` (`{
  eventId, name, timestamp, data, cursor }`), idle
  `notifications/events/heartbeat` (`{ cursor }`), or
  `notifications/events/terminated` when the stream ends due to lag, bus
  shutdown, or per-stream memory backpressure.
- Error codes: `InvalidParams` maps to `-32602`, `NotFound` to `-32023`,
  `Forbidden` to `-32024`, and `ResourceExhausted` to `-32025` (carrying `{
  limit, max }` in `error.data`).

### Stream quotas and IPC backpressure isolation (`src/daemon/events.rs`, `src/endpoint.rs`)

Active streams are bounded by `MAX_EVENT_STREAMS = 64` per server or daemon
(`limit: "global_event_streams"`) and `MAX_EVENT_STREAMS_PER_CONN = 16` per
daemon IPC connection (`limit: "connection_event_streams"`). Exceeding either
cap rejects the subscription with JSON-RPC `-32025`.

On daemon IPC connections, ordinary `EventWireFrame::Event` frames acquire
memory exclusively from the `1 GiB` main byte pool (`daemon.memory`) and never
consume permits from the `8 MiB` `daemon.reserved` control pool (`reserved` is
restricted to short control, error, and termination frames). When the main pool
cannot admit an outbound event frame for a stream, the daemon cancels only that
stream and queues `EventWireFrame::Terminated` (`code: -32025`) using the
control reserve without calling connection-level `closing.cancel()`, keeping
sibling tool calls and other event streams on the shared IPC connection alive.

On the endpoint side, receiving a stream overload or broadcast-lag termination
delivers `notifications/events/terminated` (`-32025`) to that stream and exits
cleanly without entering a zero-backoff reconnect loop. `events/list`,
`events/poll`, and `events/stream` reconnect at most once when an idle IPC
connection closed before the request frame was written.

### `daemon emit` and IPC protocol 6 (`src/cli/daemon.rs`, `src/ipc.rs`, `src/daemon.rs`)

`fastexec daemon emit [--namespace <ns>] [--source <src>] [--topic <topic>]
[--stream]` injects external SEP-3415 events into an already-running daemon
(exiting `1` without starting a daemon when none is running). Without
`--stream`, it reads one JSON object from `stdin`; with `--stream`, it reads
newline-delimited JSON (NDJSON) objects and writes one JSON acknowledgement per
input line. Both each raw input payload and each serialized `external.event`
payload cap at `MAX_EXTERNAL_EVENT_BYTES = 64 KiB` (`65,536` bytes).

Each input JSON object follows strict SEP-3415 event naming:

- Required fields: non-empty `eventId` (`string`), non-empty `name` (`string`),
  and `data` (any JSON value), plus non-empty `source` (`string` in JSON or
  supplied via `--source`).
- Optional fields: `timestamp` (RFC 3339 string; defaults to daemon receive time
  when omitted), `topic` (`string` in JSON or via `--topic`), and `eventType`
  (must equal `"external.event"` if set, otherwise rejected with `-32024`).
- Ignored transport wrappers: `cursor` and `_meta` are ignored so output from
  `mcpcli events poll` or `mcpcli events stream` pipes directly into `daemon
  emit --stream`.
- Rejected legacy or unknown keys: `upstreamEventId`, `truncated`,
  `upstreamTruncated`, and any other unknown top-level key fail validation.

Over IPC (`ipc::PROTOCOL = 6`), `daemon emit` opens a lightweight connection
using `ToDaemon::HelloEmit { protocol: 6, build }`, which sends no `cwd` or
`env` snapshot and registers no daemon session entry. Each event travels in
`ToDaemon::EmitEvent { event }` charged to the main byte pool (never routed
through `control_pool`), and the daemon replies with `ToEndpoint::EmitAck { ok,
eventId?, cursor?, deduplicated, error? }`. Concurrent emit connections cap at
`MAX_EMIT_CONNECTIONS = 128`, tracked independently of the `128` endpoint
connections.

### Background footer `External events` summary (`src/events.rs`)

The event bus tracks per-session background cursors in an LRU map capped at
`MAX_SESSION_CURSORS = 4096` sessions. When building the background footer for a
direct MCP tool call (`start`, `poll`, `kill`, `transcript`, or `codemode`),
`peek_external_background_summary` inspects unseen `external.event` entries for
that session and formats a single metadata-only line:

`External events: <total> new (<source>[/<topic>]:<name>[ x<count>], ...)`

Labels sanitize control characters, cap at `48` characters each, and group up to
`3` distinct `<source>[/<topic>]:<name>` keys in arrival order (appending `,
+<m> more` when further groups exist, or falling back to `External events:
<total> new; use events/poll.` when tight on the `512`-byte footer budget).
Event `data` is never included in the footer. `commit_external_background_cursor`
advances the session cursor only after the direct tool response is committed for
delivery; cancelled calls and nested `codemode` calls leave the cursor
untouched.
