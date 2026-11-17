# fastexec Reference and Architecture

Authoritative runtime usage rules live in
[`src/description.md`](../src/description.md) and request validation in
[`src/service.rs`](../src/service.rs).

## 1. Tool Actions and Parameter Limits

Each tool accepts a single JSON object (`deny_unknown_fields`, `camelCase`),
using a portable JSON Schema with no root combinators and flat scalar/enum
types. `null` values are treated as absent everywhere, and `false` booleans
count as absent in inapplicable-parameter and `screen` combination checks while
retaining their meaning where applicable (`eof: false`, `loginShell: false`).
Other inapplicable values or out-of-range parameters return an error before
execution, naming `exec` actions in lowercase.

| Tool | Parameters | Summary |
| --- | --- | --- |
| `exec` | the action table below | Runs bash commands as tasks. |
| `codemode` | `code`, `cwd`, `waitMs`, `returnWhen`, `killAfterMs`, `truncate`, `maxBytes` | Runs async JavaScript as a task when `FASTEXEC_CODEMODE=true`. |
| `read` | `path`, `offset`, `limit` | Shows a text file, or an image inline. |
| `write` | `path`, `content` | Saves a complete replacement of a regular file. |
| `edit` | `path`, `edits` | Applies exact text replacements to a UTF-8 file. |
| `transfer` | `path` | Publishes one regular file at a URL on HTTP, returning `{ ok, url, path, bytes }`. |

### `exec` Actions

| Action | Allowed Parameters | Summary |
| --- | --- | --- |
| `start` | `command`, `cwd`, `pty`, `loginShell`, `input`, `eof`, `killAfterMs`, `keepTree`, `cols`, `rows`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Rejects NUL bytes in `command` before execution, then spawns it in GNU bash and waits up to `waitMs` (default `30000`, range `0–240000` on direct calls; unbounded by default without `returnWhen` in nested `codemode` calls). Pipe mode defaults stdin to `/dev/null`, writes `input` (`<= 1` MiB UTF-8) and closes stdin unless `eof: false`, or keeps stdin open with `eof: false`; PTY mode rejects `input` and `eof` on `start` and defaults size to `120×30`. |
| `poll` | `taskId`, `cols`, `rows`, `input`, `keys`, `keyDelayMs`, `eof`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Resizes the terminal (PTY only), writes `input` (`<= 16` KiB), presses `keys` (PTY only), applies `eof` (pipe only, idempotent when already closed), waits (`2000` ms with a resize, `input`, or `keys` and no `returnWhen`; `30000` ms on direct calls or with `returnWhen`; unbounded by default on nested `codemode` waits without `returnWhen` or input), and returns unseen output. |
| `kill` | `taskId`, `truncate`, `maxBytes`, `waitMs` (ignored) | Terminates the task's process tree (waits up to `5` s) and returns unseen output through the reader gate without `waitEndedBy` (`head_tail` default on direct calls; uncut `none` window up to `4` MiB on nested calls). On a finished `keepTree` task, ends held processes while keeping the root exit result. |
| `list` | `scope`, `state`, `limit`, `truncate`, `maxBytes` | Lists bare task IDs newest first filtered by `scope` (`"session"` default or `"all"`), `state` (`"active"` or `"finished"`; omitted lists both), and `limit` (`1–500`, default `20`), reporting `totalCount` before `limit`; creates no readers and marks only delivered entries. |
| `transcript` | `taskId`, `truncate` (default `"tail"`), `maxBytes` | Reads a UTF-8 PTY task's terminal transcript from the first unseen final line, followed by pending lines. Does not wait or advance the `poll` cursor. |

### Key Parameter Rules

- **`taskId` and readers**: Bare task IDs are `<8-char epoch>-<1..=13-char seq>`
  in Crockford base32; in daemon mode a well-formed ID with a foreign epoch
  reports that it came from an earlier daemon, whereas malformed IDs report
  `Unknown taskId`. `start`, `poll`, `kill`, and `transcript` return
  a reader handle, `<taskId>.<reader>` (four Crockford base32 characters), in
  the status line and structured result. Reusing a handle in the same session
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
- **`waitMs` vs. `killAfterMs`**: `waitMs` (`0–240000`) bounds only the current
  call's wait. A call that hits `waitMs`, matches `returnWhen`, or is cancelled
  leaves the task running (use `list` to recover the `taskId` of a cancelled
  `start`). `killAfterMs` on `start` arms an independent timer that kills the
  owned tree, including kept processes. Expiry while the root runs reports
  `state: "killed"` and `lifetimeExpired: true`; after root exit, held processes
  end but the root's original result stays.
- **`returnWhen`**: Ends the wait early when any condition holds (`waitEndedBy`
  reports `{ condition, text? }`):
  - `outputContains`: `1–16` literal case-sensitive strings (`1–4096` bytes
    each, no line breaks) matched against cleaned unseen output lines (including
    text before a carriage return overwrites it).
  - `screenContains`, `screenAppears`, `screenGone` (UTF-8 PTY only): match
    against the live screen at the task's size (`screenAppears` and `screenGone`
    compare against the screen captured before the call's resize and `input`).
  - `quietMs`: `10–60000` ms without output after `input` and `keys` finish.
- **Window and encoding options**:
  - `maxBytes`: `100–1048576` (default `16384`). Rejected when
    `truncate: "none"`. Bounds the output window, status line, and background
    footer together; metadata and JSON framing are outside that budget. Correctness
    warnings remain whole even when they exceed the budget.
  - `truncate`: `"head_tail"` (default for `start`/`poll`/`kill`: ~10% head,
    ~90% tail), `"head"`, `"tail"` (default for `transcript`, including nested
    calls that pass `maxBytes` alone), or `"none"` (all unseen lines whole up to
    `4` MiB, with the rest reported as an omitted range). Direct bash calls
    clean ANSI/CR rewrites unless `raw: true` and cap each line in bounded modes
    at `2000` characters (`64` KiB with `raw: true`), reporting `cutLines`.
    Outer `codemode` results skip cleaning, and both outer `codemode` and nested
    `exec` results skip fixed line-length limits: lines stay whole unless a
    single line exceeds
    the call's byte budget (`maxBytes` or the `4` MiB `none` cap), in which case
    the prefix and ` … [line cut at <budget>; <n> more bytes]` fit the budget
    together on a UTF-8 boundary, and `head_tail` reserves enough head share to
    keep the first fitting line whole.
  - `encoding`: Any ASCII-compatible WHATWG label (such as `"utf-8"`, `"big5"`,
    `"gbk"`; UTF-16 is rejected). On `start`, sets the task default; on `poll`,
    overrides for that call.
  - `screen`: Boolean (`false` default, UTF-8 PTY only). Returns the rendered
    screen at the task's size and 1-based cursor `[row, col]`, marking the output
    stream read up to that point. Cannot combine with `truncate`, `raw`, or
    `encoding`.
  - `cols`, `rows` (PTY only): the terminal's size. `start` takes them,
    defaulting `120×30`; `poll` takes them too, because a resize is
    terminal-side input: a poll applies the resize, then `input`, then `keys`,
    then waits. Either axis may come alone and an omitted axis keeps its value;
    bounds are `cols` `20–500` and `rows` `5–200`; an unchanged size is a no-op.
    A pipe task refuses both.

### Codemode

`FASTEXEC_CODEMODE=true` adds `codemode` and includes JavaScript orchestration
guidance in server `instructions`. Its required `code` is an async
function body with top-level `await` and `return`, up to 256 KiB of UTF-8,
nonempty after trimming and without NUL bytes. Unknown parameters are refused.
It shares `exec start`'s directory, wait, and output options except `raw`:
`cwd`, `waitMs`, `killAfterMs`, `truncate` (default `none` up to `maxBytes`,
cutting only lines over budget), `maxBytes`, and `returnWhen`
(`outputContains`, `quietMs`). Schema-decoding failures return a tool error
without `structuredContent`; parameter validation and admission failures return
`isError: true` with `structuredContent` (`ok: false`, `action: "codemode"`)
without creating a task.

A script is a task with `kind: "codemode"`. The start result has
`action: "codemode"`; later `exec poll`, `kill`, and `list` use the same task
and reader handles (`kill` also attaches pending `image()` outputs in the
committed cursor range). A list command is `[codemode] <code>`, flattened and
truncated like bash commands. Scripts reject non-false `input`, `eof: true`,
`keys`, `keyDelayMs`, resize, `screen: true`, `raw: true`, `encoding`, and
`transcript` requests (`null` and `false` are ignored). The script has no
terminal and no process tree of its own.

The VM uses synchronous rquickjs 0.14.0 on a dedicated thread, with 128 MiB
of VM memory, a 512 KiB JavaScript stack, and a 2 MiB thread stack. A local
server admits four scripts and a daemon eight, within the existing total task
limit. The VM, output reader and capture workers are prepared before admission
is published; a failed preparation returns its capacity and files.

The script globals are `tools.exec/read/write/edit`, `sh.quote`, `text`,
`console`, `image`, `exit`, and UTF-8 `TextEncoder`/`TextDecoder` (alongside
QuickJS's built-in `atob`/`btoa`). `sh` is frozen; `sh.quote(value)` formats a
string or finite number as one POSIX single-quoted word and throws `TypeError`
for `NaN`, infinities, NUL bytes, or other types. There are no modules, Node
APIs, timers, store/load, network or direct filesystem APIs. `text` appends
strings as-is and other values as JSON (preserved verbatim without ANSI or
trailing-space stripping). Console methods join formatted arguments with spaces
and show Error stacks. A non-undefined return value is appended last. `exit()`
ends successfully even inside a try/catch. Each script owns its globals; bridge
and codec functions bind the built-ins they depend on before user code runs.

Nested calls resolve directly to unwrapped structured objects (omitting `ok`).
Schema-invalid argument shapes and unknown tools reject with `TypeError`;
validation, file, and integrity failures reject with `ToolError` (`name:
"ToolError"`, `tool`, `result` carrying full diagnostics such as `action`,
`output`, `exitCode`, `inputError`, or `failures`), while non-zero command
`exitCode`s resolve normally. `exec` returns `{action, taskId, state, exitCode,
output, ...}` (keeping lines uncut up to `maxBytes` or the `4` MiB default `none`
window, and waiting with no `waitMs` ceiling unless `waitMs`, `returnWhen`, or
input/resize/keys supplies one); text `read` returns up to `50000` lines and
`1` MiB per call as `{path, text, truncated, nextOffset?, startLine, endLine,
shownLines, totalLines, lineTooLong?, lossy?}`; image `read` returns `{path,
attached: true, image, mimeType, bytes, originalBytes, resized}` or `{path,
attached: false, note}` (`image()` throws `TypeError` with `note` when
`attached` is `false`); `write` returns `{path, bytes}`; and `edit` returns
`{path, replacements, edits}`. Eight nested calls run at once, with
1024 queued calls; additional calls reject with RangeError. Pending tool
arguments and results have a separate 128 MiB budget per script; exceeding it
rejects the call with RangeError. A completed result that cannot fit rejects
after the operation ran; its error identifies this case, and the operation
remains in effect. A script awaiting a promise with no
pending tool call or JavaScript job fails because it has no way to settle.

Calls use the script's session and launch context (with `cwd` overridden when
the script sets `cwd`). A nested task's reader can be polled by the caller, and
IPC children retain the outer call ID. Nested calls advance their reader state;
tasks started outside the script keep their session background notifications
pending, whereas tasks started by the script whose completion is already
observed inside the script mark their session entry reported so finished helper
commands do not pollute subsequent outer `Background:` footers. When a script
ends, in-flight nested calls are cancelled and drained before publishing the
final script report, so any `exec start` already inside process creation is
recorded in `startedTasks`. Each script accepts at most 4096 validated exec
start calls, counting calls that later fail or are cancelled. Further starts
reject with RangeError, so the complete startedTasks history stays bounded.

Script output streams through the existing pipe capture and task log. The
64 MiB per-script limit includes text and image files, with 4 KiB reserved
for final diagnostics. Normal completion and `exit()` have exit code 0;
syntax errors, uncaught exceptions, unhandled rejection, stalled promises
and resource failures have exit code 1 with a `Script error:` line. These are
normal task results (`isError: false`). Stop has state `killed`, null exitCode,
and a line naming cancelled calls; deadline stops also set lifetimeExpired.

The script's cancellation token is independent of a request's waiting token.
Kill, deadline, session end in local mode, or daemon stop interrupt bytecode
and the job loop, cancel pending nested calls and wake an idle VM. The final
state is published after capture drains. Any script ending cancels calls it
did not await. Completed operations remain in effect; bash tasks it started
remain independent tasks. `codemode.cancelledCalls` identifies calls cancelled
on termination, and `codemode.startedTasks` lists all registered handles with
states queried at each response. Status lines show up to three still-running
handles and report when a kill has waited five seconds without completion.

`image()` accepts an image block, base64 data URL or tool result containing
an image. Detection uses the bytes, ignores a claimed MIME type, and shares
read's normalization to at most 2000 by 2000 pixels and less than 4718592
base64 bytes. Conversion failures write an omission line. A script saves up
to 64 images next to its log; file sizes count toward the per-script and total
log budgets. A completed file and its marker's ending byte position are
registered before the marker is written, so a concurrent reader cannot miss it.

A response selects images whose marker endings fall within its committed
reader cursor range, even when the text window omits those lines. Each reader
gets an image at most once. The content blocks follow text in log order,
with at most four images and 4718592 base64 bytes per result. The counters
`codemode.imagesAttached` and `codemode.imagesNotAttached` explain attachment;
the latter contains `{index, path, reason}` with `budget`, `log_dropped`, or
`evicted`; an evicted entry omits its unavailable path. Budget warnings
preserve paths for a later read. Cursor and image
state commit together; cancelled reads do not consume them. Eviction and task
retirement remove image files and reclaim their quota along with logs.

### File Tools

- **`read`**: Shows up to `2000` lines and `50` KiB of a text file (`50000`
  lines and `1` MiB when called from `codemode`), with
  `offset` (1-indexed line) and `limit` (lines) selecting a window. Line counts
  follow real `\n` / `\r\n` lines (`""` has `totalLines: 0`, `"a\n"` and `"a"`
  have `totalLines: 1`, and `offset: 1` on an empty file returns `""` with
  `startLine: 1`, `endLine: null`, `shownLines: 0`, `truncated: false`), preserving original line
  terminators in the returned slice and emitting truncation or lossy notices
  without leading blank lines when the body is empty. A recognized image
  (JPEG, PNG, GIF, WebP) comes back inline, up to `2000`×`2000` pixels and
  `4718592` bytes of base64; a BMP is re-encoded as PNG, or as JPEG when the
  resize needs it, and a file over `512` MiB, or an image whose conversion or
  resize cannot fit, returns `attached: false` and the note as text instead of
  an image. Images ignore `offset` and `limit`. A non-regular file, and one
  that is not a recognized image and has a NUL byte in its first `8` KiB, are
  refused; any other file decodes as text, lossily, with a note naming the
  replacements.
- **`write`**: Saves `content`, creating missing parent directories of the final
  target, including a dangling symlink's resolved target. Relative, absolute,
  and chained symlinks are preserved; `edit` still requires an existing file.
  Content is staged in a private sibling file and synced before it replaces the
  destination, so a failed content write leaves the original intact. The parent directory
  must be writable. Existing owner, permissions, and access-control metadata
  are preserved; symlinks keep their normal target semantics. Non-regular files
  and files with multiple hard links are refused: use `exec` for a deliberate
  in-place update of those files. On Windows, a caller unable to retain a
  different owner or group gets an error before replacement. If native file
  replacement and its rollback both fail, the original is retained at the
  recovery path named in the error rather than deleting the only remaining copy.
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
- **`edit`**: Each `edits[]` entry is `{oldText, newText}`, matched against the
  original file, not against the result of an earlier entry. Every `oldText`
  must match at least once (`not_found`), be unique over every start position
  including overlapping ones (`ambiguous`, reporting `lines` in match order with
  repeats kept), and not overlap any other matched span (`overlap`, sweeping
  across all overlapping spans rather than only adjacent ones and listing
  partner indices in `with`). `lines` and `with` cap at `20` items (`more`
  counts any remainder), and a batch that matches uniquely without overlap but
  leaves the file unchanged fails with `failures: []`. Instead of stopping at
  the first bad entry, `edit` evaluates every entry and reports `{ ok: false,
  error, path, failures }`, counting line numbers on real `\n` breaks so they
  match `read`. The file must be UTF-8; a byte-order mark
  and every unmatched byte are kept, including mixed CRLF, LF, and CR line
  endings, and inserted text follows the file's first line ending style. The
  success result names each replaced block's first line in the file it produced,
  marking fuzzy matches. Saving uses the same staged replacement and file
  restrictions as `write`. Both `read` and `edit` validate the opened file as
  regular; an edit verifies the target's identity, contents, and path again
  before publishing.
- **`transfer`**: Resolves `path` with the unified path rules, refuses
  directories and non-regular files (hinting `tar` via `exec`) and non-missing
  metadata errors, and returns `{ ok, url, path, bytes }` where `bytes` is a
  size snapshot at mint time or `null` when the file does not exist yet.
- **Paths**: Every tool `cwd` and `path` resolves through one path helper:
  expands `~` / `~/...`, resolves relative paths against the effective `cwd`
  (a `codemode` call's `cwd` or the session's working directory), strips `.` and
  repeated separators while leaving `..` to the OS kernel so symlinks resolve
  as the filesystem sees them, accepts Windows `C:/...`, `C:\...`,
  `//server/share/...`, and Git Bash `/c` or `/c/...`, refuses Windows
  driveless rooted paths (`/tmp/...`, hinting `cygpath -m`) and drive-relative
  paths (`C:foo`), and formats every reported path with forward slashes
  (stripping `\\?\` verbatim prefixes and rendering UNC paths as
  `//server/share/...`).
- **Mutation**: `write` and `edit` serialize with each other inside one server
  process. They do not coordinate with `exec`, with a `transfer` upload, or with
  another process.

## 2. Host Configuration and Output Modes

### Progress Notifications and Timeouts

When a request carries a `progressToken`, the server emits progress every
`20` seconds during waits. Clients that refresh their timeout on progress,
including Pi's built-in MCP client, can keep long waits alive.

Other hosts need a tool timeout of at least `300` seconds. In the default mode,
keep the MCP server connected for the task's lifetime: an idle disconnect ends
its tasks. In `on` mode a disconnect ends nothing; the daemon keeps the tasks
(section 6).

### `FASTEXEC_STRUCTURED_CONTENT` and `FASTEXEC_OUTPUT_MODE`

Set these strings in the MCP server's `env` configuration. Both are read at
startup; invalid values exit with status `1`. They shape tool results across
`exec`, `codemode`, `read`, `write`, `edit`, and `transfer`.

- **`FASTEXEC_STRUCTURED_CONTENT`**: `"true"` (default) attaches
  `structuredContent` JSON metadata; `"false"` omits `structuredContent` on all
  results and uses text output placement.
- **`FASTEXEC_OUTPUT_MODE`** (when structured content is enabled):
  - `"text"` (default): `content` holds the full text (output body + status/footer
    for tasks, or file text + notices for `read`); `structuredContent` holds
    metadata without duplicating task output or file text.
  - `"both"`: `content` holds the full text, and `structuredContent.output`
    (tasks) or `structuredContent.text` (text `read`) also holds the clean body.
  - `"structured"`: `content` holds only the status/notice line;
    `structuredContent.output` (tasks) or `structuredContent.text` (text `read`)
    holds the clean body (for hosts like Codex that consume structured tool output).

### `FASTEXEC_DAEMON`

`"off"` (default) keeps tasks in this server's own session. `"on"` forwards
every tool call to the namespace's daemon (section 6). The value is read at
startup; an invalid value exits with status `1`.

### `FASTEXEC_CODEMODE`

`"false"` (default) hides the codemode route. `"true"` exposes it and adds
script-specific poll and kill rules to the exec description, before its final
Host line. Local stdio and daemon endpoints use their own startup environment;
HTTP uses the daemon's. The daemon accepts forwarded codemode calls regardless
of its advertised setting, within the same session and task permissions as exec.
Invalid values fail stdio startup, daemon start, and daemon run with exit 1.
The IPC protocol version is unchanged. YOLO rewrites codemode annotations just
as it does exec, after disabled routes have been removed.

### `FASTEXEC_YOLO`

`"false"` (default) keeps each tool's default MCP `annotations` hints.
`"true"` sets `readOnlyHint: true`, `destructiveHint: false`,
`idempotentHint: false`, and `openWorldHint: false` on every exposed tool
while keeping each tool's `title`. The value is read at startup by the MCP
server process (stdio, daemon endpoint, and HTTP daemon); an invalid value
exits with status `1`.

A nonzero command `exitCode` is a normal result (`ok: true`). Operational
failures set `isError` and `ok: false`; schema-decoding errors may have no
`structuredContent`.

### `structuredContent` Fields

These fields describe `exec` results; codemode adds `codemode` when non-empty.
Default-valued flags, zero counters, empty arrays, and null diagnostics are
omitted on final serialization unless noted as always-present below.

- **Always present on `start`, `poll`, `kill`**: `ok` (outer calls only),
  `action`, `kind` (`"bash"` or `"codemode"`), `taskId` (reader handle),
  `state` (`"running"`, `"exited"`, `"killed"`), `exitCode`, `pty`,
  `elapsedMs`, `logPath`, and `waitEndedBy` (`start` and `poll` only; `kill`
  omits `waitEndedBy`). Status lines omit ` · log <path>` when a finished task
  has no omitted or cut output, no log-loss warning, and no open output or kept
  processes, while `structuredContent.logPath` stays present. Diagnostics
  omitted when false, zero, empty, or null: `keepTree`, `keptRunning`,
  `forcedEnd`, `lifetimeExpired`, `logEvicted`, `logError`, `captureError`,
  `outputIncomplete`, `outputOpen`, `droppedBytes`, `inputWarning`,
  `inputError`, `warnings`, `leftovers` (`[{ pid, name }]`), `held` (`[{ pid,
  name }]`), `killError`, `encodingErrors`, `omittedLines`, `omittedRange`
  (`[first, last]`), and `cutLines`. With `screen: true`, `cols`, `rows`, and
  `cursor` (`[row, col]`) are always present and `omittedRows` is included when
  non-zero.
- **`transcript`**: Always includes `ok` (outer), `action`, `kind`, `taskId`,
  `state`, `exitCode`, `pty`, `elapsedMs`, `logPath`, `transcriptPath`,
  `transcriptLines`, `finalLines`, and `firstLine`, plus non-default window and
  task diagnostics when set.
- **`codemode` report**: Included when any counter or array is non-empty
  (`imagesAttached`, `imagesNotAttached`, `cancelledCalls`, `startedTasks`).
- **`read`**: Keeps its data fields unchanged (`endLine: null`,
  `nextOffset: null`, `lineTooLong`, `lossy`, and `attached: false` are data;
  nested calls strip `ok` and attach `image` or `note` on image reads).
- **`list`**: Always includes `ok` (outer), `action`, `tasks` (built after
  `limit`), and `totalCount`; each entry always carries `taskId` as a bare task
  ID, `kind`, `state`, `exitCode`, `pty`, `elapsedMs`, `command` up to 120
  chars, `owner`, and `logPath`, omitting `cols`, `rows`, `session`, `callId`,
  and task diagnostics when null/false/zero/empty. `warnings` covers selected
  tasks only and is omitted when empty.
- **Integrity and initial-input diagnostics**: On pipe `start`, an early child
  close (`BrokenPipe` or Windows pipe-closed codes `109`/`232`/`233`) sets
  `inputWarning: "stdin closed after N of M bytes"` with `ok: true`, whereas
  other initial write failures or any `poll` write failure set `ok: false` and
  `inputError`; the writer joins within the `5` s capture drain before final
  state publication. Output capture or log write failures also set `ok: false`
  and `isError: true` while preserving `exitCode` and captured output. Warnings
  stay whole regardless of `maxBytes`.
- **Background footer**: Appended to `start`, `poll`, `kill`, and `transcript`
  results (up to `512` bytes and `maxBytes / 4`) naming up to three other tasks
  this session has addressed, using bare task IDs. HTTP clients sharing a token
  share this scope. Running tasks (or tasks holding processes) outside this
  session appear **at most once per session** as `1 task running outside this
  session; use list with scope "all".` or `<n> tasks running outside this
  session; use list with scope "all".`; the session flag is consumed only when
  a delivered response actually renders that clause (never on cancelled calls or
  tiny budgets that drop it). A footer marks only session tasks it names; `list`
  marks only shown tasks' existing session entries.

## 3. Logs, Terminal Emulator, and Transcript Spool

- **Logs and retention**: File names use the bare task ID, without the reader
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
  are never replaced; unsupported filesystems are refused rather than using an
  overwrite-capable fallback. UFS and ZFS are tested. Existing directories with
  non-trivial access ACLs or inheritable POSIX default ACLs are refused without
  changing those ACLs. Its executable cache
  and short-path sockets each sit inside an additional private directory created
  after the outer directory's ACLs are removed, so an earlier `O_SEARCH` handle
  cannot reach them. Windows validates
  an existing directory's owner, DACL, and reparse
  status. `on` mode logs under
  `<runtime root>/ns/<namespace>/logs/` (section 6). Each task log caps at `64`
  MiB, and all logs on a server cap at `1` GiB total (evicting oldest finished
  task logs first). Past a cap or write error, capture keeps draining output
  and the status line reports dropped bytes. The default mode allows at most
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
- **Live PTY emulator and continuous transcript spool**: Each UTF-8 PTY task
  feeds output bytes into an `alacritty_terminal` (`0.26.0`) instance at the
  task's size (`src/terminal.rs`, `src/frames.rs`) that answers terminal queries
  (`ESC[6n`, `ESC[c`), tracks keyboard modes, serves `screen: true`, and
  continuously spools `<taskId>.transcript.txt` (up to `64` MiB, counted under
  the `1` GiB total limit). A resize settles the transcript at the old size,
  reflows at the new one, and starts the recorder fresh, so transcript text is
  never lost on either screen, and rows still visible after the resize may appear
  twice; arbitrary application redraws keep today's best effort. Lines scrolled off the normal screen and merged frames from
  alternate-screen sessions (`--- alternate screen ---` /
  `--- normal screen ---`) are written as final lines once settled. A
  `transcript` call returns unseen final lines followed by current pending
  lines; pending lines may change on the next call.
- **Bounded terminal state**: A rendered cell retains at most `32` user combining
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

## 4. PTY Keys and Bundled ConPTY

- **Key names and modes (`src/keys.rs`)**: `keys` uses case-insensitive tmux
  `send-keys` names (`Enter`, `Tab`, `BTab`, `Escape`, `Space`, `BSpace`,
  arrows, `Home`, `End`, `PageUp`/`PageDown`, `Insert`, `Delete`, `F1`–`F12`,
  keypad keys, `[NUL]`–`[US]`, `0xHH`, or single characters) with `C-`, `M-`,
  `S-`, and `^` modifiers. Keys encode according to the program's active
  keyboard mode: standard xterm/tmux sequences (respecting DECCKM cursor and
  DECKPAM keypad modes) or the kitty keyboard protocol when enabled. Initial
  validation rejects unknown or unencodable keys before input delivery; a later
  keyboard-mode change can fail a key after earlier input has been sent.
- **Key pacing**: `keyDelayMs` (`0–2000`, default `150`; requested pauses per
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
- **Bundled ConPTY (`src/conpty.rs`)**: Windows x64 and arm64 builds embed Microsoft
  ConPTY `1.24.260710001` (`conpty.dll` and `OpenConsole.exe`, MIT). Each process
  extracts into a newly reserved owner-only directory named
  `%TEMP%\fastexec-conpty-<version>-<arch>-<pid>-<random>` and pins the verified files
  read-only. The next process retires private copies whose owning process ended.
  The exact verified DLL handle supplies the console API, and dependencies are searched
  only in its private directory and System32. Loading happens before the first PTY so
  terminal output passes through without legacy Windows console re-rendering. If loading
  fails, the already loaded system kernel supplies ConPTY; CWD and PATH are never searched
  for a replacement DLL. Transcript results warn that lines may be missing on that fallback.

## 5. Bash Launch and Process-Tree Ownership

- **Launch (`src/bash.rs`, `src/process.rs`)**: Runs `bash -l -c`
  (`loginShell: true`, default) or `bash --noprofile --norc -c`
  (`loginShell: false`). Commands over `8000` bytes run from a temporary script
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
- **Tree ownership (`src/process.rs`, `src/host.rs`)**:
  Unix entry points restore `SIGCHLD` to its default disposition with automatic
  reaping disabled before spawning children. Each task host's PID stays reserved
  until its owning server explicitly waits for it. Readiness follows the bash
  spawn attempt; if the host dies before readiness, startup cleanup ends its
  reserved process group before reaping the host. Unix PTY descriptors are
  created with `O_CLOEXEC`, so an unrelated spawned command cannot retain them
  across exec. Linux opens the matching slave with `TIOCGPTPEER`, so another
  devpts mount cannot redirect a pathname lookup. FreeBSD and macOS use a
  separate buffer per allocation, with `ptsname_r` and `TIOCPTYGNAME` respectively.
  - **Windows**: Runs each task inside a Job Object with
    `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` and breakaway disabled, because Git
    Bash starts `&` children with breakaway whenever a Job allows it.
  - **Linux**: A separate task host leads the session and adopts daemonized
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
  - **FreeBSD**: The task host acquires a kernel reaper with `PROC_REAP_ACQUIRE`
    before starting bash. `PROC_REAP_GETPIDS` identifies descendants, including
    nested reapers; `PROC_REAP_KILL` lets the kernel select signal targets
    without a numeric PID-reuse race. Detached sessions and double forks stay
    owned, without procfs. Partial kills and native permission errors keep the
    host alive and are reported rather than treated as an empty tree. The
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
  - **macOS**: The same task host owns membership, because nothing adopts an
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
  - **`keepTree: true`**: Keeps leftover processes running after root bash exits
    until `kill`, `killAfterMs`, or the end of the tasks' owner: the server's
    session in the default mode, the daemon's stop or exit in `on` mode. Once the root exit has been
    reported, subsequent `poll` calls wait on the held tree
    (`waitEndedBy.condition: "tree_ended"`), and a `start`/`poll` with
    `returnWhen` waits past a root exit `0` for output from held processes.
  - **A root that does not terminate (Windows)**: A ConPTY root records its
    script's exit status as that script ends, in a file the task's directory
    holds. A root that has not terminated five seconds after its script finished
    is ended with that status, so the task publishes `exited` with `forcedEnd:
    true` and ` · root forced after 5 s exit grace` in the result line; its tree
    is then handled as on any root exit. The marker is installed only when the
    shell has no `EXIT` trap yet, so a startup file (`BASH_ENV`, a profile) that
    sets one keeps its cleanup; such a task, a command that sets its own trap,
    and a command that `exec`s all keep the plain wait.
  - **Limits**: Native permission failures appear in `killError`; unkillable
    processes remain held. Processes launched outside the owned tree, such as
    through a system service manager, are outside its cleanup scope.

## 6. Daemon Mode (`FASTEXEC_DAEMON=on`)

- **Endpoint and daemon**: The stdio process answers `initialize` and
  `tools/list` itself and forwards every tool call to the namespace's daemon,
  holding no tasks of its own and killing none. The first call works to one `10` s
  deadline that covers connecting, starting a daemon, and its `Hello`; only an
  absent daemon is started, and one that is there but does not answer in time is
  reported instead. A call whose connection fails after it was sent returns an
  error naming the call, request ID, session, and daemon epoch — the action for
  `exec`, the tool for `read`, `write`, and `edit` — and is not replayed.
  `list` shows each task's owner and, for a call through this endpoint, the
  session and call ID that started it; both are null for a task started in the
  default mode or over HTTP.
- **Identity**: The build ID is the SHA-256 of the running image. The namespace
  is the first `16` hex characters of `SHA-256(build ID, scope)`, where the
  scope is a non-empty `FASTEXEC_NAMESPACE`, else the canonical path of the
  executable, so different builds never share a daemon.
- **Runtime root**: `%LOCALAPPDATA%\fastexec` on Windows, created with an
  owner-only DACL; `$XDG_RUNTIME_DIR/fastexec` or `<temp>/fastexec-<uid>` on
  Unix, mode `0700` and checked. `ns/<namespace>/` holds `startup.lock`,
  `daemon.lock`, `daemon.log`, `logs/`, and the socket; `bin/<build>[.exe]` is the
  cached copy of the executable a daemon starts from. A daemon start sweeps the
  payload of every namespace whose locks it can take.
- **IPC**: A named pipe `\\.\pipe\fastexec-<user hash>-<namespace>` with an
  owner-only DACL that rejects remote clients, or a Unix socket at
  `ns/<namespace>/sock` when that path fits in `100` bytes, else at
  `/tmp/fastexec-<uid>/<namespace>.sock`. Frames are length-prefixed JSON, and a
  call names its tool (`exec`, `read`, `write`, `edit`). The endpoint refuses a
  call whose serialized arguments pass `3` MiB before writing it (sizing for up
  to `1` MiB of JSON-escaped `start` `input`), a request frame
  is at most `4` MiB (a `Hello` carries the endpoint's environment), a response
  frame at most `64` MiB, one connection carries at most `32` concurrent calls,
  and the daemon serves at most `128` connections.
- **Sessions**: Each endpoint process holds one random session ID with its own
  task entries and readers, and a reconnect keeps them. An endpoint session with
  no live connection is dropped after `10` minutes, and the table holds at most
  `256` endpoint sessions. The endpoint says `Bye` when its stdin closes, which
  drops that session at once. Sessionless HTTP has one principal session per
  configured token for the listener's entire lifetime; these sessions do not
  expire. A session's entries retire with their tasks.
- **Lifetimes**: The daemon does not exit idle by default. Starting it with
  `--idle-exit on` enables exit after `10` minutes with no connections and an
  empty task registry (finished tasks stay for up to `24` hours). `--idle-exit off`
  explicitly disables it. HTTP daemons never exit idle, even with `--idle-exit on`.
  `fastexec daemon stop` kills
  every task and exits. A daemon crash ends its tasks, through the Windows Job
  or the Linux task host, and a task ID from a dead daemon answers as coming
  from an earlier daemon.
- **Host confinement**: A host that keeps its children in a Job without
  breakaway, such as a fastexec task, cannot start a detached daemon, and the
  call fails naming `fastexec daemon start` from a terminal outside the host.
  `fastexec daemon run` is the supervisor's own process and serves inside such a
  host. A host that confines its children to a cgroup, such as a systemd scope or
  a container, ends the daemon with it.
- **CLI**: `fastexec` with no arguments serves MCP over stdio (exiting `1` after
  cleanup if MCP initialization fails); `--version` (`-V`) and `--help` (`-h`)
  print version or usage and exit `0`, and unknown or extra arguments print
  usage to stderr and exit `2`. `fastexec daemon start [--http <addr>] [--idle-exit on|off]` starts the daemon detached
  from the calling terminal and prints its epoch, PID, namespace, and HTTP
  address. `fastexec daemon run [--http <addr>]` serves the same daemon in the
  foreground, in the process a supervisor started: it takes the namespace lock
  like any daemon and refuses a namespace that already has one, prints the same
  line to stderr once both listeners are bound, keeps its logs on stderr, runs
  with idle exit off, and refuses `--idle-exit`. `SIGTERM` and `SIGINT`, and on
  Windows `Ctrl+C`, `Ctrl+Break`, console close and shutdown, stop it as
  `daemon stop` does, with exit `0`. That cleanup is best effort where the
  operating system ends the process first, by a close deadline it enforces or a
  forced termination. `status` prints epoch, build, PID, sessions,
  tasks, and the HTTP address or `off`; `stop` prints the stopped epoch. `status`
  and `stop` never start a daemon, and `stop` without one exits `0`. Start flags
  only apply to a new daemon; stop the existing one before changing its
  idle-exit policy.
- **HTTP**: `--http <addr>` with `FASTEXEC_HTTP_TOKEN`, one or more unique tokens
  separated by ASCII whitespace, serves the same toolset over Streamable HTTP.
  Each token is at least `16` visible ASCII characters; empty lists, invalid
  tokens, and duplicates are refused with token positions, never token values.
  Sessionless requests use the authenticated token's principal session (`t1`,
  `t2`, etc., with owner labels `http-t1`, `http-t2`, etc.). The inline `_meta`
  lifecycle chooses this path regardless of the named protocol version. Legacy
  HTTP `initialize` creates a client-named session after negotiation. The address is
  loopback unless `FASTEXEC_HTTP_ALLOW_REMOTE=1`, which belongs behind a
  TLS-terminating proxy. Every `/mcp` request carries a configured bearer
  token, checked before a session is created or resumed, and
  `FASTEXEC_HTTP_TOKEN` is dropped from the
  environment HTTP sessions launch tasks with; a `/mcp` body over `4` MiB is
  refused; `Origin` is
  limited to the bound address's own origin, the loopback names, and
  `FASTEXEC_HTTP_ALLOWED_ORIGINS`, where an entry without a port matches any
  port. This is request Origin validation, not CORS support: browser cross-origin
  preflights and CORS response headers are not provided. HTTP sessions run with
  the daemon's own environment. The `10` minute inactivity TTL expires idle
  legacy HTTP sessions without bounding their count or memory; principal
  sessions last until the listener stops.
  The HTTP listener admits at most `128` concurrent connections and allows
  `10` seconds to receive request headers. Extra connections are closed without
  queueing; management through the local daemon socket has its own capacity.
  This header deadline does not impose a total timeout on uploads, SSE, or
  long tool calls. Invalid HTTP syntax and ordinary client disconnects do not
  append one error-log entry per request. Streamed responses carry
  `X-Accel-Buffering: no`, which tells a proxy not to buffer them.
- **Transfer route**: `/t/<capability id>` serves one file without the bearer
  token, which is what makes a URL usable by `curl`; the `transfer` tool mints
  the id and builds the URL from `FASTEXEC_PUBLIC_URL` when the daemon was
  started with it, and from the request's own `Host` and scheme when it was not.
  An id expires after `10` minutes unused, and an unknown or expired one answers
  `404`. `GET` returns the file, honouring one `bytes=` range with `206` and
  `416` and ignoring any other form, so the whole file comes back with `200`.
  `HEAD` answers the current size. `PUT` writes the body at `?offset=` (default
  `0`), truncating the file to that offset before reading the body, refusing an
  offset past the end with `409`, and answering the new size on success. A
  truncated or malformed body returns `400`; the received prefix is retained,
  so `HEAD` and a later upload can resume from the actual size. Uploads to the
  same open file identity serialize even across distinct capability URLs,
  symlinks, and hard links; uploads to different files remain independent. A
  disconnected upload waiting for that lock releases its open file and waiter.
  Any other method answers `405` with `Allow`.
  Replies carry `Cache-Control: no-store`, `Content-Type:
  application/octet-stream`, and `X-Accel-Buffering: no`. `/t/` is answered
  before the bearer check and before the `4` MiB body cap that applies to
  `/mcp`. `/mcp` is the only other route: a path behind the bearer check that
  is not `/mcp` answers `404`, so a proxy that forwards its prefix unstripped
  fails at `initialize` rather than at a later download.
