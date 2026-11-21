# fastexec Reference and Architecture

Authoritative runtime usage rules live in
[`src/description.md`](../src/description.md) and request validation in
[`src/service.rs`](../src/service.rs).

## 0. Build and deployment

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

On Windows, [nssm](https://nssm.cc/usage) runs it under the user's own account
instead of LocalSystem:

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
| `read` | `path`, `offset`, `limit` | Reads a text file, or attaches an image, audio, PDF, or video file. |
| `write` | `path`, `content` | Saves a complete replacement of a regular file. |
| `edit` | `path`, `edits` | Applies exact text replacements to a UTF-8 file. |
| `transfer` | `path` | Publishes one regular file at a `/t/<id>` URL when `FASTEXEC_TRANSFER=true`, returning `{ ok, url, path, bytes }`. |

### `exec` Actions

| Action | Allowed Parameters | Summary |
| --- | --- | --- |
| `start` | `command`, `cwd`, `pty`, `loginShell`, `input`, `eof`, `killAfterMs`, `keepTree`, `cols`, `rows`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Rejects NUL bytes in `command` before execution, then spawns it in GNU bash (`loginShell: false` default with the session's login-environment snapshot; `loginShell: true` runs `bash -l -c`) and waits up to `waitMs` (default `30000`, range `0-240000` on direct calls; unbounded by default without `returnWhen` in nested `codemode` calls). Pipe mode defaults stdin to `/dev/null`, writes `input` (`<= 1` MiB UTF-8) and closes stdin unless `eof: false`, or keeps stdin open with `eof: false`; PTY mode rejects `input` and `eof: true` on `start` (`eof: false` is a no-op) and defaults size to `120x30`. |
| `poll` | `taskId`, `cols`, `rows`, `input`, `keys`, `keyDelayMs`, `eof`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Normalizes `keys: []` (and any accompanying `keyDelayMs`) to absent before validation, resizes the terminal (PTY only), writes `input` (`<= 16` KiB), presses `keys` (PTY only), applies `eof` (pipe only, idempotent when already closed), waits (`2000` ms with a resize, `input`, or `keys` and no `returnWhen`; `30000` ms on direct calls or with `returnWhen`; unbounded by default on nested `codemode` waits without `returnWhen` or input), and returns unseen output. |
| `kill` | `taskId`, `truncate`, `maxBytes`, `waitMs` (ignored) | Terminates the task's process tree (waits up to `5` s) and returns unseen output through the reader gate without `waitEndedBy` (`head_tail` / `16384` B default on direct calls; `head` / `4194304` B default on nested calls). On a finished `keepTree` task, ends held processes while keeping the root exit result. |
| `list` | `scope`, `state`, `limit`, `truncate`, `maxBytes` | Lists bare task IDs newest first filtered by `scope` (`"session"` default or `"all"`), `state` (`"active"` or `"finished"`; omitted lists both), and `limit` (`1-500`, default `20`), reporting `totalCount` before `limit`; creates no readers and marks only delivered entries. |
| `transcript` | `taskId`, `truncate` (default `"tail"`), `maxBytes`, `waitMs` (ignored) | Reads a UTF-8 PTY task's terminal transcript from the first unseen final line, followed by pending lines. Does not wait or advance the `poll` cursor. |

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
- **`waitMs` vs. `killAfterMs`**: `waitMs` (`0-240000`) bounds only the current
  call's wait. A call that hits `waitMs`, matches `returnWhen`, or is cancelled
  leaves the task running (use `list` to recover the `taskId` of a cancelled
  `start`). `killAfterMs` on `start` arms an independent timer that kills the
  owned tree, including kept processes. Expiry while the root runs reports
  `state: "killed"` and `lifetimeExpired: true`; after root exit, held processes
  end and the root's original result stays.
- **`returnWhen`**: Ends the wait early when any condition holds (`waitEndedBy`
  reports `{ condition, text? }`):
  - `outputContains`: `1-16` literal case-sensitive strings (`1-4096` bytes
    each, no line breaks) matched against unseen output lines: cleaned for bash
    (including text before a carriage return overwrites it), unchanged for codemode.
  - `screenContains`, `screenAppears`, `screenGone` (UTF-8 PTY only): match
    against the live screen at the task's size (`screenAppears` and `screenGone`
    compare against the screen captured before the call's resize and `input`).
  - `quietMs`: `10-60000` ms without output after `input` and `keys` finish.
- **Window and encoding options**:
  - `maxBytes`: `100-4194304` (`4` MiB; default `16384` on direct `exec` and
    outer `codemode`, `4194304` on nested `tools.exec`). Bounds the output body
    (`structuredContent.output`) only; the status line, background footer (`<= 512`
    bytes), metadata, JSON framing, errors, and correctness warnings stay outside
    that budget.
  - `truncate`: `"head_tail"` (default for direct `start`/`poll`/`kill`/`list`
    and outer `codemode`), `"head"` (default for nested `start`/`poll`/`kill`/`list`),
    or `"tail"` (default for `transcript`, direct or nested). Placement and
    `maxBytes` default independently. Direct bash calls clean ANSI/CR rewrites
    unless `raw: true` and drop NUL (`0x00`) bytes; nested bash clean output
    preserves NUL bytes so `find -print0` and `rg -0` streams parse cleanly.
    There is no fixed per-line length cap: across all windows (bash or codemode,
    direct or nested, clean or `raw`), fitting output returns whole; a single
    overflowing line cuts its UTF-8 prefix so `<prefix> … [line cut at <B>; <n> more bytes]`
    fits `B` and increments `cutLines`; and multi-line `head_tail` overflow
    reserves the inline `... [<k> lines omitted: log lines <a>-<b>] ...` marker,
    reserves `B / 10` for the head, fills the tail from the last line backward
    (cutting the last line if needed), and returns unused tail bytes to the head.
    `head` and `tail` use the full body budget for complete lines, cutting only
    an oversized first or last line. Omitted ranges are also reported in the
    status line, outside the body budget.
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
`returnWhen` (`outputContains`, `quietMs`). Schema-decoding failures return a tool error
without `structuredContent`; parameter validation and admission failures return
`isError: true` with `structuredContent` (`ok: false`, `action: "codemode"`)
without creating a task.

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

The script globals are `tools.exec/read/write/edit`, `sleep`, `sh.quote`, `text`,
`console`, `attach`, `exit`, and UTF-8 `TextEncoder`/`TextDecoder` (alongside
QuickJS's built-in `atob`/`btoa`). `sleep(ms)` returns a `Promise<void>` without
spawning a process (yielding once when `ms <= 0` and throwing `TypeError` for
non-finite numbers). `sh` is frozen; `sh.quote(value)` formats a
string or finite number as one POSIX single-quoted word and throws `TypeError`
for `NaN`, infinities, NUL bytes, or other types. There are no modules, Node
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

Nested calls resolve directly to unwrapped structured objects (omitting `ok`).
Schema-invalid argument shapes and unknown tools reject with `TypeError`;
validation, file, and integrity failures reject with `ToolError` (`name:
"ToolError"`, `tool`, `result` carrying full diagnostics such as `action`,
`output`, `exitCode`, `inputError`, or `failures`), while non-zero command
`exitCode`s resolve normally. Async rejections from `tools.*` and `sleep` carry
the caller's script stack frame (`codemode.js:<line>:<col>`). `exec` returns
`{action, taskId, state, exitCode, output, ...}` (defaulting to `truncate: "head"`,
or `"tail"` for `transcript`, and `maxBytes: 4194304`, keeping NUL bytes in clean
output and waiting with no `waitMs` ceiling unless `waitMs`, `returnWhen`, or
input/resize/keys supplies one); text `read` without `offset` or `limit` returns
the whole UTF-8 file up to `4` MiB or rejects with `ToolError` when larger or
non-UTF-8, and with `offset` or `limit` reads a slice up to `4` MiB (no line cap,
marking invalid UTF-8 with `lossy: true`), always returning `{kind: "text", path,
text, startLine, endLine, shownLines, totalLines, truncated, nextOffset,
lineTooLong, lossy}`; media `read` (image, audio, PDF, video) returns a
lightweight descriptor `{kind, path, mimeType, bytes, attachable: true}` or
`{kind, path, mimeType, sourceBytes?, attachable: false, note}` without sending
base64 into QuickJS (`attach()` throws `TypeError` with `note` when `attachable`
is `false`); `write` returns `{path, bytes}`; and `edit` returns
`{path, replacements, edits}`. Each script has `8` `exec start` slots and `8`
file/transcript I/O slots (`read`, `write`, and `edit` hold their slot inside the
blocking worker until disk I/O finishes; `poll`, `kill`, `list`, and `sleep` take
no slot), within `1024` pending calls. Starts waiting for a slot are admitted in
script submission order. Additional calls past the pending limit reject with `RangeError`.
Pending tool arguments and results have a separate 256 MiB budget per script;
exceeding it rejects the call with `RangeError`. A completed result that cannot
fit rejects after the operation ran; its error identifies this case, and the
operation remains in effect. A script awaiting a promise with no pending tool
call, `sleep`, or JavaScript job fails because it has no way to settle.

Calls use the script's session and launch context (with `cwd` overridden when
the script sets `cwd`). A nested task's reader can be polled by the caller, and
IPC children retain the outer call ID. Nested calls advance their reader state.
Nested poll, kill, and transcript leave background notifications pending for
tasks started outside the script; completion observed for a script's own tasks
marks their session entries reported. A nested list marks every existing session
entry delivered in its `tasks` array, regardless of which script started it.
When a script
ends, in-flight nested calls are cancelled and drained before publishing the
final script report, so any `exec start` already inside process creation is
recorded in `startedTasks`. Each script accepts at most 4096 validated exec
start calls, counting calls that later fail or are cancelled. Further starts
reject with RangeError, so the complete startedTasks history stays bounded.

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
remain independent tasks. `codemode.cancelledCalls` identifies calls cancelled
on termination, and `codemode.startedTasks` lists all registered handles with
states queried at each response. Status lines show up to three still-running
handles and report when a kill has waited five seconds without completion.

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
`index` and retained path. When `FASTEXEC_STRUCTURED_CONTENT=true`,
`codemode.attachmentsDelivered` lists `[{index, kind, mimeType, bytes}]` in
`content[]` block order; when `false`, the text summary appends `Attached:
#<index> (<mime>, <bytes> B)`. Nested `exec poll` and `kill` return unforgeable
script-scoped handles in `result.codemode.attachments`, holding short-term read
leases across source-task eviction until the calling script exits or cancels. If
a transport write is interrupted, attaching a fresh reader via the bare `taskId`
re-reads retained output and attachments from index `1` for deduplication by
`index`.

### File Tools

- **`read`**: Direct calls show up to `2000` lines and `50` KiB of a text file
  (nested `codemode` calls without `offset` or `limit` return the whole UTF-8
  file up to `4` MiB or reject with `ToolError` when larger or non-UTF-8, and
  with `offset` or `limit` read up to `4` MiB with no line cap). `offset` is a
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
  `FASTEXEC_MAX_MEDIA_BYTES` or rejected during opt-in compression return
  `attached: false` with `note` (and `sourceBytes` when exact) without `bytes`,
  while genuine I/O failures return `isError: true`. Media reads ignore valid
  `offset` and `limit` values. A non-regular file, and one
  that is not a recognized media format and has a NUL byte in its first `8` KiB, are
  refused (binary files suggest `exec`, and also `transfer` when
  `FASTEXEC_TRANSFER=true` for that session). Slices report invalid UTF-8 with
  replacements and a note. A nested whole-file read requires valid UTF-8
  throughout.
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
- **`edit`**: Each `edits[]` entry is `{oldText, newText, startLine?, endLine?}`,
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
  counts any remainder), and a batch that matches uniquely without overlap but
  leaves the file unchanged fails with `failures: []`. Instead of stopping at
  the first bad entry, `edit` evaluates every entry and reports `{ ok: false,
  error, path, failures }`, counting line numbers on real `\n` breaks so they
  match `read`. The file must be UTF-8; a byte-order mark
  and every unmatched byte are kept, including mixed CRLF, LF, and CR line
  endings, and inserted text follows the file's first line ending style. The
  success result names each replaced block's first line in the original file,
  so earlier insertions or deletions do not shift later reported lines, and marks
  fuzzy matches. Saving uses the same staged replacement and file
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
without blocking `daemon status` or `daemon stop`. The IPC protocol version is
unchanged.

### `FASTEXEC_TRANSFER` and `FASTEXEC_TRANSFER_ADDR`

`FASTEXEC_TRANSFER` defaults to `"false"` across local stdio, daemon endpoints,
and Streamable HTTP, leaving `transfer` unregistered. `"true"` registers
`transfer` and enables binary-file `read` hints naming `transfer`. Invalid
values fail stdio startup, `daemon start`, and `daemon run` with exit `1`
without blocking `daemon status` or `daemon stop`.

- **Local stdio (`FASTEXEC_DAEMON=off`)**: When `FASTEXEC_TRANSFER=false`,
  `FASTEXEC_PUBLIC_URL` and `FASTEXEC_TRANSFER_ADDR` are unread and no port is
  bound. When `true`, startup validates both (refusing an unspecified
  `0.0.0.0` or `[::]` listener address when `FASTEXEC_PUBLIC_URL` is unset),
  binds a `/t/` listener on `FASTEXEC_TRANSFER_ADDR` (default `127.0.0.1:0`),
  and mints `{base}t/{id}` from `FASTEXEC_PUBLIC_URL` or `http://<bound>/`.
- **Daemon stdio (`FASTEXEC_DAEMON=on`)**: Each endpoint advertises `transfer`
  from its own `FASTEXEC_TRANSFER` setting (validating `FASTEXEC_PUBLIC_URL` and
  `FASTEXEC_TRANSFER_ADDR` at startup when `true`) and forwards `"transfer"`
  over the unchanged IPC protocol. A non-HTTP daemon started with
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
- **Streamable HTTP (`--http <addr>`)**: Uses the daemon's startup
  `FASTEXEC_TRANSFER` setting to decide whether HTTP sessions register
  `transfer`. The `/t/` route stays mounted on the HTTP listener backed by the
  daemon's shared `Transfers` table, returning `404` when no capability exists
  and serving capabilities minted by `FASTEXEC_TRANSFER=true` stdio endpoints on
  the same daemon. Non-loopback `FASTEXEC_TRANSFER_ADDR` values require
  `FASTEXEC_HTTP_ALLOW_REMOTE=1`, and setting `FASTEXEC_TRANSFER_ADDR` alongside
  `--http` fails startup with exit `1`.

### `FASTEXEC_IMAGE_COMPRESS` and `FASTEXEC_MAX_MEDIA_BYTES`

Both settings validate at MCP serving startup (and per endpoint session on a
shared daemon, where HTTP uses the daemon's environment and each stdio endpoint
sends its own values in `Hello`; `daemon status` and `daemon stop` ignore them).

- **`FASTEXEC_IMAGE_COMPRESS`**: `"false"` (default) sends recognized images,
  audio, PDF, and video verbatim. `"true"` decodes PNG, JPEG, GIF, WebP, and
  BMP (excluding APNG and JPEG-LS), scales dimensions over `2000` pixels down
  to at most `2000` on the long side, and searches PNG or JPEG quality until
  the encoded payload fits `4,718,592` base64 bytes (`3.5` MiB raw), while
  rejecting images past `32768` pixels per side or `64` megapixels and leaving
  APNG, JPEG-LS, TIFF, HEIC/HEIF, audio, PDF, and video verbatim.
- **`FASTEXEC_MAX_MEDIA_BYTES`**: Maximum raw byte size per media file or
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
member names before copying them. Their rendering workspace covers 32 times the
source text plus fixed row/member keys, including simultaneous snapshot, warning,
JSON and growing text buffers. Short lists use the independent control pool;
larger candidates can move to the main pool after acquiring its replacement charge.
Native member queries admit names and conversion buffers before allocation, then
release their temporary scan allowance before rendering the selected names. Repeated
queries reuse a live-byte high-water allowance. On macOS, a budget refusal drains
the current native reply so subsequent queries remain aligned.

A list candidate checks the final frame limit and reserves its payload and
application-owned wire before marking entries reported. HTTP reserves its payload
until handler return; the SDK creates its own encoding afterward. Cancellation or refusal leaves those marks
unchanged. Direct calls still mark only text rows that fit; nested calls mark every
selected structured entry. The final result owns its payload, and daemon callers
reserve only their IPC encoding while checking the eventual MCP envelope ceiling.

Text windows reserve their working peak before entering a blocking reader. Retained
line prefixes release unused capacity after truncation and terminal padding removal;
legacy encoding boundary checks stream through fixed buffers. The text workspace
travels with blocking read and attachment workers, including after cancellation.
The completed result retains a smaller owner for its text and structured strings.

File operations first reserve a shared path workspace before resolving, displaying,
opening, or sniffing a path. The allowance is 32 times the borrowed request, cwd,
and selected home source bytes, plus 4 KiB for fixed notices; reads also reserve
the sniff buffer. Native and display conversions, path copies, and error/result
formatting share this owner. Media configuration diagnostics reserve their source
text before formatting. Missing paths, binary files, media results, write/edit
results, and transfer inspection retain the owner through their returned result.
Media and path failures do not require the text decoder allowance.

The native path bounds below were checked against Rust 1.92 and 1.100 source.
The package's `rust-version` is a minimum supported version, not a toolchain lock;
CI selects stable Rust and used 1.100 during this review.

Native target resolution has a separate budget because a short symlink can name
a much longer target. Unix `canonical_target` supplies `realpath` with a
`PATH_MAX` caller buffer, including the terminating NUL; it finds the NUL inside
that buffer and moves the same byte Vec into an OsString without a UTF-8 copy.
The checked Rust 1.92 and 1.100 implementations pass a null output buffer to
`realpath`. Our caller-buffer route returns the native
`ENAMETOOLONG` error when the resolved output cannot fit that platform limit.
On Linux the caller buffer is 4096 bytes; macOS and FreeBSD use 1024. Native path
bytes, including non-UTF-8 Unix names, are preserved; native filesystem naming
restrictions still apply. The raw-name filesystem round trip runs on Linux and
FreeBSD. The macOS CI filesystem rejects its `0xFF` filename with `EILSEQ`, so
all Unix hosts also test the native argument bytes and their lease directly,
without creating that name. The Unicode canonical-path budget case runs on all
platforms.

Windows canonicalization reopens the current name with the same access mode and
backup-semantics flag as the checked Rust 1.92 and 1.100 implementations. It queries
`GetFinalPathNameByHandleW(VOLUME_NAME_DOS)` for its actual UTF-16 size. The query
includes NUL and a successful read excludes it. Each caller buffer is admitted
before allocation; if a later query needs more space, the old buffer is dropped
before its replacement. Conversion reserves three WTF-8 bytes per UTF-16 unit
plus String growth while the wide buffer is still alive, preserving unpaired
surrogates. No approximate Windows maximum path length is used as a memory bound.

Unix final-link reads admit each caller-owned `readlink` buffer before use and
grow from 256 bytes only when the result fills it. Windows retains the relative-link
and junction semantics checked in Rust 1.92 and 1.100 `read_link`. Its native
reparse buffer is 16 KiB. Eight times that size covers the fixed reparse buffer,
the substitute-name UTF-16 Vec, WTF-8 conversion growth and the short (at most
260 UTF-16 units) prefix check. This is derived from `sys/fs/windows.rs`,
`sys/args/windows.rs` and `sys/path/windows.rs` in both checked versions. Native Win32
internal allocations remain outside this allowance. The original forty-link walk and directory requirements
remain unchanged.

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
text keeps the outer owner. Save borrows the canonical target instead of cloning
it. Edit verification still checks the current path and original file identities,
rather than canonicalizing only the original handle.

**WONT FIX: opaque native-library scratch.** glibc `realpath` maintains its own
dynamic rname/extra/link scratch even with a caller buffer; a supplied `PATH_MAX`
buffer does not bound those internal allocations. libc and WinAPI internals
remain outside the application byte pool. We neither copy their implementations
nor claim a whole-process RSS bound. Caller-owned buffers, owned targets and the
application's derived paths remain budgeted as described above.

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

Unicode bounds come from the locked `unicode-normalization 0.1.26` implementation,
not a fixed expansion assumption. `char::decompose_compatible` in
`src/normalize.rs` emits fully decomposed characters from static tables or the
Hangul calculation without building a string. The preflight counts those
characters, line breaks, the longest combining sequence and the largest expansion
of one input character. Composition cannot increase the decomposed character
count, so four UTF-8 bytes per character bounds a non-ASCII NFKC result; ASCII
retains its original byte length. The longest sequence plus one expansion bounds
the decomposition and recomposition buffers in `src/decompose.rs` and
`src/recompose.rs`. Their element sizes, geometric growth and stable-sort scratch
are reserved before running the normalizer. The fuzzy-form allowance also covers
its NFKC String, line-slice vector, joined String and character-mapped String.

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

HTTP metadata checks and GET/PUT file opens additionally reserve
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

Nested callbacks borrow QuickJS-owned UTF-8 strings. Before parsing tool arguments,
the Rust bridge reserves four times their UTF-8 length, including the tool name,
for decoded values, escape scratch, typed requests, diagnostics and worker copies.
Short control arguments may use the control pool while being classified; ordinary
calls, including sleep, must move to the main pool before dispatch. Only nested
list/kill replies may also use that bounded reserve for their encoded wire String;
the queue keeps the wire owner until the VM consumes or drops the reply. Workers and pending-call metadata
keep the input owner through cancellation and final report construction. This
does not change the separate QuickJS heap limit.

Nested exec candidates reserve their actual payload capacity `P` plus
`W = json_len(CallToolResult) + 256` before committing a reader cursor, attachment
watermark or reported state. Moving fields into `{ok,value}` adds at most 64 encoded
bytes, including the fixed missing-content fallback; 128 bytes cover new envelope
keys and fallback text. The 256-byte allowance covers both that temporary workspace
and the final encoding without a second admission after commit. The script report
atomically replaces this pending call's argument/result quota with `W`; refusal
preserves its previous charge. A failed memory growth likewise keeps the old lease
until a replacement is acquired. Only short list/kill replies can use control space.
Temporary opaque attachment handles are removed on every uncommitted exit.

The result carries its payload owner separately from the prepaid wire lease. Host
exec hands that lease to the bridge encoder; it stays intact until encoding finishes
and the old envelope is dropped, then shrinks to the String's actual capacity. The
report's quota shrinks from `W` to the final byte length. Nested file results retain
their own payload lease and reserve a separate encoding. Wake queue items keep the
wire owner through synchronous VM consumption or queue disposal.

A refusal before commit leaves reader, attachment and reported state available for
a later poll. Already-executed start/input operations are not rolled back or replayed.
When the detailed error fits, it includes the task handle, log path and recovery
instructions. If the remaining script quota cannot hold even that error, the
existing fixed `RangeError` explains that the operation was not undone and must not
be repeated. Accepted starts are recorded before waiting; their handles remain in
the outer codemode report even in this small-quota case. This does not reserve an
extra unbudgeted error frame.
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
Media packing moves Base64 blocks between candidates instead of cloning them.
Payload and wire owners are split only after their corresponding buffers are gone.

Fastexec uses the official registry `rmcp = 3.8.0` with the checked-in lockfile.
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

The custom stdio input accumulator reserves old and replacement capacity together.
Raw bytes, decoded JSON strings and escape scratch are admitted before parsing;
four times the input length remains for model strings, metadata and application
parameter conversion. Official public Request/Notification Extensions carry the
application owner to handlers and their workers. Recent cancellation IDs retain
only their transferred ID allowance. Short inputs up to 64 KiB may use the control
reserve for classification. Ordinary calls and initialize metadata must move to
the main pool before dispatch. Short list/kill, ping, discovery and cancellation
or lifecycle control messages can remain in the reserve. A refused stdio admission
sends a short overload error and closes that transport.

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

**WONT FIX in this repository: official SDK internal ownership.** Public rmcp APIs
do not provide complete ownership hooks for all of the following paths:

- HTTP body collection, JSON parsing, header decoding and validation scratch.
  Fastexec retains its existing authentication, route and body-limit checks; the
  SDK keeps its original HTTP validation order and error behavior.
- Independently cloned request parameters, initialization peer information,
  response models, handler cancellation and SDK queues after application handoff.
- HTTP JSON/SSE encoding, response/replay caches, keepalive frames and returned
  body slices, including errors and progress notifications.

The former whole-HTTP budget guarantee, response-owner propagation, prepaid
correlated SSE fallback and SDK clone/replay accounting are withdrawn. Fixing
those SDK lifetimes requires upstream support. Fastexec does not replace or copy
SDK internals into application code, and these exclusions do not exempt the file,
media, IPC, terminal or nested buffers that the application actually owns. The
process pool therefore limits those tracked active payload buffers, not total
process memory or every allocation made by the SDK.

The HTTP integration case
`slow_sse_client_holds_active_memory_lease_until_response_completes_or_drops`
remains as an ignored upstream-dependent check. Its assertion requires the SDK's
slow response body to retain a lease after the handler has returned its bare
result. CI lists this case as ignored, not passed. Ordinary HTTP media delivery,
application admission, cancellation and control checks remain enabled.

IPC request senders count JSON without a temporary encoding, reserve the measured
frame capacity, and encode directly into that allocation. The frame owns its
reservation while waiting for the writer and during partial writes. A Hello also
reserves its temporary environment and identity copies before constructing them.
Daemon request readers reserve six times the declared body length: the raw body,
a parsed Value, and four times the body length for borrowed-Value decoding and
subsequent argument copies. The earlier escape-decoding scratch fits within that
peak. After parsing, the raw body and Value are dropped before retaining the
four-times allowance. This covers payload strings and native OS-string units,
not JSON object metadata, persistent launch environments, task state or VM heap.
Short Hello, Cancel, Bye, Status, Stop and list/kill requests may use the control
reserve; ordinary calls must move to the main pool before dispatch. A refused
call returns a short correlated receipt without running the operation. Handshake
and CLI response readers use the same budgeted endpoint parser.

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
`daemon stop`, leave the variable unread. The IPC protocol version is unchanged.

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
- **`codemode` report**: Included when any array is non-empty
  (`attachmentsDelivered`, nested `attachments`, `cancelledCalls`, `startedTasks`).
- **`read`**: Keeps its data fields unchanged (`endLine: null`,
  `nextOffset: null`, `lineTooLong`, `lossy`, and `attached: false` are data;
  nested calls strip `ok` and return `{attachable, ...}` media descriptors without base64).
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
  `inputError`; the writer joins within the `2` s post-tree output drain before
  final state publication. Output capture or log write failures also set
  `ok: false` and `isError: true` while preserving `exitCode` and captured
  output. Warnings stay whole regardless of `maxBytes`.
- **Background footer**: Appended to `start`, `poll`, `kill`, `transcript`, and
  `codemode` results (up to `512` bytes, outside `maxBytes`) naming up to three
  other tasks this session has addressed, using bare task IDs. HTTP clients
  sharing a token share this scope. Running tasks (or tasks holding processes)
  outside this session appear **at most once per session** as `1 task running
  outside this session; use list with scope "all".` or `<n> tasks running
  outside this session; use list with scope "all".`; the session flag is
  consumed atomically when a delivered response shows that clause or when the
  session's first committed footer-bearing result finds no outside tasks (never
  on nested or cancelled calls). A footer marks only session tasks it names; `list`
  marks existing session entries shown in direct text or delivered in a nested
  `tasks` array.

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
  GiB total (evicting oldest finished task files first and refunding quota only
  when file deletion succeeds or reports `NotFound`). After a partial write,
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
- **Live PTY emulator and continuous transcript spool**: Each UTF-8 PTY task
  feeds output bytes into an `alacritty_terminal` (`0.26.0`) instance at the
  task's size (`src/terminal.rs`, `src/frames.rs`) that answers terminal queries
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
  arrows, `Home`, `End`, `PageUp`/`PageDown`, `Insert`, `Delete`, `F1` to `F12`,
  keypad keys, `[NUL]` to `[US]`, `0xHH`, or single characters) with `C-`, `M-`,
  `S-`, and `^` modifiers. Keys encode according to the program's active
  keyboard mode: standard xterm/tmux sequences (respecting DECCKM cursor and
  DECKPAM keypad modes) or the kitty keyboard protocol when enabled. Initial
  validation rejects unknown or unencodable keys before input delivery; a later
  keyboard-mode change can fail a key after earlier input has been sent.
- **Key pacing**: `keyDelayMs` (`0-2000`, default `150`; requested pauses per
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

- **Launch (`src/bash.rs`, `src/process.rs`, `src/session.rs`)**: Runs
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
    host alive and are reported instead of counting as an empty tree. The
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
  reported instead. A timeout in the writer queue leaves the connection usable
  and identifies the frame that never went out. Interrupting a frame write
  closes the connection. A call whose connection fails after sending began returns an
  error naming the call, request ID, session, and daemon epoch: the action for
  `exec`, and the tool for `codemode`, `read`, `write`, `edit`, and `transfer`.
  Failed calls are not replayed.
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
  call names its tool (`exec`, `codemode`, `read`, `write`, `edit`, `transfer`).
  The endpoint refuses a
  call whose serialized arguments pass `3` MiB before writing it (sizing for up
  to `1` MiB of JSON-escaped `start` `input`), a request frame
  is at most `4` MiB (a `Hello` carries the endpoint's environment), a response
  frame at most `512` MiB (protocol version `3`), one connection carries at most `32` concurrent calls,
  and the daemon serves at most `128` connections.
- **Sessions**: Each endpoint process holds one random session ID with its own
  task entries and readers, and a reconnect keeps them. An endpoint session with
  no live connection is dropped after `10` minutes, and the table holds at most
  `256` endpoint sessions. The endpoint says `Bye` when its stdin closes, which
  drops that session at once. Sessionless HTTP has one principal session per
  configured token for the listener's entire lifetime; these sessions do not
  expire. A session's entries retire with their tasks.
- **Lifetimes**: The daemon does not exit idle by default. Starting it with
  `--idle-exit on` enables exit after `10` minutes with no connections, an empty
  task registry (finished tasks stay for up to `24` hours), and no live or
  in-flight `/t/` transfers. `--idle-exit off` explicitly disables it. HTTP
  daemons never exit idle, even with `--idle-exit on`.
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
  the daemon's own environment. The listener retains up to `256` legacy HTTP
  sessions; at capacity it rejects new legacy initializations. Idle legacy
  sessions expire after `60` minutes, and DELETE also releases the slot. The
  cap is a count, not a memory or namespace-wide quota. Principal sessions
  do not use this cap and last until the listener stops.
  The HTTP listener admits at most `128` concurrent connections and allows
  `10` seconds to receive request headers. Extra connections are closed without
  queueing; management through the local daemon socket has its own capacity.
  This header deadline does not impose a total timeout on uploads, SSE, or
  long tool calls. Invalid HTTP syntax and ordinary client disconnects do not
  append one error-log entry per request. Streamed responses carry
  `X-Accel-Buffering: no`, which tells a proxy not to buffer them.
- **Transfer route**: `/t/<capability id>` serves one file without the bearer
  token, which makes the URL usable by `curl`. It runs on `--http` (sharing the
  daemon's table with any `FASTEXEC_TRANSFER=true` stdio endpoints) or on the
  standalone `/t/` listener in local stdio and non-HTTP daemon modes when
  `FASTEXEC_TRANSFER=true`, returning `404` on any non-`/t/` path there.
  An id expires after `10` minutes unused, and an unknown or expired one answers
  `404`. `GET` returns the file, honouring one `bytes=` range with `206` and
  `416` and ignoring any other form, so the whole file comes back with `200`.
  `HEAD` answers the current size. `PUT` writes the body at `?offset=` (default
  `0`), truncating the file to that offset before reading the body, refusing an
  offset past the end with `409`, and answering the new size on success. A
  truncated or malformed body returns `400`. An upload that waits `30` seconds
  for more body data returns `408`. This inactivity deadline applies while
  waiting for the network, not to disk writes or the total upload time. In
  either case, the accepted prefix is retained so `HEAD` and a later upload
  can resume from its actual size. Uploads to the
  same open file identity serialize even across distinct capability URLs,
  symlinks, and hard links; uploads to different files remain independent. A
  disconnected upload waiting for that lock releases its open file and waiter.
  Any other method answers `405` with `Allow`.
  Replies carry `Cache-Control: no-store`, `Content-Type:
  application/octet-stream`, and `X-Accel-Buffering: no`. `/t/` is answered
  before the bearer check and before the `4` MiB body cap that applies to
  `/mcp`. `/mcp` is the only other route: a path behind the bearer check that
  is not `/mcp` answers `404`, so a proxy that forwards its prefix unstripped
  fails at `initialize` instead of waiting until a later download.
