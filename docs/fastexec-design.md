# fastexec Reference and Architecture

Authoritative runtime usage rules live in
[`src/description.md`](../src/description.md) and request validation in
[`src/service.rs`](../src/service.rs).

## 1. Tool Actions and Parameter Limits

Each tool accepts a single JSON object (`deny_unknown_fields`, `camelCase`).
Parameters that do not apply to the chosen `action` or fall outside declared
ranges return an error before execution.

| Tool | Parameters | Summary |
| --- | --- | --- |
| `exec` | the action table below | Runs bash commands as tasks. |
| `read` | `path`, `offset`, `limit` | Shows a text file, or an image inline. |
| `write` | `path`, `content` | Writes a file in place. |
| `edit` | `path`, `edits` | Applies exact text replacements to a UTF-8 file. |
| `transfer` | `path` | Publishes one file at a URL, on an HTTP session. |

### `exec` Actions

| Action | Allowed Parameters | Summary |
| --- | --- | --- |
| `start` | `command`, `cwd`, `pty`, `loginShell`, `killAfterMs`, `keepTree`, `cols`, `rows`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Spawns `command` in GNU bash and waits up to `waitMs` (default `30000`, range `0–240000`). `cols`/`rows` set a PTY terminal's size (default `120×30`). |
| `poll` | `taskId`, `cols`, `rows`, `input`, `keys`, `keyDelayMs`, `eof`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Resizes the terminal (PTY only), writes `input` (`<= 16` KiB), presses `keys` (PTY only), applies `eof` (pipe only), waits (`2000` ms with a resize, `input`, or `keys` and no `returnWhen`, `30000` ms otherwise), and returns unseen output. |
| `kill` | `taskId`, `maxBytes`, `waitMs` (ignored) | Terminates the task's process tree (waits up to `5` s) and returns its final state. On a finished `keepTree` task, ends held processes while keeping the root exit result. |
| `list` | `truncate`, `maxBytes` | Lists tasks in this server (the namespace's daemon in `on` mode, newest first) with their state, owner, session, call ID, and held processes; text fits `maxBytes` while `structuredContent.tasks` stays complete. |
| `transcript` | `taskId`, `truncate` (default `"tail"`), `maxBytes` | Reads a UTF-8 PTY task's terminal transcript from the first unseen final line, followed by pending lines. Does not wait or advance the `poll` cursor. |

### Key Parameter Rules

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
    footer together; metadata and JSON framing are outside that budget.
  - `truncate`: `"head_tail"` (default for `start`/`poll`: ~10% head, ~90%
    tail), `"head"`, `"tail"` (default for `transcript`), or `"none"` (all
    unseen lines whole up to `4` MiB, with the rest reported as an omitted
    range, so one line over that cap is cut). Bounded modes cap each line at
    `2000` characters (`64` KiB when `raw: true`) and report `cutLines`.
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

### File Tools

- **`read`**: Shows up to `2000` lines and `50` KiB of a text file, with
  `offset` (1-indexed line) and `limit` (lines) selecting a window. A
  recognized image (JPEG, PNG, GIF, WebP) comes back inline, up to
  `2000`×`2000` pixels and `4718592` bytes of base64; a BMP is re-encoded as
  PNG, or as JPEG when the resize needs it, and a file over `512` MiB, or an
  image whose conversion or resize cannot fit, returns the note as text instead
  of an image. Images ignore `offset` and `limit`. A non-regular file, and one
  that is not a recognized image and has a NUL byte in its first `8` KiB, are
  refused; any other file decodes as text, lossily, with a note naming the
  replacements.
- **`write`**: Writes `content` in place, creating missing parent directories,
  and keeps the permissions of a file that exists.
- **`edit`**: Each `edits[]` entry is `{oldText, newText}`, matched against the
  original file, not against the result of an earlier entry. Every `oldText`
  must be unique in that file, counted over every start position including
  overlapping ones, and must not overlap another. Exact duplicates are rejected
  before fuzzy normalization can hide them; near copies are also ambiguous.
  A sole exact and a sole fuzzy match must map to the same original target,
  allowing for the whitespace fuzzy matching trims. The file must be UTF-8; a
  byte-order mark and the first line ending style are kept, only the matched
  bytes are replaced, and the result names each replaced block's first line in
  the file it produced, marking a block whose match needed normalizing.
- **Paths**: A path resolves against the session's working directory, accepts
  `~` for the home directory, or is a native absolute path (`C:/...` or
  `C:\...` on Windows; a Git Bash `/c/...` path is not one).
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
startup; invalid values exit with status `1`. They shape `exec` results:
`read`, `write`, and `edit` answer with their own text or image content, and
`transfer` adds `structuredContent.url` when structured content is enabled.

- **`FASTEXEC_STRUCTURED_CONTENT`**: `"true"` (default) attaches
  `structuredContent` JSON metadata; `"false"` omits `structuredContent` on all
  results and uses text output placement.
- **`FASTEXEC_OUTPUT_MODE`** (when structured content is enabled):
  - `"text"` (default): `content` holds the output body, status line, and
    background footer; `structuredContent` holds metadata only.
  - `"both"`: `content` holds body + status + footer, and
    `structuredContent.output` also holds the output body.
  - `"structured"`: `content` holds only the status line and footer;
    `structuredContent.output` holds the output body (for hosts like Codex that
    consume structured tool output).

### `FASTEXEC_DAEMON`

`"off"` (default) keeps tasks in this server's own session. `"on"` forwards
every tool call to the namespace's daemon (section 6). The value is read at
startup; an invalid value exits with status `1`.

A nonzero command `exitCode` is a normal result (`ok: true`). Operational
failures set `isError` and `ok: false`; schema-decoding errors may have no
`structuredContent`.

### `structuredContent` Fields

These fields describe `exec` results.

- **`start`, `poll`, `kill`**: `ok`, `action`, `taskId`, `state` (`"running"`,
  `"exited"`, `"killed"`), `exitCode`, `forcedEnd`, `pty`, `elapsedMs`,
  `logPath`, `lifetimeExpired`, `logError`, `keepTree`, `leftovers`
  (`[{ pid, name }]`), `keptRunning`, `held` (`[{ pid, name }]`), `killError`.
- **`start`, `poll` (additional)**: `waitEndedBy` (`{ condition, text? }`). With
  `screen: true`, adds `cols`, `rows`, `cursor` (`[row, col]`), and `omittedRows`. Otherwise adds
  `omittedLines`, `omittedRange` (`[first, last]` or `null`), `cutLines`, and
  `encodingErrors`.
- **`transcript` (additional)**: `transcriptPath`, `transcriptLines`,
  `finalLines`, `firstLine`, `omittedLines`, `omittedRange`, `cutLines`, and
  `encodingErrors`.
- **`list`**: `ok`, `action`, and `tasks` (`taskId`, `state`, `exitCode`, `pty`,
  `cols`, `rows`, `elapsedMs`, `command` up to 120 chars, `logPath`,
  `lifetimeExpired`, `keepTree`, `keptRunning`, `held`, `leftovers`, `killError`,
  `owner`, `session`, `callId`). `cols` and `rows` are `null` for a pipe task.
- **Background footer**: Appended to `start`, `poll`, `kill`, and `transcript`
  results (up to `512` bytes and `maxBytes / 4`) naming up to three other tasks
  that finished unseen, had held processes end, are still running, or have a
  changed (or failed-to-kill) set of held processes not yet shown.

## 3. Logs, Terminal Emulator, and Transcript Spool

- **Logs and retention**: In the default mode each task logs raw output to
  `<temp>/fastexec-<uid>-<pid>/<taskId>.log` on Unix (`0700` dir, `0600` file)
  or `<temp>\fastexec-<pid>\<taskId>.log` on Windows; `on` mode logs under
  `<runtime root>/ns/<namespace>/logs/` (section 6). Each task log caps at `64`
  MiB, and all logs on a server cap at `1` GiB total (evicting oldest finished
  task logs first). Past a cap or write error, capture keeps draining output
  and the status line reports dropped bytes. The default mode allows at most
  `16` running tasks (including `keepTree` tasks still holding processes) and
  retains the `64` most recently finished tasks without expiry. `on` mode allows
  `64` running and `256` finished tasks, drops a finished task after `24` hours,
  and keeps task logs and transcripts under
  `<runtime root>/ns/<namespace>/logs/` (section 6). The default mode removes its
  log directory on server shutdown and cleans up one left by a server whose PID
  is gone; `on` mode empties the namespace's `logs/` directory at daemon start
  while it holds that namespace's `daemon.lock`.
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
  Zero sends input and keys together.
- **Bundled ConPTY (`src/conpty.rs`)**: Windows x64 builds embed Microsoft
  ConPTY `1.24.260710001` (`conpty.dll` and `OpenConsole.exe`, MIT), extract and
  pin them read-only in `%TEMP%\fastexec-conpty-1.24.260710001`, and load
  `conpty.dll` before opening the first PTY so terminal output passes through
  without legacy Windows console re-rendering. If loading fails, system ConPTY
  is used and transcript results warn that lines may be missing.

## 5. Bash Launch and Process-Tree Ownership

- **Launch (`src/bash.rs`, `src/process.rs`)**: Runs `bash -l -c`
  (`loginShell: true`, default) or `bash --noprofile --norc -c`
  (`loginShell: false`). Commands over `8000` bytes run from a temporary script
  file in the log directory. Both modes set `LANG`/`LC_ALL` (`C.UTF-8`, or
  `en_US.UTF-8` on macOS), `PAGER=cat`, `GIT_PAGER=cat`, `PYTHONUNBUFFERED=1`,
  and `PYTHONIOENCODING=utf-8`; pipe mode also sets `TERM=dumb`, `NO_COLOR=1`,
  `CLICOLOR=0`, `FORCE_COLOR=0`, `EDITOR=true`, `VISUAL=true`,
  `GIT_EDITOR=true`, and `GIT_TERMINAL_PROMPT=0`, whereas PTY mode sets
  `TERM=xterm-256color`.
- **Tree ownership (`src/process.rs`, `src/host.rs`)**:
  - **Windows**: Runs each task inside a Job Object with
    `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` and breakaway disabled, because Git
    Bash starts `&` children with breakaway whenever a Job allows it.
  - **Linux**: A separate task host leads the session and adopts daemonized
    descendants as a subreaper. Tree termination sends `SIGKILL` to discovered
    descendants and the host's group. A lifeline pipe triggers cleanup if the
    server dies.
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
  call whose serialized arguments pass `1` MiB before writing it, a request frame
  is at most `4` MiB (a `Hello` carries the endpoint's environment), a response
  frame at most `64` MiB, one connection carries at most `32` concurrent calls,
  and the daemon serves at most `128` connections.
- **Sessions**: Each endpoint process holds one random session ID with its own
  cursors, and a reconnect keeps them. A session with no live connection is
  dropped after `10` minutes, and the table holds at most `256` sessions. The
  endpoint says `Bye` when its stdin closes, which drops the session at once.
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
- **CLI**: `fastexec daemon start [--http <addr>] [--idle-exit on|off]` starts the daemon detached
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
- **HTTP**: `--http <addr>` with `FASTEXEC_HTTP_TOKEN`, at least `16` visible
  ASCII characters, serves the same toolset over Streamable HTTP. The address is
  loopback unless `FASTEXEC_HTTP_ALLOW_REMOTE=1`, which belongs behind a
  TLS-terminating proxy. Every `/mcp` request carries the bearer token, checked
  before a session is created or resumed, and the token is dropped from the
  environment HTTP sessions launch tasks with; a `/mcp` body over `4` MiB is
  refused; `Origin` is
  limited to the bound address's own origin, the loopback names, and
  `FASTEXEC_HTTP_ALLOWED_ORIGINS`, where an entry without a port matches any
  port. This is request Origin validation, not CORS support: browser cross-origin
  preflights and CORS response headers are not provided. HTTP sessions run with
  the daemon's own environment, and the `10` minute
  inactivity TTL expires idle ones without bounding their count or memory.
  Streamed responses carry `X-Accel-Buffering: no`, which a proxy reads as the
  instruction not to buffer them.
- **Transfer route**: `/t/<capability id>` serves one file without the bearer
  token, which is what makes a URL usable by `curl`; the `transfer` tool mints
  the id and builds the URL from `FASTEXEC_PUBLIC_URL` when the daemon was
  started with it, and from the request's own `Host` and scheme when it was not.
  An id expires after `10` minutes unused, and an unknown or expired one answers
  `404`. `GET` returns the file, honouring one `bytes=` range with `206` and
  `416` and ignoring any other form, so the whole file comes back with `200`.
  `HEAD` answers the current size. `PUT` writes the body at `?offset=` (default
  `0`), truncating the file to that offset before reading the body, refusing an
  offset past the end with `409`, keeping the bytes of a body that disconnects,
  and answering the new size. Any other method answers `405` with `Allow`.
  Replies carry `Cache-Control: no-store`, `Content-Type:
  application/octet-stream`, and `X-Accel-Buffering: no`. `/t/` is answered
  before the bearer check and before the `4` MiB body cap that applies to
  `/mcp`. `/mcp` is the only other route: a path behind the bearer check that
  is not `/mcp` answers `404`, so a proxy that forwards its prefix unstripped
  fails at `initialize` rather than at a later download.
