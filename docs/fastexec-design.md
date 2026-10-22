# fastexec Reference and Architecture

Authoritative runtime usage rules live in
[`src/description.md`](../src/description.md) and request validation in
[`src/main.rs`](../src/main.rs).

## 1. Tool Actions and Parameter Limits

The `fastexec` tool accepts a single JSON object (`deny_unknown_fields`,
`camelCase`). Parameters that do not apply to the chosen `action` or fall
outside declared ranges return an error before execution.

| Action | Allowed Parameters | Summary |
| --- | --- | --- |
| `start` | `command`, `cwd`, `pty`, `loginShell`, `killAfterMs`, `keepTree`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Spawns `command` in GNU bash and waits up to `waitMs` (default `30000`, range `0–240000`). |
| `poll` | `taskId`, `input`, `keys`, `keyDelayMs`, `eof`, `waitMs`, `returnWhen`, `truncate`, `maxBytes`, `raw`, `encoding`, `screen` | Writes `input` (`<= 16` KiB), presses `keys` (PTY only), applies `eof` (pipe only), waits (`2000` ms with `input`/`keys` and no `returnWhen`, `30000` ms otherwise), and returns unseen output. |
| `kill` | `taskId`, `maxBytes`, `waitMs` (ignored) | Terminates the task's process tree (waits up to `5` s) and returns its final state. On a finished `keepTree` task, ends held processes while keeping the root exit result. |
| `list` | *(none)* | Lists all tasks in this server (newest first) with their state and held processes. |
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
    against the live 120×30 screen (`screenAppears` and `screenGone` compare
    against the screen captured before the call's `input`).
  - `quietMs`: `10–60000` ms without output after `input` and `keys` finish.
- **Window and encoding options**:
  - `maxBytes`: `100–1048576` (default `16384`). Rejected when
    `truncate: "none"`. Bounds the output window, status line, and background
    footer together; metadata and JSON framing are outside that budget.
  - `truncate`: `"head_tail"` (default for `start`/`poll`: ~10% head, ~90%
    tail), `"head"`, `"tail"` (default for `transcript`), or `"none"` (all
    unseen lines whole). Bounded modes cap each line at `2000` characters (`64`
    KiB when `raw: true`) and report `cutLines`.
  - `encoding`: Any ASCII-compatible WHATWG label (such as `"utf-8"`, `"big5"`,
    `"gbk"`; UTF-16 is rejected). On `start`, sets the task default; on `poll`,
    overrides for that call.
  - `screen`: Boolean (`false` default, UTF-8 PTY only). Returns the rendered
    120×30 screen and 1-based cursor `[row, col]`, marking the output stream
    read up to that point. Cannot combine with `truncate`, `raw`, or `encoding`.

## 2. Host Configuration and Output Modes

### Progress Notifications and Timeouts

When a request carries a `progressToken`, the server emits progress every
`20` seconds during waits. Clients that refresh their timeout on progress,
including Pi's built-in MCP client, can keep long waits alive.

Other hosts need a tool timeout of at least `300` seconds. Keep the MCP server
connected for the task's lifetime: an idle disconnect ends its tasks.

### `FASTEXEC_STRUCTURED_CONTENT` and `FASTEXEC_OUTPUT_MODE`

Set these strings in the MCP server's `env` configuration. Both are read at
startup; invalid values exit with status `1`:

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

A nonzero command `exitCode` is a normal result (`ok: true`). Operational
failures set `isError` and `ok: false`; schema-decoding errors may have no
`structuredContent`.

### `structuredContent` Fields

- **`start`, `poll`, `kill`**: `ok`, `action`, `taskId`, `state` (`"running"`,
  `"exited"`, `"killed"`), `exitCode`, `pty`, `elapsedMs`, `logPath`,
  `lifetimeExpired`, `logError`, `keepTree`, `leftovers` (`[{ pid, name }]`),
  `keptRunning`, `held` (`[{ pid, name }]`), `killError`.
- **`start`, `poll` (additional)**: `waitEndedBy` (`{ condition, text? }`). With
  `screen: true`, adds `cursor` (`[row, col]`) and `omittedRows`. Otherwise adds
  `omittedLines`, `omittedRange` (`[first, last]` or `null`), `cutLines`, and
  `encodingErrors`.
- **`transcript` (additional)**: `transcriptPath`, `transcriptLines`,
  `finalLines`, `firstLine`, `omittedLines`, `omittedRange`, `cutLines`, and
  `encodingErrors`.
- **`list`**: `ok`, `action`, and `tasks` (`taskId`, `state`, `exitCode`, `pty`,
  `elapsedMs`, `command` up to 120 chars, `logPath`, `lifetimeExpired`,
  `keepTree`, `keptRunning`, `held`).
- **Background footer**: Appended to `start`, `poll`, `kill`, and `transcript`
  results (up to `512` bytes and `maxBytes / 4`) naming up to three other tasks
  that finished unseen, had held processes end, or are still running/holding
  processes.

## 3. Logs, Terminal Emulator, and Transcript Spool

- **Logs and retention**: Each task logs raw output to
  `<temp>/fastexec-<uid>-<pid>/<taskId>.log` on Unix (`0700` dir, `0600` file)
  or `<temp>\fastexec-<pid>\<taskId>.log` on Windows. Each task log caps at `64`
  MiB, and all logs on a server cap at `1` GiB total (evicting oldest finished
  task logs first). Past a cap or write error, capture keeps draining output
  and the status line reports dropped bytes. The
  server allows at most `16` running tasks (including `keepTree` tasks still
  holding processes) and retains the `64` most recently finished tasks. Log
  directories are removed on server shutdown and cleaned up on startup when a
  prior server PID is gone.
- **Live PTY emulator and continuous transcript spool**: Each UTF-8 PTY task
  feeds output bytes into a 120×30 `alacritty_terminal` (`0.26.0`) instance
  (`src/terminal.rs`, `src/frames.rs`) that answers terminal queries (`ESC[6n`,
  `ESC[c`), tracks keyboard modes, serves `screen: true`, and continuously
  spools `<taskId>.transcript.txt` (up to `64` MiB, counted under the `1` GiB
  total limit). Lines scrolled off the normal screen and merged frames from
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
    `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE` (breakaway disabled).
  - **Linux**: A separate task host leads the session and adopts daemonized
    descendants as a subreaper. Tree termination sends `SIGKILL` to discovered
    descendants and the host's group. A lifeline pipe triggers cleanup if the
    server dies. Other Unix builds lack Linux's descendant tracking and cannot
    provide the same `keepTree` guarantees.
  - **`keepTree: true`**: Keeps leftover processes running after root bash exits
    until `kill`, `killAfterMs`, or session end. Once the root exit has been
    reported, subsequent `poll` calls wait on the held tree
    (`waitEndedBy.condition: "tree_ended"`), and a `start`/`poll` with
    `returnWhen` waits past a root exit `0` for output from held processes.
  - **Limits**: Native permission failures appear in `killError`; unkillable
    processes remain held. Processes launched outside the owned tree, such as
    through a system service manager, are outside its cleanup scope.
