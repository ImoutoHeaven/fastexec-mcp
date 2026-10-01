# fastexec Design

fastexec uses rmcp 2.2.0 and portable-pty 0.9.0, with pi 0.99.2 as its reference host. FastCtx commit [`ccaa157`](https://github.com/yc-duan/fastctx/tree/ccaa157790d02328a60786eb94ee5ad698995a5f) supplies the upstream implementations identified in §6 and §8.

## 1. Scope

fastexec is a stdio MCP server written in Rust. It ships as one binary and exposes one tool, `fastexec`, which runs bash commands:

- a command that finishes within its wait window returns like a foreground command;
- a command that outlives the window keeps running as a task, and the model returns to it with `poll`;
- running tasks accept input through a stdin pipe or, on request, a pseudo-terminal (PTY);
- `kill` terminates a task's whole process tree.

Tasks belong to the server process. The host starts the server with the session and stops it when the session ends, so every task ends with its session. The model learns about task completion by calling `poll`; the server sends nothing to the model on its own.

Release platforms are Windows x64 with Git for Windows and Linux x64 with GNU bash. Other Unix systems build from the same Unix code; the `/proc` session sweep in `kill` (§6.3) runs only on Linux. pi 0.99.2 is the reference host; any MCP host that supports tools can run fastexec.

## 2. Host Facts (pi 0.99.2)

| Fact | Source |
|---|---|
| Every tool call carries a `progressToken`. Each `notifications/progress` re-arms the request timeout (default 60 s, `timeout` in `mcp.json`) and appears in the TUI as a tool update. | `pi-mcp/dist/client.js` (`requestInternal`, `handleProgress`), `extensions/mcp/tools.js` |
| An aborted tool call sends `notifications/cancelled`. rmcp exposes it as `RequestContext::ct`. | `pi-mcp/dist/client.js` (`cancelPending`), rmcp `service.rs` |
| The stdio server starts with the session directory as its working directory and inherits pi's full environment plus `env` from `mcp.json`. | `extensions/mcp/runtime.js` (`createDefaultTransport`), `pi-mcp/dist/transports/stdio.js` |
| `session_shutdown` closes every MCP connection; this covers quit, reload, new, resume, and fork. The shutdown sequence is: close server stdin, SIGTERM after 500 ms, SIGKILL 2 s later. On Unix the signals go to the server's process group; on Windows pi runs `taskkill /T /F`. | `extensions/mcp/index.js`, `pi-mcp/dist/transports/stdio.js` |
| Model-facing text above 20 KiB (`MCP_OUTPUT_MAX_BYTES`) is cut in the middle, and the full text is saved to a temp file. Codemode scripts receive the whole `CallToolResult`, including `structuredContent`, without truncation. | `extensions/mcp/tools.js` (`limitMcpContent`, `convertMcpResult`) |
| Server logging notifications go only to `~/.pi/agent/mcp.log`. | `docs/mcp.md` |
| The default exposure is `codemode`. `"exposure": "direct"` declares the tool to the model. The model-facing name is `mcp__fastexec__fastexec`. | `docs/mcp.md` |

rmcp 2.2.0 provides what the server needs: `Meta::get_progress_token`, `Peer<RoleServer>::notify_progress`, and the per-request cancellation token.

## 3. Tool Interface

The tool takes one flat object with `deny_unknown_fields`. `action` selects the operation. A parameter that does not apply to the chosen action is rejected, and an out-of-range value returns an error without clamping.

| Parameter | Actions | Type, default, range | Meaning |
|---|---|---|---|
| `action` | all | `start` / `poll` / `kill` / `list` | Required |
| `command` | start | non-blank string | Bash command line |
| `cwd` | start | existing absolute directory; default: the server working directory | Working directory |
| `pty` | start | boolean, `false` | Run inside a pseudo-terminal |
| `loginShell` | start | boolean, `true` | `bash -lc` when true; `bash --noprofile --norc -c` when false |
| `killAfterMs` | start | nonnegative integer; 0 or omitted: no limit | Kill the whole tree this long after launch, independently of waits, polls, and request cancellation |
| `taskId` | poll, kill | string | Task returned by `start` or `list` |
| `input` | poll | string, at most 16 KiB UTF-8 | Written exactly as given, before `keys` and before waiting |
| `keys` | poll, PTY tasks | array of key names | Pressed after `input`, in array order (§3 Keys). `input` and the encoded keys together hold at most 16 KiB |
| `eof` | poll | boolean, `false` | Pipe mode: close stdin after `input`. PTY mode: returns an error |
| `waitMs` | start, poll | integer 0–240000 | Defaults: start 30000; poll with `input` or `keys` 2000; other polls 30000 |
| `truncate` | start, poll | `head_tail` (default) / `head` / `tail` / `none` | Output window mode (§5) |
| `maxBytes` | start, poll | integer 1024–1048576, default 16384 | Output window budget; rejected with `truncate: "none"` |
| `raw` | start, poll | boolean, `false` | Return output without cleaning (§5) |
| `encoding` | start, poll | ASCII-compatible WHATWG label, e.g. `gbk`, `big5` | Decode output from this encoding. A value on `start` becomes the task default; a value on `poll` applies to that call. UTF-16 labels are rejected because lines split on the LF byte |
| `screen` | start, poll; PTY tasks | boolean, `false` | Return the rendered terminal screen instead of the output window (§5). Rejected with `truncate`, `raw`, or `encoding`, on `start` without `pty: true`, and on a task started with a non-UTF-8 `encoding` |

### Action semantics

- **`start`** spawns the command and waits up to `waitMs`. If the command exits within the window, the result carries the final state and all of its output, as a foreground command would. Otherwise the result carries `state: "running"`, the task ID, and the output so far. `waitMs: 0` returns immediately.
- **`poll`** writes `input`, then the bytes of `keys`, as one write, then applies `eof`. It then waits until the task ends or `waitMs` elapses, and returns the output the caller has not seen yet. New output during the window leaves the wait running. `waitMs: 0` takes an immediate snapshot. A poll without `input` always reads, including on a finished task, where it returns the final state and any remaining unseen output. `input` after stdin has closed (by `eof`, by the program, or by exit) returns an error and delivers nothing. An invalid key name also returns an error and delivers nothing.
- **`kill`** terminates the whole process tree (§6.3), waits up to 5 s for the exit, and returns the status with `state: "killed"`; a tree still running after 5 s returns `state: "running"` with a note to poll later. Unseen output stays available to `poll`. Killing a finished task returns its existing final state. A failed native termination call returns an error result naming the cause; an already empty tree counts as terminated.
- **Lifetime limit.** A positive `killAfterMs` arms a timer owned by the task. When it fires while the task runs, the task is killed through the same tree termination and reports `state: "killed"` with `lifetimeExpired: true`. The kill and expiry flags are read when the root exits, so a root that exits before a kill keeps its natural result while its output drains.
- **`list`** returns every task of this server, newest first.

### Keys

`keys` names keys as tmux `send-keys` does (`key-string.c`), and each key sends the bytes tmux sends to a pane in its standard key mode (`input-keys.c`):

- Names, case-insensitive: `Enter`, `Tab`, `BTab`, `Escape`, `Space`, `BSpace`, `Up`, `Down`, `Left`, `Right`, `Home`, `End`, `PageUp`/`PgUp`/`PPage`, `PageDown`/`PgDn`/`NPage`, `Insert`/`IC`, `Delete`/`DC`, `F1`–`F12`, `KP0`–`KP9`, `KP/`, `KP*`, `KP-`, `KP+`, `KP.`, `KPEnter`, and `[NUL]`–`[US]` for C0 controls.
- One character (printable ASCII or any Unicode character), or `0xHH` for a code point.
- Modifier prefixes `C-`, `M-`, and `S-` combine in any order; `^c` means `C-c`.
- Arrow keys send `ESC O x` when the program has enabled application cursor mode (DECCKM), and keypad keys do so in application keypad mode (DECKPAM); the PTY task's terminal emulator (§5) tracks both modes.
- Modified function, arrow, and editing keys use the xterm form `ESC [ <n> ; <m> <final>`. `M-` before any other key sends `ESC` first; `C-` maps characters to C0 controls as a VT terminal does.

fastexec differs from tmux in two cases, both errors that deliver nothing: a name tmux would send as literal text (literal text belongs in `input`), and a modifier that the key's legacy encoding cannot carry, which tmux drops silently (`C-Enter`, `C-Tab`, `S-a`, `C-é`, `C-BSpace`, any modified `BTab`, Ctrl or Shift on a keypad key). Pipe tasks reject `keys`.

### Waiting

- An MCP cancellation ends the wait, and the process keeps running.
- If a `start` is cancelled, its task ID is lost with the response; `list` recovers it.
- While a wait is in progress and the request carries a `progressToken`, the server sends `notifications/progress` every 20 s, so a 240 s wait stays inside pi's 60 s request timeout.
- Hosts that send no progress token need a request timeout above 240 s, for example Codex `tool_timeout_sec = 300`.

### Results

- `content` holds one text block.
  - Example status line: `[exited 0] t3 · 41.2s · 812 lines · log /tmp/fastexec-1000-1234/t3.log`
  - Example omission marker in the output window: `... [770 lines omitted: log lines 21-790] ...`
- Two environment variables, read at server startup, shape results. Any other value fails startup with a diagnostic on stderr.
  - `FASTEXEC_STRUCTURED_CONTENT`: unset or `true` includes `structuredContent`; `false` selects text-only results for all actions and operational errors, with the output window and status line in `content`.
  - `FASTEXEC_OUTPUT_MODE` selects output placement when structured content is enabled:
    - `text` (default): `content` holds the output window and status line; `structuredContent` holds metadata.
    - `both`: `content` holds the output window and status line; `structuredContent.output` holds the same window.
    - `structured`: `content` holds the status line; `structuredContent.output` holds the window. This mode suits hosts that expose structured results to the model. pi's text-consuming path sees the status line.
  - Every mode uses the same output window, byte budget, and poll cursor. `list`, `kill`, and operational error results keep their text in every mode.
- `structuredContent` carries:
  - for `start`, `poll`, and `kill`: `ok`, `action`, `taskId`, `state` (`running` / `exited` / `killed`), `exitCode`, `pty`, `elapsedMs`, `logPath`, `lifetimeExpired`, and `logError` (the first log write failure, or `null`);
  - for `start` and `poll` with `screen`, additionally: `cursor` (`[row, column]`, 1-based) and `omittedRows`; in `both` and `structured` modes also `output`, the screen text;
  - for other `start` and `poll` results, additionally: `omittedLines`, `omittedRange` (`[first, last]` log lines or `null`), `cutLines` (shown lines that lost part of their text to a per-line limit or the budget), and `encodingErrors` (lines with invalid byte sequences). In `both` and `structured` modes they also carry `output`, the window text, with an empty string for an empty window; `content` displays `(no new output)` for that empty window in `text` and `both` modes;
  - for `list`: `ok`, `action`, and a `tasks` array of `{taskId, state, exitCode, pty, elapsedMs, command (first 120 chars), logPath, lifetimeExpired}`;
  - for operational errors: `ok: false`, `action`, and `error`.
- **Background footer.** `start`, `poll`, and `kill` results append one line after the status line, such as `(Background: t3 exited 7, t5 running 4m3s.)`. It names the other tasks that need attention: finished tasks whose final state no delivered result has shown, ranked failures (nonzero exit or `killAfterMs` expiry) first, then other completions, then running tasks, newest first within a rank. It names at most three, then appends the remaining count, aggregate counts, and `use list`; when that exceeds the cap it shows counts only. The cap is 512 bytes, and at most a quarter of `maxBytes` for bounded windows; the footer is reserved before the output window is sized, so the whole text still fits `maxBytes`. Showing a finished task's final state in a result, in the footer, or in `list` marks it reported. Results without such tasks carry no footer. The footer is best-effort delivery; `list` remains the complete view.
- Task IDs are short and server-scoped: `t1`, `t2`, and so on.
- Exit codes follow the bash convention: a signal exit reports `128 + signal`. On Windows the msys runtime reports signal N as status `N << 8`, which the server converts the same way. A Windows Job Object termination reports 1.
- A non-zero exit code is a normal result with `ok: true`. Operational failures set `isError: true` and `ok: false` and carry a message that tells the model what to do next. They cover invalid parameter values and combinations, spawn errors, too many running tasks, unknown task IDs, closed stdin, input backpressure, `eof` in PTY mode, failed tree termination, and cancellation.
- Arguments that fail schema decoding (unknown fields, wrong types, negative numbers) are rejected by the MCP layer before the tool runs, as an error result without `structuredContent`.

## 4. Tool Description

The tool description (`src/description.md`, at most 3 KB) says how to use the tool, and the server instructions returned by `initialize` say when. pi-mcp-adapter shows the instructions in its `mcp` proxy description. pi's built-in MCP lists one line for each server with `codemode` or `deferred` tools, taken from the configured `description` or else the first line of the instructions, so that first line stands alone; `describeNamespace()` returns the full instructions. `serverInfo` reports `fastexec` and the crate version.

`src/description.md` is the authoritative text. It covers:

- the four actions, their defaults, and the 240 s wait limit; a non-zero exit code is a normal result;
- GNU bash (Git Bash on Windows) and a fresh shell per `start`: calls pass `cwd` or chain with `&&`;
- wait sizing: long waits for builds and tests, short ones for prompts, no repeated `waitMs: 0` polls or `sleep` commands;
- task lifetime: tasks end with the session, and the rest of a tree ends with its root bash, so long-lived servers run as their own task;
- output: one merged stream, cleaning and `raw`, the `truncate` modes and per-line caps, narrowing with pipelines and `set -o pipefail`, and reading the log file with `sed` or `grep`;
- stdin: pipe-mode stdin stays open until `eof` or exit; `< /dev/null` for programs that read it;
- PTY: `keys` with tmux names for Enter and control keys, since a newline in `input` is not Enter to a TUI; password prompts need `pty: true`; `screen: true` for full-screen and TUI programs; `input` is recorded in the transcript;
- Windows: Git Bash path conversion with `MSYS_NO_PATHCONV=1` or `MSYS2_ARG_CONV_EXCL`, PowerShell through a script file or `-EncodedCommand`, and `loginShell: false` for faster starts;
- U+FFFD in output as the cue to pass `encoding`.

## 5. Output

### Capture

- Pipe mode writes stdout and stderr into one pipe, so output keeps the order in which the OS delivered it.
- PTY mode produces one terminal stream.
- A reader thread drains each task continuously and appends the raw bytes to the task log `<temp>/<dir>/<taskId>.log`, where `<dir>` is `fastexec-<uid>-<serverPid>` on Unix (users sharing /tmp neither collide nor examine each other's directories) and `fastexec-<serverPid>` on Windows (the temp directory is per user). On Unix the directory has mode 0700 and each log file mode 0600; on Windows both take the user's temp-directory ACL.
- Log limits:
  - Each task log holds the first 64 MiB of the task's output.
  - All logs of one server together hold up to 1 GiB; past that, logs of finished tasks are evicted, oldest task first.
  - Past a limit, the reader keeps draining, stops writing, and records the drop, so the child process keeps running at full speed.
  - A failed log write stops writing for that task in the same way and records the first error. The readable range ends at the last fully written chunk. Interrupted reads are retried; any other read error ends capture like EOF.
- The server keeps the 64 most recently finished tasks. Each time a task finishes, the tasks that finished earliest beyond that count are removed with their logs; the task that just finished always stays.
- The server deletes its log directory on shutdown. At startup it deletes the current user's log directories whose server process has ended; a forced kill is the only way to leave one behind. A process the user may not query (Windows access denied, Unix `EPERM`) counts as running.

### Screen

Each PTY task feeds all of its output into a 120×30 terminal emulator (`vt100` 0.16) without scrollback. `screen: true` returns the emulator's visible rows instead of the output window, after the usual wait:

- Each row loses its trailing spaces, and blank rows at the bottom are dropped. An empty screen shows `(blank screen)`.
- The status line ends with `screen, cursor row R col C` (1-based).
- The rows, status line, and footer fit `maxBytes`. When the rows do not fit, the top rows are dropped first, behind the marker `... [N top rows omitted] ...`.
- The output stream counts as read up to the screen, so a later stream poll starts after it. The log keeps every byte.
- The emulator decodes UTF-8.

### Model-facing window

Each `start` or `poll` result covers the bytes after the task's cursor. The cursor advances to the end of what the window covered, including omitted lines. Line numbers count LF-terminated lines of the log, and the log and the cleaned output share that numbering. A window that ends inside a line shows that partial line, such as a prompt; the next window shows the rest of the line when it has text.

While output continues, a window ends before an unfinished character. For UTF-8, capture holds back an incomplete trailing sequence. For other encodings, the window decodes its end from the last byte below 0x30, which no ASCII-compatible encoding uses inside a multibyte character, or from the window start, and holds back the 1–3 trailing bytes whose removal lets it decode. At EOF, or after the log is evicted, every byte is shown.

Every window decodes its text as UTF-8, or with `encoding` when given. Invalid sequences become U+FFFD; `encodingErrors` counts the affected lines, and the status line suggests `encoding`.

**Cleaning** (default; `raw: false`):

- Strip ANSI escape sequences: CSI, OSC, and two-byte ESC sequences.
- Treat CRLF as a newline.
- Treat a lone CR as overwriting the current line: the text after the last CR remains.
- Drop other C0 control characters and DEL, except tab.
- Trim trailing spaces; terminals such as ConPTY pad lines with them.
- In bounded modes, cap each line at 2000 characters and mark the cut.

With `raw: true`, the text keeps escape sequences, CR, and control characters. In bounded modes a line keeps its first 64 KiB, and the window marks the cut. `truncate: "none"` returns whole lines in both modes.

**Truncation** keeps whole lines within `maxBytes` UTF-8 bytes for the output window, status line, and footer together, across all output-placement modes. The budget covers the status line, its notes, the footer, and the omission marker, and a final cut enforces it exactly. `both` includes a second copy of the window in JSON; metadata and JSON serialization add bytes to the MCP response. A status line longer than half of `maxBytes` is cut as well; `structuredContent.logPath` keeps the full path when structured content is enabled:

| Mode | Window |
|---|---|
| `head_tail` | About 10% of the budget for the first lines, the rest for the last lines, with the omission marker between them |
| `head` | First lines that fit, then the omission marker |
| `tail` | Omission marker, then the last lines that fit |
| `none` | Every unseen line, whole. The host's own limit applies; in pi that is the 20 KiB middle cut |

The default budget of 16 KiB leaves room under pi's 20 KiB limit, so fastexec's line-aware window is the one the model sees.

Filtering beyond these modes lives outside the tool:

- shell pipelines inside `command`;
- `grep` or `sed` on the log file;
- in pi codemode, JavaScript applied to the returned text block, or to `structuredContent.output` in `both` and `structured` modes. Codemode receives the server's complete window before the host's text limit is applied.

## 6. Processes

### 6.1 Bash and launch

Bash discovery copies FastCtx [`src/shell/bash.rs`](https://github.com/yc-duan/fastctx/blob/ccaa157790d02328a60786eb94ee5ad698995a5f/src/shell/bash.rs):

- `FASTEXEC_BASH` sets an absolute override.
- On Windows, the search order is:
  1. `<git root>/usr/bin/bash.exe`, walking up to four directories from each `git.exe` on `PATH`;
  2. `ProgramFiles` and `ProgramFiles(x86)`;
  3. `LocalAppData\Programs\Git`;
  4. Scoop (`SCOOP`, then `%USERPROFILE%\scoop`);
  5. `bash.exe` on `PATH`.
- Candidates under `SystemRoot` (the WSL launcher) or `WindowsApps` are excluded.
- On Unix, the search order is `/bin/bash`, `/usr/bin/bash`, then `PATH`.
- A candidate qualifies when `--version` exits successfully and closes its output within 5 s, and the output is at most 4 KiB and contains `GNU bash`. Duplicate candidates compare case-insensitively on Windows and exactly on Unix. The result, including a failure, is cached per server.

Launch copies FastCtx [`src/shell/process.rs`](https://github.com/yc-duan/fastctx/blob/ccaa157790d02328a60786eb94ee5ad698995a5f/src/shell/process.rs):

- `-l -c <command>` or `--noprofile --norc -c <command>`.
- A command longer than 12000 bytes runs from a script file in the log directory, which keeps the command line under the Windows 32767-character limit.
- On Windows, a login shell gets `MSYS2_PATH_TYPE=inherit` unless the variable is already set. A non-login shell gets `usr/bin` and the existing `mingw64`, `mingw32`, and `clangarm64` `bin` directories prepended to `PATH`.
- Pipe-mode children and `--version` probes are created with `CREATE_NO_WINDOW`; PTY children attach to a ConPTY pseudoconsole.
- Commands get the `C.UTF-8` locale, which glibc 2.35+, musl, and msys provide; macOS gets `en_US.UTF-8`. `ponytail:` a fixed locale; probing `locale -a` covers older distributions if they matter.

PTY commands start from the server's process environment: portable-pty's own seeding, which on Windows reads the registry and drops runtime PATH entries and host-set values, is cleared first. Both modes then apply these overrides:

| Variable | Pipe mode | PTY mode |
|---|---|---|
| `LANG`, `LC_ALL` | `C.UTF-8` (macOS `en_US.UTF-8`) | `C.UTF-8` (macOS `en_US.UTF-8`) |
| `TERM` | `dumb` | `xterm-256color` |
| `NO_COLOR=1`, `CLICOLOR=0`, `FORCE_COLOR=0` | set | inherited |
| `PAGER=cat`, `GIT_PAGER=cat` | set | set |
| `EDITOR`, `VISUAL`, `GIT_EDITOR` = `true`; `GIT_TERMINAL_PROMPT=0` | set | inherited |
| `PYTHONUNBUFFERED=1`, `PYTHONIOENCODING=utf-8` | set | set |

### 6.2 Backends

- **Pipe:**
  - `std::process::Command`; stdout and stderr share one `std::io::pipe()`.
  - stdin is a pipe owned by a per-task writer thread with a 1 MiB queue; a full queue returns `INPUT_BACKPRESSURE`. When a write fails because the program closed stdin, the task's stdin counts as closed.
  - `eof` drops the pipe after the queued input has been written.
- **PTY:**
  - `portable-pty` 0.9.0 (ConPTY on Windows, Unix PTY elsewhere), size 120×30.
  - Input goes through the master writer, fed by the same per-task queue.
  - The reader drains `try_clone_reader()` into the same log path as pipe mode.
  - The reader feeds every chunk into the task's terminal emulator (§5), which answers each cursor-position query (`ESC[6n`) with the cursor's position. ConPTY sends one at startup and holds all output until a terminal answers. Replies share the 1 MiB input limit; replies past it are dropped.

### 6.3 Process-tree ownership

| Platform | Pipe | PTY | `kill` |
|---|---|---|---|
| Windows | FastCtx `KillOnCloseJobObject`: spawn with `CREATE_SUSPENDED`, assign to a Job Object with `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`, then resume | `AssignProcessToJobObject` on `Child::as_raw_handle()` right after `spawn_command` | `TerminateJobObject` |
| Unix | `setsid` in `pre_exec`, so the child PID is the session and group ID | portable-pty calls `setsid` before exec, so the child PID is the session and group ID | `kill(-pgid, SIGKILL)`, then on Linux SIGKILL to every process whose session ID (from `/proc/<pid>/stat`) is the task's, which covers job-control groups |

- On stdin EOF, SIGTERM, SIGINT, or SIGHUP (on Windows: stdin EOF or Ctrl-C), the server kills every task tree at once, removes its log directory, and exits. Stdin EOF ends the server even while waits are in flight, inside pi's 500 ms grace period.
- Windows: job handles close when the server exits, including when the server itself is killed. The kernel then terminates every task tree.
- Windows PTY: a process the child starts between spawn and job assignment stays outside the job. `ponytail:` assign-after-spawn race; the upgrade path is a portable-pty patch that spawns suspended.
- Unix: a process that starts its own session (`setsid`, daemonizing tools) leaves the task's tree. `ponytail:` session-based ownership; a per-task cgroup is the upgrade path for full containment.
- Unix: if the server receives SIGKILL before its shutdown cleanup runs, task groups survive it. `ponytail:` the upgrade path is FastCtx's helper-pipe watchdog ([`src/shell/jobs/host.rs`](https://github.com/yc-duan/fastctx/blob/ccaa157790d02328a60786eb94ee5ad698995a5f/src/shell/jobs/host.rs), "EOF means the host died and the command group must die").

### 6.4 Concurrency

Each task runs three threads: an output reader, an input writer, and a waiter. Short locks guard the output counters, the cursor, the input sender, and the final status; waits subscribe to a completion channel and hold no lock. Tool calls run in parallel, so a 240 s `poll` on one task leaves `kill` and other polls responsive. At most 16 tasks run at once; a further `start` returns `TOO_MANY_TASKS`.

A task is its whole process tree. When the root bash exits, the waiter terminates whatever remains of the tree, closes a PTY, and waits up to 2 s for the reader to reach EOF. It then closes stdin and commits the final state once. Long-lived processes such as development servers therefore run as their own task. If output stays open past the 2 s cap, the status line says so and later polls show any further output.

## 7. Host Configuration

pi (`~/.pi/agent/mcp.json`):

```json
{
  "mcpServers": {
    "fastexec": {
      "command": "fastexec",
      "exposure": "direct",
      "description": "Run bash commands with background tasks, stdin input, and PTY"
    }
  }
}
```

Progress notifications keep long waits inside the default 60 s `timeout`. Hosts without progress support set their tool timeout to at least 300 s.

pi with the `pi-mcp-adapter` extension (2.26), which replaces the built-in MCP support when installed, reads the same `mcpServers` shape with adapter fields:

```json
"fastexec": {
  "command": "C:/path/to/fastexec.exe",
  "lifecycle": "eager",
  "requestTimeoutMs": 300000,
  "directTools": true
}
```

- The adapter sends no progress token, so `requestTimeoutMs` must exceed the 240 s wait limit.
- `eager` keeps the server connected for the whole session. The default `lazy` lifecycle disconnects an idle server after 10 minutes, and the disconnect ends every running task.
- With the default `toolPrefix: "server"`, the model sees the tool as `fastexec_fastexec`.

## 8. Crate Layout and Provenance

| File | Content | Origin |
|---|---|---|
| `src/main.rs` | rmcp stdio server, tool schema, validation, waits, progress, results, shutdown | new |
| `src/description.md` | tool description shown to the model (§4) | new |
| `src/bash.rs` | bash discovery | FastCtx `src/shell/bash.rs` |
| `src/process.rs` | launch, environment, locale, Job Object, process session, PTY spawn | FastCtx `src/shell/process.rs` and `src/process_policy.rs`, plus PTY |
| `src/output.rs` | streaming ANSI/CR cleaning and head/tail windows under a byte budget | FastCtx `src/shell/normalize.rs` and `src/shell/output.rs`, with CR overwrite |
| `src/tasks.rs` | task registry, log capture, cursors, input queues, retention, shutdown | new |
| `src/keys.rs` | tmux key names to terminal input bytes (§3 Keys) | tmux `key-string.c` and `input-keys.c` |
| `tests/mcp.rs` | contract tests that drive the built binary over MCP stdio | new |

`Cargo.toml` lists the dependencies and pins rmcp (`=2.2.0`) and portable-pty (`=0.9.0`) exactly.

Files derived from FastCtx open with a header that names the FastCtx source files and credits FastCtx (Apache-2.0, Copyright 2026 yc-duan). The repository `NOTICE` lists those files and credits tmux (ISC) for the key names and encodings in `src/keys.rs`.

## 9. Acceptance

Run every case on Windows 11 x64 with Git for Windows and on Ubuntu x64 with bash, through pi 0.99.2 with `exposure: "direct"`.

| ID | Scenario | Pass criteria |
|---|---|---|
| A1 | Short command | `start` returns `exited`, exit code, and full output in one call |
| A2 | Long command | `start` returns `running` at `waitMs`; `poll` returns new output only; the final `poll` returns `exited` |
| A3 | 240 s poll | Completes within pi's request timeout; progress appears in the TUI; an early exit returns early |
| A4 | Cancel during wait | The call ends; the task keeps running; `list` shows a task whose `start` was cancelled |
| A5 | Pipe input | A line-reading program receives each exact `input`; `eof` delivers EOF |
| A6 | PTY input | The program sees a TTY; `keys` Enter, C-c, and C-d behave as terminal keys after `input`; arrows follow the program's cursor mode; an ssh password prompt accepts `input` |
| A7 | `kill` | Shell → child → grandchild trees end on both platforms; repeated `kill` returns the same final state |
| A8 | Session end | `/reload`, `/new`, and quit end every task tree and remove the log directory |
| A9 | Output windows | All four `truncate` modes respect `maxBytes`; the omitted line range matches the log; `raw` keeps ANSI and CR |
| A10 | Cleaning | Progress bars that use CR collapse to their final text; split UTF-8 and ANSI sequences across chunks decode correctly; `encoding: "big5"` decodes cp950 output |
| A11 | Log limits | Output beyond 64 MiB keeps the child running and reports the drop; memory stays bounded |
| A12 | Windows specifics | `MSYS_NO_PATHCONV=1` passes `/F` intact to a native tool; commands over 12000 bytes run; every child runs windowless |
| A13 | Parameters | Misplaced or out-of-range parameters return errors; `null` optional fields behave as omitted |
| A14 | TUI | pi's interactive TUI accepts a prompt typed with `input` and submitted with `keys: ["Enter"]`; `screen: true` shows one rendered frame |
