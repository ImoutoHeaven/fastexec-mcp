# fastexec

fastexec is a stdio MCP server with one tool, `fastexec`, that runs bash commands as tasks:

- A command that finishes within its wait window returns its full result in one call.
- A command that outlives the window keeps running in the background; the model continues with `poll`.
- Running tasks accept input through a stdin pipe or, on request, a pseudo-terminal (PTY). The PTY handles interactive prompts such as ssh passwords.
- `kill` terminates a task's whole process tree.

It runs on Windows (Git Bash) and Linux (GNU bash) and ships as a single binary.

## Build

```bash
cargo build --release
```

The binary is `target/release/fastexec` (`fastexec.exe` on Windows). Windows needs [Git for Windows](https://git-scm.com/downloads); fastexec finds its bash automatically. Set `FASTEXEC_BASH` to an absolute path to choose a specific GNU bash.

## Configure a host

pi with its built-in MCP support (`~/.pi/agent/mcp.json`):

```json
{
  "mcpServers": {
    "fastexec": {
      "command": "/path/to/fastexec",
      "exposure": "direct",
      "description": "Run long-running, stdin-driven, and terminal (PTY) bash commands as tasks"
    }
  }
}
```

The model sees the tool as `mcp__fastexec__fastexec`. pi sends progress tokens, and fastexec reports progress every 20 s, so long waits stay within pi's default 60 s request timeout.

pi with the `pi-mcp-adapter` extension:

```json
"fastexec": {
  "command": "C:/path/to/fastexec.exe",
  "lifecycle": "eager",
  "requestTimeoutMs": 300000,
  "directTools": true
}
```

- `requestTimeoutMs` must exceed the 240 s wait limit, because the adapter sends no progress token.
- `eager` keeps the server connected for the whole session. With `lazy`, an idle disconnect after 10 minutes ends every running task.
- The model sees the tool as `fastexec_fastexec`.

For any other MCP host, set its tool timeout to at least 300 s unless it sends progress tokens.

### Output placement

Set `FASTEXEC_OUTPUT_MODE` in the MCP server's `env` configuration. The mode applies to every `start` and `poll` result for that server:

| Mode | Text `content` | `structuredContent.output` |
|---|---|---|
| `text` (default) | Output body and status line | Omitted |
| `both` | Output body and status line | Output body |
| `structured` | Status line | Output body |

Every mode preserves structured task and window metadata, including `taskId`, `state`, `exitCode`, `logPath`, `omittedRange`, and `cutLines`. `list`, `kill`, and error results keep their text and metadata. An invalid mode fails at startup with a diagnostic on stderr.

Use `text` for pi and other text-consuming hosts. Use `structured` for hosts that expose structured results to the model, such as Codex. Use `both` for callers that require the body in both forms. A host that reads only `content` sees the status line in `structured` mode; the task log holds the body.

For example, add this field to the server configuration:

```json
"env": { "FASTEXEC_OUTPUT_MODE": "structured" }
```

Programmatic callers read the body from the text block in `text` mode, or from `structuredContent.output` in `both` and `structured` modes. Output cleaning, truncation, shell pipelines, and the poll cursor work identically in every mode.

## Agent instructions

The tool description explains how to use fastexec, and the server instructions explain when. To make an agent prefer fastexec for long-running and interactive work, append this section to its global instructions file, such as `~/.pi/agent/AGENTS.md`, `~/.codex/AGENTS.md`, or `~/.claude/CLAUDE.md`. The markers let a later update replace the block in place.

```markdown
<!-- fastexec:begin -->
### fastexec

- Use the fastexec tool for commands that may run longer than a minute, servers and
  watchers, programs that read stdin, and programs that need a terminal (`pty: true`),
  including password prompts.
- Start with `start`; continue with `poll`, using long `waitMs` values (up to 240000)
  for builds and tests and short ones for interactive prompts. Stop with `kill`.
- fastexec tasks end with the session. Use a persistent job runner for work that must
  survive a session restart.
- Each `start` runs a fresh bash: `cd` and exported variables do not carry over; pass
  `cwd` or chain commands with `&&`.
<!-- fastexec:end -->
```

The tool's name depends on the host, for example `mcp__fastexec__fastexec` in pi and `fastexec_fastexec` with `pi-mcp-adapter`, so the section names it "the fastexec tool". If the file already routes work to other shell tools, keep one owner per kind of work: one-shot commands, in-session long-running or interactive tasks, and jobs that outlive the session.

## Tool

One tool takes an `action` and the parameters that apply to it:

| Action | Parameters | Behavior |
|---|---|---|
| `start` | `command`, `cwd`, `pty`, `loginShell`, `waitMs` (default 30000), output options | Runs the command in bash and waits. Returns the final result when the command exits in time; otherwise returns a `taskId` and the output so far. |
| `poll` | `taskId`, `input`, `eof`, `waitMs` (default 2000 with input, 30000 without), output options | Writes `input` exactly as given, closes stdin when `eof` is true, waits until the task ends or `waitMs` elapses, and returns output not yet seen. |
| `kill` | `taskId` | Terminates the task's process tree and returns its final state. |
| `list` | — | Lists this server's tasks, newest first. |

Output options:

- `truncate`: `head_tail` (default), `head`, `tail`, or `none`. The bounded modes cut lines longer than 2000 characters (64 KiB with `raw`) and count them in `cutLines`; `none` returns whole lines.
- `maxBytes`: 1024–1048576, default 16384. It bounds the output window and status line together. `both` includes a second copy of the window in JSON; metadata and JSON serialization add bytes to the MCP response.
- `raw`: `true` skips cleaning.
- `encoding`: a WHATWG label such as `big5` or `gbk`.

Behavior:

- **Waits.** Each wait lasts at most 240 s. An ended wait, a cancelled call, or a timeout leaves the process running.
- **Environment.** Pipe and PTY tasks inherit the server's environment, then fastexec sets terminal, pager, and locale variables.
- **Output.** stdout and stderr share one stream. Cleaning strips ANSI sequences, collapses carriage-return progress bars to their final text, and trims trailing spaces.
- **Results.** Each `start` or `poll` text result ends with a status line such as `[exited 0] t3 · 41.2s · 812 lines · log /tmp/fastexec-1000-1234/t3.log`. `structuredContent` carries task and window metadata as JSON. `FASTEXEC_OUTPUT_MODE` selects where the output body appears.
- **Logs.** Each task's output is kept in a log file of up to 64 MiB, and omitted lines are named by their log line numbers. The server keeps the 64 most recently finished tasks and deletes its log directory on exit. Log directories are `fastexec-<uid>-<pid>` on Unix and `fastexec-<pid>` on Windows, in the temp directory; at startup the server removes the current user's directories whose server process has ended, such as those left by a forced kill.
- **Process trees.** A task is its whole process tree. When the root bash exits, the rest of the tree ends too, so long-lived servers run as their own task. Windows uses a kill-on-close Job Object. Linux kills the task's process group and every process in its session; a process that starts its own session (`setsid`, daemons) leaves the tree.
- **PTY input.** In PTY mode, send a carriage return (`"\r"`) for Enter, `"\u0003"` for Ctrl-C, and `"\u0004"` for Ctrl-D.
- **Lifetime.** Tasks live as long as the server. The host stops the server when its session ends, and every task ends with it.

## Test

```bash
cargo test
```

The integration tests in `tests/mcp.rs` drive the built binary over MCP stdio.

## License

Apache-2.0. Parts of `src/bash.rs`, `src/process.rs`, and `src/output.rs` derive from [FastCtx](https://github.com/yc-duan/fastctx); see `NOTICE`.
