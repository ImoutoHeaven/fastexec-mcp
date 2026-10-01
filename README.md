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
      "exposure": "direct"
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

## Tool

One tool takes an `action` and the parameters that apply to it:

| Action | Parameters | Behavior |
|---|---|---|
| `start` | `command`, `cwd`, `pty`, `loginShell`, `waitMs` (default 30000), output options | Runs the command in bash and waits. Returns the final result when the command exits in time; otherwise returns a `taskId` and the output so far. |
| `poll` | `taskId`, `input`, `eof`, `waitMs` (default 2000 with input, 30000 without), output options | Writes `input` exactly as given, closes stdin when `eof` is true, waits until the task ends or `waitMs` elapses, and returns output not yet seen. |
| `kill` | `taskId` | Terminates the task's process tree and returns its final state. |
| `list` | — | Lists this server's tasks, newest first. |

Output options:

- `truncate`: `head_tail` (default), `head`, `tail`, or `none`.
- `maxBytes`: 1024–1048576, default 16384. It bounds the whole result, status line included.
- `raw`: `true` skips cleaning.
- `encoding`: a WHATWG label such as `big5` or `gbk`.

Behavior:

- **Waits.** Each wait lasts at most 240 s. An ended wait, a cancelled call, or a timeout leaves the process running.
- **Output.** stdout and stderr share one stream. Cleaning strips ANSI sequences, collapses carriage-return progress bars to their final text, and trims trailing spaces.
- **Results.** Each result ends with a status line such as `[exited 0] t3 · 41.2s · 812 lines · log /tmp/fastexec-1234/t3.log`. `structuredContent` carries the same data as JSON: `state`, `exitCode`, `output`, `omittedRange`, and `logPath`.
- **Logs.** Each task's output is kept in a log file of up to 64 MiB, and omitted lines are named by their log line numbers. The server keeps the 64 most recently finished tasks and deletes its log directory on exit.
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
