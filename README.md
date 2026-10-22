# fastexec

`fastexec` is a stdio MCP server that runs bash commands as background tasks,
with stdin pipes, interactive PTYs, bounded output, and process-tree termination.

## Requirements and Quickstart

- **Build**: Rust `1.92+` (`edition = "2024"`).
- **Runtime**: GNU bash on Linux, or [Git for Windows](https://git-scm.com/downloads)
  on Windows. Set `FASTEXEC_BASH` to an absolute GNU bash path to override discovery.

Build `target/release/fastexec` (`fastexec.exe` on Windows), then register its
absolute path in Pi's `~/.pi/agent/mcp.json`:

```bash
cargo build --release
```

```json
{
  "mcpServers": {
    "fastexec": {
      "command": "/absolute/path/to/fastexec",
      "exposure": "direct",
      "description": "Run long-running, stdin-driven, and terminal (PTY) bash commands as tasks"
    }
  }
}
```

Hosts without progress-aware timeouts need at least 300 seconds per tool call.

## Tool Usage and Example

The server exposes one MCP tool, `fastexec` (`mcp__fastexec__fastexec` in Pi).
Operations are selected via the tool's `action` field (`start`, `poll`, `kill`,
`list`, `transcript`)—these are MCP tool arguments, not CLI subcommands. See
[`src/description.md`](src/description.md) for the runtime parameter contract.

Start a command through the tool:

```json
{"action": "start", "command": "printf 'ready\\n'; sleep 5; printf 'done\\n'", "waitMs": 1000}
```

If it is still running, poll with the returned `taskId` (replace the placeholder):

```json
{"action": "poll", "taskId": "<returned taskId>", "waitMs": 10000}
```

## Key Constraints

- **Wait window vs. process deadline**: `waitMs` (`0–240000` ms) bounds only how
  long the current `start` or `poll` call waits. If `waitMs` elapses,
  `returnWhen` matches, or the call is cancelled, the task keeps running. Use
  `killAfterMs` on `start` or `action: "kill"` to bound or stop the process tree.
- **Session-bound process trees**: Tasks inherit server permissions, without a
  sandbox. Every `start` runs a fresh bash (`cd` and exports do not persist).
  Processes left behind when root bash exits are killed
  unless started with `keepTree: true`, and all tasks end when the server exits.
- **Pipe stdin vs. PTY keys**: Pipe-mode stdin stays open until `eof: true` or
  exit (pass `< /dev/null` when stdin is unused). In PTY mode (`pty: true`),
  send text with `input` and terminal keys (such as `"Enter"` or `"C-c"`) with `keys`.
- **Sensitive input and logs**: MCP hosts may log tool arguments (`command`,
  `input`, `keys`). Task output is written to temp log files (up to `64` MiB per
  task), and PTY transcripts record terminal-rendered screen text, including
  echoed input.
- **Windows path conversion**: Git Bash rewrites POSIX-shaped arguments (`/c`,
  `/tmp/x`) for native Windows binaries; prefix commands with `MSYS_NO_PATHCONV=1`.

## Testing, Reference, and License

```bash
cargo test
```

See [`docs/fastexec-design.md`](docs/fastexec-design.md) for host timeouts,
`FASTEXEC_OUTPUT_MODE`, PTY keys, transcript spooling, and process trees, and
[`src/description.md`](src/description.md) for agent usage rules.

Licensed under [Apache-2.0](LICENSE). Derived parts from
[FastCtx](https://github.com/yc-duan/fastctx) and key-encoding credits are in
[`NOTICE`](NOTICE). Windows x64 builds embed Microsoft ConPTY under the MIT
license in [`vendor/conpty/README.md`](vendor/conpty/README.md).
