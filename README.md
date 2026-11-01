# fastexec

`fastexec` is a stdio MCP server that runs bash commands as background tasks,
with stdin pipes, interactive PTYs, bounded output, and process-tree termination.
It also runs a background daemon shared by every session in a namespace, opt-in
with `FASTEXEC_DAEMON=on`.

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
- **Owned process trees**: Tasks inherit server permissions, without a
  sandbox. Every `start` runs a fresh bash (`cd` and exports do not persist).
  Processes left behind when root bash exits are killed
  unless started with `keepTree: true`.
- **Task lifetime**: with the default `FASTEXEC_DAEMON=off`, tasks end when the
  server's session ends. With `FASTEXEC_DAEMON=on` they run in the namespace's
  daemon, outlive sessions, and are visible to every session; they end by exit,
  `kill`, `killAfterMs`, or `fastexec daemon stop`, and finished tasks stay
  listable for up to 24 h.
- **Pipe stdin vs. PTY keys**: Pipe-mode stdin stays open until `eof: true` or
  exit (pass `< /dev/null` when stdin is unused). In PTY mode (`pty: true`),
  send text with `input` and terminal keys (such as `"Enter"` or `"C-c"`) with `keys`.
- **Sensitive input and logs**: MCP hosts may log tool arguments (`command`,
  `input`, `keys`). Task output is written to log files (up to `64` MiB per
  task), and PTY transcripts record terminal-rendered screen text, including
  echoed input.
- **Windows path conversion**: Git Bash rewrites POSIX-shaped arguments (`/c`,
  `/tmp/x`) for native Windows binaries; prefix commands with `MSYS_NO_PATHCONV=1`.

## Shared daemon (`FASTEXEC_DAEMON=on`)

Set `env` on the server entry to run every session's tasks in one daemon per
namespace:

```json
{
  "mcpServers": {
    "fastexec": {
      "command": "/absolute/path/to/fastexec",
      "env": {"FASTEXEC_DAEMON": "on"}
    }
  }
}
```

`FASTEXEC_NAMESPACE` names the daemon: sessions that set the same name share
one, and different builds never share one. `fastexec daemon start|status|stop`
starts it, reports its epoch, build, PID, sessions, tasks, and HTTP address, and
stops it; `status` and `stop` never start one.

A host that keeps its children in a Job without breakaway, such as a fastexec
task, cannot start the daemon: run `fastexec daemon start` from a terminal.

`fastexec daemon start --http 127.0.0.1:<port>` also serves the tool over
Streamable HTTP, with `FASTEXEC_HTTP_TOKEN` as the bearer token every request
carries.

See [`docs/fastexec-design.md`](docs/fastexec-design.md) §6 for the directory
layout, limits, and lifetimes.

## Testing, Reference, and License

```bash
cargo test
```

Windows daemon tests must run outside any Job: build with
`cargo test --no-run` and run the test executables from a shell outside any Job
(`cargo test` places its children in one).

See [`docs/fastexec-design.md`](docs/fastexec-design.md) for host timeouts,
`FASTEXEC_OUTPUT_MODE`, PTY keys, transcript spooling, and process trees, and
[`src/description.md`](src/description.md) for agent usage rules.

Licensed under [Apache-2.0](LICENSE). Derived parts from
[FastCtx](https://github.com/yc-duan/fastctx) and key-encoding credits are in
[`NOTICE`](NOTICE). Windows x64 builds embed Microsoft ConPTY under the MIT
license in [`vendor/conpty/README.md`](vendor/conpty/README.md).
