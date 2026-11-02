# fastexec

`fastexec` is an MCP server that runs bash commands as background tasks, with
stdin pipes, interactive PTYs, bounded output, and process-tree termination, and
reads, writes, and edits files on the host. It also runs a background daemon
shared by every session in a namespace, opt-in with `FASTEXEC_DAEMON=on`.

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
    "fastexec_local": {
      "command": "/absolute/path/to/fastexec",
      "exposure": "direct"
    }
  }
}
```

Hosts without progress-aware timeouts need at least 300 seconds per tool call.

## Tool Usage and Example

The server exposes `exec` for bash tasks and `read`, `write`, and `edit` for
files; an HTTP session also gets `transfer`. Pi names each tool
`mcp__<server>__<tool>`, where `<server>` is the key in the configuration, so
the entry above gives `mcp__fastexec_local__exec`.

`exec` selects an operation with its `action` field (`start`, `poll`, `kill`,
`list`, `transcript`)—these are MCP tool arguments, not CLI subcommands. See
[`src/description.md`](src/description.md) for the runtime parameter contract,
and [the design document](docs/fastexec-design.md) for the file tools.

Start a command through the tool:

```json
{"action": "start", "command": "printf 'ready\\n'; sleep 5; printf 'done\\n'", "waitMs": 1000}
```

If it is still running, poll with the returned `taskId` (replace the placeholder):

```json
{"action": "poll", "taskId": "<returned taskId>", "waitMs": 10000}
```

### Several servers

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

`${NAME}` reads the variable from the environment Pi runs in. Prefer `codemode`
or `deferred` exposure for a remote server: a `direct` server that has not
connected within the first prompt's 10 second wait changes the declared tools
mid-session.

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
    "fastexec_local": {
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

Daemons stay running when idle by default. `fastexec daemon start --idle-exit on`
enables exit after 10 minutes with no connections and an empty task registry;
`--idle-exit off` explicitly keeps the default. Finished tasks remain in the
registry for up to 24 hours. These options apply only when starting a new daemon;
stop an existing daemon before changing its policy. HTTP daemons never exit idle.

A host that keeps its children in a Job without breakaway, such as a fastexec
task, cannot start a detached daemon: run `fastexec daemon start` from a
terminal outside the host, or `fastexec daemon run` in the foreground.

See [`docs/fastexec-design.md`](docs/fastexec-design.md) §6 for the directory
layout, limits, and lifetimes.

## Remote host

`fastexec daemon run --http 127.0.0.1:<port>` serves the same toolset over
Streamable HTTP in the foreground, in the process the supervisor started. Once
both listeners are bound it prints one line to stderr — `fastexec daemon <epoch>
pid <pid> namespace <namespace> http <addr>` — and keeps its logs on stderr.
`SIGTERM` and `SIGINT`, and on Windows `Ctrl+C`, `Ctrl+Break`, console close and
shutdown, stop it the way `fastexec daemon stop` does: every task is killed and
the process exits `0`. That cleanup is best effort where the operating system
ends the process first, by a close deadline it enforces or a forced termination.
It runs with idle exit off, refuses `--idle-exit`, and exits `1` when the
namespace already has a daemon.

`FASTEXEC_HTTP_TOKEN`, at least 16 visible ASCII characters, is the bearer token
every `/mcp` request carries.

A systemd **user** unit shares `XDG_RUNTIME_DIR` with the user's shells, so
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
rather than LocalSystem:

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
the service and the user's shell must see the same value: set it explicitly
rather than inherit an administrator's profile. A service runs in Session 0
without a desktop, so an app that needs a visible window belongs in Task
Scheduler "at logon" instead.

### Behind a proxy

`FASTEXEC_HTTP_ALLOW_REMOTE=1` is required even on a loopback bind, because a
proxy forwards the public `Host`. Route `/`, or both `/mcp` and `/t/`, to the
daemon: a `/mcp`-only route leaves `transfer` URLs unreachable.

```nginx
location / {
    proxy_pass http://127.0.0.1:8765;
    proxy_http_version 1.1;
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-Proto $scheme;
    client_max_body_size 4m;
}
```

`transfer` builds its URLs from the `Host` it receives, so without that line
they name the loopback address. The server sends `X-Accel-Buffering: no` on the
responses a proxy would otherwise buffer.

Cloudflare reaches the daemon through a named tunnel; Quick Tunnels do not
support server-sent events. Plan limits are in
[Cloudflare's connection limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/).

`transfer` publishes one file at a URL that serves it to `curl`, valid until 10
minutes pass unused. A proxy that limits request size still carries a large file
in parts, one at a time; the tool description has the recipe.

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
