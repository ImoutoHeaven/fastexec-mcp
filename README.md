# fastexec

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/fastexec-dark.svg">
  <img src="assets/fastexec.svg" alt="fastexec" width="560">
</picture>

`fastexec` is an MCP server that runs bash commands as background tasks, with
stdin pipes, interactive PTYs, bounded output, and process-tree termination, and
reads, writes, and edits files on the host. It also runs a background daemon
shared by every session in a namespace, opt-in with `FASTEXEC_DAEMON=on`.

## Requirements and Quickstart

- **Runtime**: GNU bash on Linux (including Alpine), macOS, and FreeBSD, or
  [Git for Windows](https://git-scm.com/downloads) on Windows. Install bash with
  `apk add bash` on Alpine or `pkg install bash` on FreeBSD. Set `FASTEXEC_BASH`
  to an absolute GNU bash path to override discovery. Linux requires kernel
  `5.3+`, readable `/proc`, and permission to use `pidfd_open` and
  `pidfd_send_signal`; these keep process-tree termination safe when PIDs are reused.
- **Build** (optional): Rust `1.92+` (`edition = "2024"`) and a C compiler.
  Linux musl builds need `musl-tools`. FreeBSD builds also need `llvm`; set
  `LIBCLANG_PATH` to its `lib` directory, such as `/usr/local/llvm21/lib`.

Download the archive for your platform from
[Releases](https://github.com/ImoutoHeaven/fastexec-mcp/releases)
(`linux`, `macos`, or `windows`, each `amd64` or `arm64`, and `freebsd-amd64`),
check it against `SHA256SUMS`, and extract `fastexec` (`fastexec.exe` on Windows).
The Linux binaries link musl statically and run directly on Alpine without a
glibc compatibility package. The FreeBSD archive is built and tested on FreeBSD
15.1; source builds require FreeBSD 15.1 or newer and use its native toolchain.
Shared runtime directories require atomic no-replace directory renames; UFS and
ZFS are tested. The macOS binaries are
unsigned: clear the quarantine a browser download sets with
`xattr -d com.apple.quarantine fastexec`. To build from source instead:

```bash
cargo build --release
```

Register the absolute path of the binary (`target/release/fastexec` when built)
in Pi's `~/.pi/agent/mcp.json`:

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

## JavaScript tasks

Set `FASTEXEC_CODEMODE=true` to expose `codemode` and advertise it in server
instructions. Its `code` argument is the body of an async JavaScript function.
The script can call `exec`, `read`, `write`, and `edit`, inspect their
structured results, and return only the output it needs:

```javascript
const [run, file] = await Promise.all([
  tools.exec({action: "start", command: "printf ready"}),
  tools.read({path: "README.md", limit: 5})
]);
text(run.output);
text(file.text);
```

`text`, `console.log/info/warn/error/debug`, and a returned value write to the
task log. `image(await tools.read({path: "shot.png"}))` saves and returns an
image. A result carries at most four images and 4718592 base64 bytes; warnings
name saved images that can be read separately. A nested `exec poll` or `kill`
that receives images exposes the blocks in `result.codemode.images`. Pass a
block to `image()` to include it in the outer script's result.

`codemode` accepts `cwd`, `waitMs`, `returnWhen` with `outputContains` or `quietMs`,
`killAfterMs`, `truncate`, and `maxBytes`. When `cwd` is set, nested `exec`
starts (unless they pass their own `cwd`) and relative paths in `read`, `write`,
and `edit` resolve against it. Continue with `exec poll`,
stop with `exec kill`, and inspect with `exec list`. A script has no terminal
or input. `sh.quote(value)` single-quotes strings and finite numbers for POSIX
bash, and UTF-8 `TextEncoder` and `TextDecoder` are available alongside `atob`
and `btoa`, and `sleep(ms)` pauses without spawning a process; JavaScript has no
Node APIs, modules, callback timers, filesystem, or network access, and host
operations go through `tools` (`exec` starts run non-interactively with the
session's login-environment snapshot by default; pass `loginShell: true` when a
command needs fresh login profile side effects).

Stopping a script cancels its pending tool calls. Bash tasks it already
started continue running, and the result lists their handles for `exec kill`.
A request cancellation ends only that request's wait. Set `killAfterMs` to
bound the script's lifetime.

| Setting | Default | Effect |
| --- | --- | --- |
| `FASTEXEC_CODEMODE` | `false` | Exposes the JavaScript tool when `true`. |
| `FASTEXEC_YOLO` | `false` | Overrides annotations on all exposed tools, including codemode. |
| `FASTEXEC_STRUCTURED_CONTENT` | `true` | Includes task metadata in results. |
| `FASTEXEC_OUTPUT_MODE` | `text` | Places task output in `text`, `both`, or `structured` output. |
| `FASTEXEC_DAEMON` | `off` | Runs tasks in the shared daemon when `on`. |

`FASTEXEC_CODEMODE`, `FASTEXEC_YOLO`, and `FASTEXEC_STRUCTURED_CONTENT` accept
only `true` or `false`; invalid values stop startup.
An endpoint's environment decides whether it advertises codemode, even when
its daemon was started without it. HTTP uses the daemon's startup setting.
Nested calls resolve directly to unwrapped structured objects regardless of the
outer output setting (`exec` returns `{output, exitCode, state, ...}` defaulting
to `truncate: "head"` and `maxBytes: 4194304` with NUL bytes preserved in clean
output and no default `waitMs` ceiling, text `read` without `offset` or `limit`
returns the whole UTF-8 file up to `4` MiB or rejects with `ToolError` if larger
or non-UTF-8 — and with `offset` or `limit` reads a slice up to `4` MiB — as
`{kind: "text", path, text, startLine, endLine, shownLines, totalLines,
truncated, nextOffset, lineTooLong, lossy}`, image `read` returns
`{path, attached, ...}`, `write` returns `{path, bytes}`, and `edit` returns
`{path, replacements, edits}`). Validation, file, and integrity errors reject
with `ToolError` (`{tool, result}`), while non-zero command exit codes
resolve normally.

Each script has 128 MiB of VM memory, eight `exec start` calls and eight
file/transcript I/O calls in flight (`poll`, `kill`, `list`, and `sleep` take no
concurrency slot) within 1024 pending calls, a separate 128 MiB budget for
pending tool arguments and results,
64 MiB of output including images and diagnostic space, and up
to 64 images. Up to four scripts run in a local stdio server, or eight in a
daemon. Each script accepts at most 4096 `exec start` calls, including calls
that later fail or are cancelled; further starts reject with `RangeError`.
If a completed tool result cannot fit the bridge budget, its call rejects
with `RangeError` after the operation ran; inspect its effects before retrying.
The existing total task and log limits also apply. See
[`src/description-codemode.md`](src/description-codemode.md) for the tool contract.

## Key Constraints

- **Wait window and process deadline**. Direct `start` and `poll` calls accept
  `waitMs` from 0 to 240000 ms. Nested calls accept longer waits and default to
  waiting until completion. `returnWhen` sets a 30 s default. Without it,
  a poll that sends input, presses keys, or resizes the PTY defaults to 2 s. If
  `waitMs` elapses, `returnWhen` matches, or the call is cancelled, the task
  keeps running. Use `killAfterMs` on `start` or `action: "kill"` to bound or
  stop the process tree.
- **Owned process trees**: Tasks inherit server permissions, without a
  sandbox. Every `start` runs a fresh bash (`cd` and exports do not persist).
  Processes left behind when root bash exits are killed
  unless started with `keepTree: true`.
- **Task lifetime**: with the default `FASTEXEC_DAEMON=off`, tasks end when the
  server's session ends. With `FASTEXEC_DAEMON=on` they run in the namespace's
  daemon, outlive sessions, and are visible to every session; they end by exit,
  `kill`, `killAfterMs`, or `fastexec daemon stop`, and finished tasks stay
  listable for up to 24 h.
- **Pipe stdin vs. PTY keys**: Pipe-mode `start` connects stdin to `/dev/null`
  by default so tools like `rg` or `cat` never hang waiting on unused stdin;
  pass `input` (up to `1` MiB on `start`, closing stdin afterwards unless
  `eof: false`) or `eof: false` to stream follow-up `poll` input (`<= 16` KiB
  per poll). In PTY mode (`pty: true`), send text with `input` on `poll` and
  terminal keys (such as `"Enter"` or `"C-c"`) with `keys`.
- **Data integrity feedback**: Invalid or oversized requests are rejected. Input
  writes are acknowledged; a failed or unconfirmed write returns an error and
  may have partially taken effect. Canonical PTY input warns that long lines can
  be discarded even when split across polls; use a pipe or file for long text.
  Windows MSYS readers may also lose a leading byte after a resize, even for
  short keys; PTY input results explicitly warn about this [known risk](https://github.com/ImoutoHeaven/fastexec-mcp/issues/5).
  Output capture failures and permanent log loss are explicit, with warnings
  preserved even beyond `maxBytes`.
- **Sensitive input and logs**: MCP hosts may log tool arguments (`command`,
  `input`, `keys`). Task output is written to log files (up to `64` MiB per
  task), and PTY transcripts record terminal-rendered screen text, including
  echoed input.
- **Windows paths and path conversion**: Tool `cwd` and `path` fields accept
  relative paths, `~`, `C:/...`, `C:\...`, and Git Bash drive paths (`/c`,
  `/c/...`), refuse driveless POSIX paths like `/tmp/x` (use `cygpath -m` when
  needed), and always report forward slashes. Inside bash commands, Git Bash
  rewrites POSIX-shaped arguments for native Windows binaries unless prefixed
  with `MSYS_NO_PATHCONV=1`.

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

`FASTEXEC_HTTP_TOKEN` holds one or more bearer tokens separated by ASCII
whitespace. Each token needs at least 16 visible ASCII characters; empty lists
and duplicate tokens are refused. Every `/mcp` request carries one of them.

Modern MCP clients use `server/discover` and sessionless requests. Each token
has its own session for the listener's lifetime, with owner labels `http-t1`,
`http-t2`, and so on in configuration order. Clients sharing a token share
background notices. Other clients' running tasks appear once per session in the
footer; `list` defaults to the current session and accepts `scope: "all"` to
show all tasks with their owner labels. Legacy clients retain
their client-named sessions through the `initialize` handshake.

A `start` result's `taskId` is a reader handle, `<taskId>.<reader>`. Poll with
that handle to keep your own output cursor. IDs from `list` and background
notices are bare task IDs: polling one creates a new reader from the beginning
of the retained log and returns its handle with a `new reader` notice. A task
can hold 16 readers per session; a new reader replaces the least recently used
idle reader, while readers with active calls stay in place. A handle whose
reader was evicted, or belongs to an ended session, attaches again from the
retained log's beginning. These reader rules also apply to stdio and daemon
endpoint clients.

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
proxy forwards the public `Host`. The daemon serves `/mcp` and `/t/<id>`, and an
authenticated request for any other path answers 404, so a proxy that forwards
its prefix unstripped fails at `initialize` instead of at a download.

One domain carries every machine, each on a prefix of its own that the proxy
strips. Each daemon is started with the address clients reach, which is the root
`transfer` mints its URLs from:

```bash
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

The same block with `/linux/`, `/windows/` and one port per machine. MCP clients
use `<root>mcp`: `https://example.com/mac/mcp` for the block above.

When `FASTEXEC_PUBLIC_URL` is unset, `transfer` uses the `Host` and
`X-Forwarded-Proto` of the request it answers. The daemon reads the variable
once when it starts and keeps that root until it stops; a `daemon start` that
finds a running HTTP daemon keeps the root that daemon already has, and a daemon
started over stdio has no HTTP front door and no root. A value that is not
`http` or `https`, names no host, has a port outside 1-65535, or carries
userinfo, a query, a fragment, a dot segment or an empty path segment is refused
with its reason and exit 1, before anything binds.

The server sends `X-Accel-Buffering: no` on the responses a proxy would
otherwise buffer.

Cloudflare reaches the daemon through a named tunnel; Quick Tunnels do not
support server-sent events. Plan limits are in
[Cloudflare's connection limits](https://developers.cloudflare.com/fundamentals/reference/connection-limits/).

`transfer` publishes one regular file at a URL that serves it to `curl`,
returning its resolved `path` and current `bytes` (`null` when the file does not
exist yet) and staying valid until 10 minutes pass unused. Directories are
refused (pack them with `tar` via `exec` first). A proxy that limits request
size still carries a large file in parts, one at a time; the tool description
has the recipe.

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
[`NOTICE`](NOTICE). Windows x64 and arm64 builds embed Microsoft ConPTY under the MIT
license in [`vendor/conpty/README.md`](vendor/conpty/README.md).
