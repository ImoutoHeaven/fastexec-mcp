# fastexec

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/fastexec-dark.svg">
  <img src="assets/fastexec.svg" alt="fastexec" width="560">
</picture>

`fastexec` is an MCP server for bash tasks and host files. Run commands with
`exec`, or use `read`, `write`, and `edit` for files. Tasks run with the
server account's permissions and have no sandbox.

## MCP compatibility

`fastexec` serves MCP over stdio and Streamable HTTP across two client
lifecycles:

- Session-based `initialize` and `notifications/initialized` (including HTTP `Mcp-Session-Id` sessions).
- Stateless `server/discover` and requests carrying inline `2026-07-28` `_meta`.

On protocol `2026-07-28`, `fastexec` also exposes MCP Events (`events/list`,
`events/poll`, and `events/stream`) for `task.exited` and `external.event`.

## Quick start

Install GNU bash on Linux, macOS, or FreeBSD, or
[Git for Windows](https://git-scm.com/downloads) on Windows. Linux requires
kernel `5.3` or newer, readable `/proc`, and access to the pidfd syscalls.
Set `FASTEXEC_BASH` to an absolute path if Bash is not on `PATH`.

Download the matching archive from
[Releases](https://github.com/ImoutoHeaven/fastexec-mcp/releases), verify it
against `SHA256SUMS`, then extract `fastexec` (`fastexec.exe` on Windows).
Linux, macOS, and Windows provide amd64 and arm64 archives; FreeBSD provides
amd64. See [platform notes](docs/fastexec-design.md#platform-notes) for package
and build prerequisites. To build from source:

```bash
cargo build --release --locked
```

Register the absolute binary path (`target/release/fastexec`). For Pi, add an
entry in `~/.pi/agent/mcp.json`:

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

In Pi the command tool is `mcp__fastexec_local__exec`. Start a bash task:

```json
{"action": "start", "command": "printf 'ready\\n'; sleep 5; printf 'done\\n'", "waitMs": 1000}
```

If the task is still running, poll with the returned `taskId`:

```json
{"action": "poll", "taskId": "<returned taskId>", "waitMs": 10000}
```

Reuse the returned `taskId` handle to keep your output cursor. A wait timeout
or cancelled request leaves the task running; set `killAfterMs` for a hard
deadline or send `action: "kill"` to stop it. Each `start` launches a fresh
Bash shell, so `cd` and shell exports do not carry over. Use pipes for standard
input, or `pty: true` with `keys` for interactive terminals.

Hosts without progress-aware timeouts need at least `300` seconds per tool
call. Hosts may log MCP arguments, and task logs and PTY transcripts record
terminal output and echoed input, so avoid passing secrets in arguments or
typing them into a recorded PTY. After an unconfirmed input write or an
oversized Codemode result, inspect task state before retrying.

## Choose how to run

| Setting | Default | When to change it |
| --- | --- | --- |
| `FASTEXEC_DAEMON` | `off` | Set `on` to share tasks across sessions in one namespace and keep them running after disconnect. |
| `FASTEXEC_CODEMODE` | `false` | Set `true` to expose `codemode` for async JavaScript tool workflows. |
| `FASTEXEC_TRANSFER` | `false` | Set `true` to publish files through `/t/<id>` capability URLs. Treat each URL as a bearer credential. |
| `FASTEXEC_PUBLIC_URL` | unset | Set the external base URL (such as `https://example.com/mac/`) when `FASTEXEC_TRANSFER=true` runs behind a reverse proxy or on `0.0.0.0` / `[::]`. |
| `FASTEXEC_TRANSFER_ADDR` | `127.0.0.1:0` | Set the standalone `/t/` listener address outside `--http` daemon mode. |
| `FASTEXEC_YOLO` | `false` | Set `true` to mark exposed tools read-only in MCP annotations without changing runtime permissions. |
| `FASTEXEC_IMAGE_COMPRESS` | `false` | Set `true` to downscale and re-encode compressible images for size-constrained hosts. |
| `FASTEXEC_MAX_MEDIA_BYTES` | `104857600` | Set the maximum raw size per media file or attachment (`1 MiB` to `360 MiB`). |

Direct MCP tool calls return text and media content blocks without
`structuredContent`. For stdio, set environment variables on the client server
entry. For HTTP, set them in the daemon environment and restart the daemon.

### Instructions

Set `FASTEXEC_INSTRUCTIONS_FILE` to append a UTF-8 file (up to `16 KiB`) at
startup. Choose `FASTEXEC_INSTRUCTIONS_TOOL` based on what your client displays:

| Value | Protocol `instructions` | `instructions` tool description and result |
| --- | --- | --- |
| `off` (default) | Built-in guidance plus file text | Tool absent |
| `move` | Built-in guidance plus a pointer | File text only |
| `copy` | Built-in guidance plus file text | Same full text |

With no extra file text, `move` sends the built-in guidance on both surfaces.
Do not place secrets in the instructions file.

### JavaScript tasks

Pass an async function body as `codemode`'s `code`:

```javascript
const [run, file, shot] = await Promise.all([
  tools.exec({action: "start", command: "printf ready"}),
  tools.read({path: "README.md", limit: 5}),
  tools.read({path: "shot.png"})
]);
text(run.output);
text(file.text);
attach(shot);
```

Nested `tools.exec`, `tools.read`, `tools.write`, and `tools.edit` calls return
typed JavaScript objects directly, keeping `output` and `text` free of status
lines, footers, or inline truncation markers; diagnostics appear in explicit
fields (`truncated`, `omittedRange`, `cutLines`, `nextOffset`, `warnings`).
Failed tool calls reject with `ToolError` (`VALIDATION`, `NOT_FOUND`,
`OVERLOAD`, `LIMIT`, `CANCELLED`, `EDIT_FAILED`, or `IO`) and partial `result`
data when available.

`attach` adds a media file, `read` descriptor, inline media block, or nested
task attachment to the outer response. Poll or kill the script with `exec`;
killing a script cancels pending nested calls while leaving already-started
Bash tasks running. `exec` with `action: "list"` folds child tasks under their
script and supports `parentTaskId` (with optional `state: "active"` or
`"finished"`) to list children flat. See the
[Codemode tool contract](src/description-codemode.md) for arguments and limits.

### Shared daemon, remote HTTP, and events

Stdio tasks end with the session unless `FASTEXEC_DAEMON=on`. Clients with the
same `FASTEXEC_NAMESPACE` share one daemon. Manage it with
`fastexec daemon start`, `fastexec daemon status`, and `fastexec daemon stop`.
Stop an existing daemon before changing its startup settings.

For Streamable HTTP, set `FASTEXEC_HTTP_TOKEN` (one or more whitespace-separated
tokens, each at least `16` visible ASCII characters) and run:

```bash
fastexec daemon run --http 127.0.0.1:8765
```

Point clients to `/mcp` with `Authorization: Bearer <token>`. Behind a
TLS-terminating reverse proxy or gateway that forwards the public `Host`, set
`FASTEXEC_HTTP_ALLOW_REMOTE=1` even on a loopback bind. With `FASTEXEC_TRANSFER=true` behind a reverse proxy, also set
`FASTEXEC_PUBLIC_URL` (for example `https://example.com/mac/`) and forward
`/t/` alongside `/mcp` so minted capability URLs use the external origin and
path prefix. Stopping the HTTP daemon ends its tasks.

On MCP `2026-07-28`, clients can list, poll, and stream `task.exited` and
`external.event` via `events/list`, `events/poll`, and `events/stream`. Inject
external SEP-3415 events (up to `64 KiB` each; required `eventId`, `name`,
`data`, optional `timestamp`, plus `source` and optional `topic` in JSON or via
flags) into a running daemon with `fastexec daemon emit`:

```bash
printf '%s\n' '{"eventId":"evt-1","name":"build.finished","timestamp":"2026-11-25T18:00:00Z","data":{"ok":true}}' \
  | fastexec daemon emit --namespace default --source ci --topic builds --stream
```

Omit `--stream` to send a single JSON object from `stdin`, or keep `--stream`
to pipe newline-delimited JSON events (such as `mcpcli events stream` output).
See [service setup](docs/fastexec-design.md#run-http-under-a-service-manager),
[proxy setup](docs/fastexec-design.md#behind-a-proxy),
[daemon behavior](docs/fastexec-design.md#6-daemon-mode-fastexec_daemonon), and
[MCP Events](docs/fastexec-design.md#7-mcp-events-and-daemon-emit).

## Reference and testing

The [tool description](src/description.md) lists `exec` parameters, and the
[design reference](docs/fastexec-design.md) documents tool semantics, memory
budgets, platform process trees, daemon IPC, and MCP Events. Run `cargo test`
for local tests. On Windows, run `cargo test --no-run` and execute daemon test
binaries from a shell outside any Windows Job.

Licensed under [Apache-2.0](LICENSE). Derived code, key-encoding credits,
QuickJS-NG, and bundled library notices are listed in [NOTICE](NOTICE).
Windows builds embed [Microsoft ConPTY](vendor/conpty/README.md) under the MIT
license.
