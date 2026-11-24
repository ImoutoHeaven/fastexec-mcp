# fastexec

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="assets/fastexec-dark.svg">
  <img src="assets/fastexec.svg" alt="fastexec" width="560">
</picture>

`fastexec` is an MCP server for bash tasks and host files. Run commands with
`exec`, or use `read`, `write`, and `edit` for files. Tasks run with the
server account's permissions, without a sandbox.

Clients must support MCP `2026-07-28`, including `server/discover` and
per-request metadata. See the [protocol contract](docs/fastexec-design.md#mcp-protocol).

## Quick start

Install GNU bash on Linux, macOS, or FreeBSD, or
[Git for Windows](https://git-scm.com/downloads) on Windows. Linux requires a
5.3 or newer kernel, readable `/proc`, and access to the pidfd syscalls.
Set `FASTEXEC_BASH` to an absolute path if Bash is not found.

Download the matching archive from
[Releases](https://github.com/ImoutoHeaven/fastexec-mcp/releases), verify it
against `SHA256SUMS`, then extract `fastexec` (`fastexec.exe` on Windows).
Linux, macOS, and Windows have amd64 and arm64 archives; FreeBSD has amd64.
See [platform notes](docs/fastexec-design.md#platform-notes) for package and
build requirements. To build from source:

```bash
cargo build --release
```

Register the absolute binary path (built binaries are under `target/release`).
For Pi, use `~/.pi/agent/mcp.json`:

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

In Pi the command tool is `mcp__fastexec_local__exec`. Start a bash task with
these MCP tool arguments:

```json
{"action": "start", "command": "printf 'ready\\n'; sleep 5; printf 'done\\n'", "waitMs": 1000}
```

If it is still running, poll with the returned `taskId`:

```json
{"action": "poll", "taskId": "<returned taskId>", "waitMs": 10000}
```

Use the returned `taskId` to keep your output cursor. A wait timeout or a
cancelled request does not stop the task. Use `killAfterMs` to set its
deadline, or send `action: "kill"` to stop it. Each start runs a fresh Bash:
`cd` and exports do not carry over. Use pipes for input, or `pty: true` with
`keys` for interactive terminals.

Hosts without progress-aware timeouts need at least 300 seconds per tool
call. Hosts may log MCP arguments, and task logs and PTY transcripts can
record sensitive input. Avoid putting secrets in arguments or typing them
into a recorded terminal. After an unconfirmed input write or an oversized
Codemode result, check what happened before retrying.

## Choose how to run

| Setting | Default | When to change it |
| --- | --- | --- |
| `FASTEXEC_DAEMON` | `off` | Set `on` to share tasks across sessions in one namespace and keep them running after disconnect. |
| `FASTEXEC_CODEMODE` | `false` | Set `true` to run a JavaScript script that calls tools. |
| `FASTEXEC_TRANSFER` | `false` | Set `true` to publish files through capability URLs. Treat each URL as a credential. |
| `FASTEXEC_TRANSFER_ADDR` | `127.0.0.1:0` | Set the transfer listener address outside HTTP daemon mode. |
| `FASTEXEC_YOLO` | `false` | Override hints for all exposed tools. Does not restrict what tools can do. |
| `FASTEXEC_IMAGE_COMPRESS` | `false` | Set `true` to scale and re-encode compressible images for size-constrained hosts. |
| `FASTEXEC_MAX_MEDIA_BYTES` | `104857600` | Set the maximum raw size per media file or attachment (`1 MiB` to `360 MiB`). |
| `FASTEXEC_STRUCTURED_CONTENT` | `true` | Set `false` to omit structured metadata. |
| `FASTEXEC_OUTPUT_MODE` | `text` | Use `both` or `structured` to include the output body in structured results. |

For stdio, set environment variables on the client entry. For HTTP, set them
on the daemon and restart it.

### Instructions

Set `FASTEXEC_INSTRUCTIONS_FILE` to append a UTF-8 file (up to 16 KiB) at
startup. Choose `FASTEXEC_INSTRUCTIONS_TOOL` based on what your client shows:

| Value | Protocol instructions | `instructions` tool description and result |
| --- | --- | --- |
| `off` (default) | Built-in guidance plus file text | Tool absent |
| `move` | Built-in guidance plus a pointer | File text only |
| `copy` | Built-in guidance plus file text | Same full text |

With no extra text, `move` sends the built-in guidance on both surfaces.
Clients that list tools can receive the description without calling it. Do
not put secrets in the file.

### JavaScript tasks

Pass the body of an async function as `codemode`'s `code`:

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

`attach` adds a media file, `read` descriptor, inline media block, or nested
task attachment to the result. Poll or stop the script with `exec`. Stopping
the script cancels pending tool calls and leaves Bash tasks it already started
running. Set `killAfterMs` to bound the script itself. See the
[Codemode tool contract](src/description-codemode.md) for arguments and limits.

`exec` with `action: "list"` groups child tasks under their script. Script
rows summarize their children and include up to eight active children. To list
all of a script's children, pass its task ID as `parentTaskId`; add
`state: "active"` or `state: "finished"` to filter them.

### Shared daemon and remote HTTP

Stdio tasks end with the session unless you set `FASTEXEC_DAEMON=on`. Set the
same `FASTEXEC_NAMESPACE` on clients that should share a daemon. Use
`fastexec daemon start`, `fastexec daemon status`, and `fastexec daemon stop`
to manage it. Stop the existing daemon before changing its startup settings.

For HTTP, set `FASTEXEC_HTTP_TOKEN` in the server's environment. Each token
needs at least 16 visible ASCII characters. Run:

```bash
fastexec daemon run --http 127.0.0.1:8765
```

Point clients to `/mcp` and send `Authorization: Bearer <token>` on every
request. Use a TLS-terminating proxy for remote access. A proxy forwarding the
public Host requires `FASTEXEC_HTTP_ALLOW_REMOTE=1`, even with a loopback bind.
Stopping the HTTP daemon stops its tasks.

See [service setup](docs/fastexec-design.md#run-http-under-a-service-manager),
[proxy setup](docs/fastexec-design.md#behind-a-proxy), and
[daemon behavior](docs/fastexec-design.md#6-daemon-mode-fastexec_daemonon).

## Reference and testing

The [tool description](src/description.md) lists `exec` arguments.
The [design reference](docs/fastexec-design.md) covers tool behavior, output,
platform differences, and daemon limits. Run `cargo test` for local tests.
On Windows, use `cargo test --no-run`, then run daemon test executables from a
shell outside any Windows Job.

Licensed under [Apache-2.0](LICENSE). Derived parts from
[FastCtx](https://github.com/yc-duan/fastctx) and key-encoding credits are
listed in [NOTICE](NOTICE). Windows builds embed
[Microsoft ConPTY](vendor/conpty/README.md) under the MIT license.
