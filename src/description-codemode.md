Run JavaScript that calls this server's exec, read, write, and edit tools in-process, as a task.
Prefer this over multi-turn exec or read calls when aggregating across multiple files, looping or
branching on tool results, or filtering bulky output before returning it to context.

- code is the body of an async function: top-level await and return work. cwd sets the directory
  for nested exec starts and relative paths in read, write, and edit. sleep(ms) pauses without a
  process; TextEncoder, TextDecoder, atob, and btoa are available. There are no modules, Node
  APIs, callback timers, file system, or network.
- tools.exec/read/write/edit(args) take those tools' arguments and resolve to the report:
  exec { taskId, state, exitCode, output, ... }, text read { path, text, totalLines,
  nextOffset, ... }, write { path, bytes }, edit { path, replacements, edits }. Non-zero
  exitCode resolves. Failed operations (missing file, unmatched or ambiguous edit, invalid value,
  failed start, lost input or output) reject with ToolError (result holds the report). Wrong
  argument shapes reject with TypeError, and concurrency or bridge limits with RangeError. Await
  concurrent calls with Promise.all or Promise.allSettled.
- Nested exec defaults to truncate "head" (transcript: "tail") and maxBytes 4194304 (4 MiB), has
  no per-line cap, and keeps NUL bytes in clean output (find -print0, rg -0). cutLines and
  omittedRange report unseen overflow (read it from logPath). Without waitMs, start and poll have
  no deadline; resize, input, or keys waits 2000 ms, and returnWhen defaults waitMs to 30000 ms.
  Drive a PTY in one turn by awaiting start with pty: true and returnWhen, then poll with input
  and keys.
- tools.read({ path }) returns a media descriptor { kind, path, mimeType, bytes, attachable:
  true } (or { kind, path, mimeType, sourceBytes?, attachable: false, note }) for images, audio,
  PDF, and video without loading base64 into JS. For UTF-8 text without offset or limit it returns
  the whole file up to 4 MiB, or rejects with ToolError if larger or non-UTF-8; passing offset or
  limit reads a 4 MiB slice.
- sh`...` builds bash commands using raw template text (keeping backslashes), quotes scalar
  interpolations with sh.quote(value), and expands arrays with sh.join(values); do not wrap
  ${...} in quotes. Call sh.quote or sh.join directly when needed. For multiline interpreter
  scripts (python3, awk, jq), pass command: "python3 -" with input: String.raw`...` instead of
  nested quotes or heredocs.
- text(value) appends strings verbatim and other values as JSON. console.log/info/warn/error/debug
  join arguments with spaces. A non-undefined return value must be a string or JSON-serializable
  and is appended last; exit() ends with 0. Output fits maxBytes (default 16384); logPath keeps
  the rest.
- attach(value) saves a file path, read media descriptor (attachable: false throws TypeError with
  note), opaque handle from nested poll/kill codemode.attachments, MCP image/audio/resource(blob)
  block, or base64 data URL as 1-based attachment #index. Responses pack attachments in index
  order (listed in codemode.attachmentsDelivered or Attached: #index) and leave overflow for
  inputless poll; reattach with bare taskId and deduplicate by index if delivery is uncertain.
- Waits like exec start (waitMs, returnWhen with outputContains or quietMs, killAfterMs, truncate,
  maxBytes) and returns a task handle for exec poll and kill. Uncaught errors exit 1 with
  "Script error:".
- Ending cancels unawaited calls. Killing cancels pending calls: completed calls stand, and
  started tasks keep running (listed for exec kill).
- Limits: 256 KiB code, 256 MiB VM memory, 8 exec starts and 8 file/transcript I/O calls in
  flight (poll, kill, list, and sleep take no slot), 1024 pending calls, 256 MiB pending bridge
  data, 64 MiB text output, 1024 attachments per script (within the 4 GiB task store), 4096 starts
  per script, and 4 running scripts (8 in daemon or HTTP). Oversized results reject with
  RangeError after running; check effects before retrying.
