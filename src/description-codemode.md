Run JavaScript that calls this server's exec, read, write, and edit tools in-process, as a task.
Prefer this over multi-turn exec or read calls when aggregating across multiple files, looping or
branching on tool results, or filtering bulky output before returning it to context.

- code is the body of an async function: top-level await and return work. cwd sets the directory
  for nested exec starts and relative paths in read, write, and edit. sleep(ms) pauses without a
  process; TextEncoder, TextDecoder, atob, and btoa are available. There are no modules, Node
  APIs, callback timers, file system, or network.
- tools.exec/read/write/edit(args) take those tools' arguments and resolve directly to typed
  objects: exec { action, taskId, kind, state, exitCode, pty, elapsedMs, output, truncated,
  logPath, ... }, text read { kind: "text", path, text, startLine, endLine, shownLines, totalLines,
  truncated, nextOffset?, ... }, write { path, bytes }, edit { path, replacements, edits }.
  output and text are raw (no status lines, footers, line prefixes, or inline omission/cut
  markers); truncation and diagnostics live in explicit fields. Non-zero exitCode resolves. Failed
  calls reject with ToolError carrying tool, code ("VALIDATION", "NOT_FOUND", "OVERLOAD", "LIMIT",
  "CANCELLED", "EDIT_FAILED", "IO"), message, and partial result when available (full exec report
  on IO errors, { path, failures } on EDIT_FAILED, { taskId, logPath } on OVERLOAD after a task
  ran). Wrong argument shapes reject with TypeError. Use Promise.all or Promise.allSettled for
  concurrency.
- Nested exec defaults to truncate "head" (transcript: "tail") and maxBytes 4194304 (4 MiB), keeps
  NUL bytes in clean output, and rejects truncate "head_tail" or list maxBytes/truncate
  (VALIDATION). raw: true preserves ANSI/control sequences and line endings; output is still
  decoded using encoding (UTF-8 by default). Invalid sequences become replacement characters,
  and encodingErrors counts affected lines. The task log holds captured bytes.
  truncated, omittedLines, omittedRange, cutLines, and
  omittedRows report overflow (read logPath for the rest). Without waitMs, start and poll wait
  until exit; resize/input/keys waits 2000 ms, and returnWhen waits 30000 ms. Drive a PTY by
  awaiting start with pty: true and returnWhen, then poll with input and keys. Nested exec list
  returns { action: "list", totalCount, tasks }, folding codemode children under their parent
  unless parentTaskId is passed.
- tools.read({ path }) returns a media descriptor { kind, path, mimeType, bytes, attachable:
  true } (or { ..., sourceBytes?, attachable: false, note }) for images, audio, PDF, and video
  without loading base64 into JS. For text without offset or limit, whole-file mode reads up to
  4 MiB of strict UTF-8 or rejects with ToolError (LIMIT if larger, IO if non-UTF-8); offset or
  limit reads a 4 MiB slice.
- sh`...` builds bash commands with raw template text (keeping backslashes), quotes scalars with
  sh.quote(value), and expands arrays with sh.join(values); do not quote ${...}. For multiline
  interpreter scripts (python3, awk, jq), pass command: "python3 -" with input: String.raw`...`.
- text(value) appends strings verbatim and other values as JSON; console.log/info/warn/error/debug
  join args with spaces. A non-undefined return value must be a string or JSON-serializable and
  appends last; exit() ends with 0. Output fits maxBytes (default 16384); logPath keeps the rest.
- attach(value) saves a file path, read media descriptor (attachable: false throws TypeError with
  note), opaque handle from nested poll/kill codemode.attachments, MCP image/audio/resource(blob)
  block, or base64 data URL as 1-based attachment #index. Outer MCP responses return text and media
  blocks (no structuredContent), pack attachments in index order with Attached: #index in the text
  summary, and leave overflow for inputless poll; reattach with bare taskId and deduplicate by
  index if delivery is uncertain.
- Waits like exec start (waitMs, returnWhen outputContains/quietMs, killAfterMs, truncate,
  maxBytes) and returns a task handle for poll/kill. Uncaught errors exit 1 with "Script error:".
- Ending cancels unawaited calls. Killing cancels pending calls: completed calls stand, started
  tasks keep running.
- Limits: 256 KiB code, 256 MiB VM memory, 8 exec starts and 8 file/transcript I/O calls in
  flight (poll/kill/list/sleep take no slot), 1024 pending calls, 256 MiB pending bridge data, 64
  MiB text output, 1024 attachments per script (4 GiB task store), 4096 starts per script, 4
  running scripts (8 in daemon/HTTP). Pending-call, start-count, and bridge limits reject with
  ToolError ("LIMIT" or "OVERLOAD"); oversized results reject after running, so check effects
  before retrying. attach() count/disk quotas throw RangeError.
