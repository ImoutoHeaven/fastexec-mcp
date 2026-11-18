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
- tools.read({ path }) without offset or limit returns the whole UTF-8 file up to 4 MiB, or
  rejects with ToolError if larger or non-UTF-8; passing offset (positive, or negative from the
  end) or limit reads a 4 MiB slice.
- sh.quote(value) quotes value as one bash word.
- text(value) appends strings verbatim and other values as JSON. console.log/info/warn/error/debug
  join arguments with spaces. A non-undefined return value must be a string or JSON-serializable
  and is appended last; exit() ends with 0. Output fits maxBytes (default 16384); logPath keeps
  the rest.
- image(value) saves an attached image read, { type: "image", data, mimeType }, or data URL
  beside the log and appends [image N: path]. Results attach at most 4 images and 4718592 base64
  bytes, listing others with paths; nested poll and kill expose attached blocks in codemode.images.
- Waits like exec start (waitMs, returnWhen with outputContains or quietMs, killAfterMs, truncate,
  maxBytes) and returns a task handle for exec poll and kill. Uncaught errors exit 1 with
  "Script error:".
- Ending cancels unawaited calls. Killing cancels only awaited calls: completed calls stand, and
  started tasks keep running (listed for exec kill).
- Limits: 256 KiB code, 128 MiB VM memory, 8 exec starts and 8 file/transcript I/O calls in
  flight (poll, kill, list, and sleep take no slot), 1024 pending calls, 128 MiB pending bridge
  data, 64 MiB output, 64 images and 4096 starts per script, and 4 running scripts (8 in daemon or
  HTTP). Oversized results reject with RangeError after running; check effects before retrying.
