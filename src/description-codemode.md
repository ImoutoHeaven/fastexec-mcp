Run JavaScript that calls this server's exec, read, write, and edit tools in-process, as a task.
Prefer this over multi-turn exec or read calls when aggregating across multiple files, looping or
branching on tool results, or filtering bulky output before returning it to context.

- code is the body of an async function: top-level await and return work. cwd sets the directory
  for nested exec starts and relative paths in read, write, and edit. TextEncoder, TextDecoder,
  atob, and btoa are available; there are no modules, Node APIs, timers, file system, or network.
- tools.exec/read/write/edit(args) take those tools' arguments and resolve to the tool's report:
  exec { taskId, state, exitCode, output, ... }, a text read { path, text, totalLines,
  nextOffset, ... }, write { path, bytes }, edit { path, replacements, edits }. A non-zero
  exitCode resolves. A failed operation (missing file, unmatched or ambiguous edit, invalid value,
  failed start, lost input or output) rejects with an Error named ToolError whose result holds the
  report. Wrong argument shapes reject with TypeError, and concurrency or bridge limits with
  RangeError. Await concurrent calls with Promise.all or Promise.allSettled.
- Nested exec has no per-line limit. Without truncate or maxBytes it returns all unseen output up
  to 4 MiB; cutLines and omittedRange report what did not fit, which a later poll does not return
  (read it from logPath). Without waitMs, start and poll have no deadline; a poll that sends input
  or keys waits 2000 ms, and a call with returnWhen 30000 ms. Text reads return up to 50000 lines
  or 1 MiB per call.
- sh.quote(value) returns value as one single-quoted bash word.
- text(value) appends a line: strings as they are, other values as JSON. console.log, info, warn,
  error, and debug append their arguments joined by spaces. A returned value is appended last;
  exit() ends the script successfully. Script output is shown as written, with no per-line limit;
  maxBytes (default 16384) bounds one result, and the log keeps the rest.
- image(value) takes an attached image read, an image block { type: "image", data, mimeType }, or
  a data URL, saves the image beside the log, and appends an [image N: path] line. A result
  carries at most 4 images and 4718592 base64 bytes and lists the others with their paths.
  Nested exec poll and kill expose attached blocks in codemode.images.
- The call waits like exec start (waitMs, returnWhen with outputContains or quietMs, killAfterMs,
  truncate, maxBytes) and returns a task handle: continue with exec poll, stop with exec kill. An
  uncaught error ends the task with exit 1 and a "Script error:" line.
- When the script ends, calls it did not await are cancelled. Stopping a script cancels only the
  calls it is awaiting: completed calls are not undone, and tasks it started keep running (the
  result lists them; exec kill stops each one).
- Limits: code up to 256 KiB, 128 MiB of script memory, 8 nested calls in flight (1024 queued),
  128 MiB of pending tool arguments and results, 64 MiB of output, 64 images and 4096 exec starts
  per script, and 4 running scripts (8 with FASTEXEC_DAEMON=on or HTTP). A result that cannot fit
  the pending budget rejects with RangeError after the operation ran; inspect its effects before
  retrying.
