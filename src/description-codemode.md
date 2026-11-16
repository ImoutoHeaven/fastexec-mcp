Run JavaScript that calls this server's exec, read, write, and edit tools in-process, as a task.
Prefer this over multi-turn exec or read calls when aggregating across multiple files, looping or
branching on tool results, or filtering bulky output before returning it to context.

- code is the body of an async function: top-level await and return work. Pass cwd (an existing
  absolute directory) to set the default working directory for nested exec starts and relative
  paths in read, write, and edit. There are no modules, Node APIs, timers, file system, or
  network; the script reaches the host only through tools.
- tools.exec(args), tools.read(args), tools.write(args), and tools.edit(args) take the same
  arguments as those tools and resolve to { content, structuredContent, isError }, including
  parameter validation and operational failures (isError: true, structuredContent.ok: false);
  only malformed argument schemas or unknown tools reject with TypeError, and concurrency or
  bridge limits reject with RangeError. Await concurrent calls with Promise.all or
  Promise.allSettled so an early rejection is never left unhandled across another await.
- Nested calls always include structuredContent: exec start, poll, and transcript place clean
  output without the status line in structuredContent.output; text reads place clean file text
  without truncation or UTF-8 notices in structuredContent.text alongside truncated, nextOffset,
  startLine, endLine, shownLines, totalLines, lineTooLong, and lossy; image reads put an image
  block { type: "image", data, mimeType } in content and metadata in structuredContent; write
  sets { ok, path, bytes } and edit sets { ok, path, replacements, edits }.
- text(value) appends a line: strings as they are, other values as JSON. console.log, info,
  warn, error, and debug append their arguments joined by spaces. Lines reach the task log as
  they are written; a returned value is appended last; exit() ends the script successfully.
- image(block, data URL, or a result holding an image block) saves the image beside the log and
  appends an [image N: path] line. The result that reads past that line carries the image, at
  most 4 images and 4718592 base64 bytes per result; the others are listed in warnings with
  their paths for read.
- The call waits like exec start (cwd, waitMs, returnWhen with outputContains or quietMs,
  killAfterMs, truncate, maxBytes, raw) and returns a task handle: continue with exec poll, stop
  with exec kill. An uncaught error ends the task with exit 1 and a "Script error:" line.
- When the script ends, calls it did not await are cancelled. Stopping a script (exec kill,
  killAfterMs, or the end of the tasks' owner) cancels only the calls it is awaiting. Calls that
  already ran are not undone, and tasks it started with exec start keep running; the result
  lists them, and exec kill stops each one.
- Limits: code up to 256 KiB, 128 MiB of script memory, 8 nested calls in flight (1024 queued),
  128 MiB of pending tool arguments and results, 64 MiB of output including diagnostic space,
  64 images and 4096 validated exec start calls per script (failed or cancelled starts count),
  and 4 running scripts (8 with FASTEXEC_DAEMON=on or HTTP).
  If a completed result cannot fit the pending-result budget, the call rejects with RangeError
  after the operation ran; inspect its effects before retrying.
