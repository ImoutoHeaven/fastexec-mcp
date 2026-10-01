Run bash commands as tasks: Git Bash on Windows, system bash elsewhere. Write POSIX bash.
- start: runs `command` and waits up to waitMs (default 30000). If it exits in time you get the final result; otherwise you get a taskId and the output so far while it keeps running. Continue with poll, stop with kill, find tasks with list. A non-zero exit code is a normal result.
- poll: writes `input` (exactly as given; add \n, or \r in PTY mode, for Enter), closes stdin when eof is true, then waits until the task ends or waitMs elapses (default 2000 with input, 30000 without) and returns the output you have not seen yet. Each wait lasts at most 240000 ms; the process keeps running past it.
- kill: terminates the task's whole process tree. When the root bash exits, the rest of its tree ends too, so run long-lived servers as their own task.

Output: ANSI codes are stripped and progress-bar rewrites collapse to their final text (raw: true keeps them). Each result fits maxBytes (default 16384) with truncate head_tail (default), head, or tail; truncate none returns every unseen line and leaves size limits to the host. Narrow output inside the command (cmd 2>&1 | grep ... | tail -n 50, with set -o pipefail to keep the exit code). Output is also kept in the log file named in the status line (up to 64 MiB per task); read omitted lines with sed -n 'A,Bp' or grep on that file.

stdin: in pipe mode stdin stays open until eof or exit, so programs that read stdin wait; pass eof: true or start the command with < /dev/null.

PTY (pty: true): for programs that need a terminal, including password prompts such as ssh. Send a carriage return (JSON "\r") for Enter, "\u0003" for Ctrl-C, "\u0004" for Ctrl-D; these are control characters, not backslash text. Input is recorded in the transcript like every tool argument.

Windows: Git Bash rewrites arguments that look like POSIX paths (/c, /F, /tmp/x) when launching native programs; prefix MSYS_NO_PATHCONV=1 for one command or set MSYS2_ARG_CONV_EXCL. Run PowerShell from a script file (pwsh -NoProfile -NonInteractive -File x.ps1) or with -EncodedCommand, not inline inside bash quotes.

Garbled text (U+FFFD) means another encoding: pass encoding, e.g. "big5" or "gbk".
