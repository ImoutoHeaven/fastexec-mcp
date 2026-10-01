Run bash commands as tasks: Git Bash on Windows, system bash elsewhere. Write POSIX bash.
- start: runs `command` in a fresh bash and waits up to waitMs (default 30000). If it exits in time you get the final result; otherwise a taskId and the output so far while it keeps running. A non-zero exit code is a normal result. Every start is a new shell: cd and exports do not carry over, so pass cwd (default: the server's working directory) or chain with &&.
- poll: writes `input` exactly as given, closes stdin when eof is true, waits until the task ends or waitMs elapses (default 2000 with input, 30000 without), and returns only output you have not seen. Use long waits (up to 240000) for builds and tests, short ones for prompts; avoid repeated waitMs 0 polls and sleep commands. The process keeps running past every wait.
- kill: ends the task's whole process tree; poll afterwards for remaining output. When the root bash exits, the rest of its tree ends too, so run long-lived servers as their own task.
- list: this server's tasks. All tasks end when the session ends.

Output: stdout and stderr form one stream. ANSI codes are stripped and progress-bar rewrites collapse to their final text (raw: true keeps them). Results fit maxBytes (default 16384) with truncate head_tail (default), head, or tail, which also cut lines over 2000 chars (64 KiB with raw) and count them in cutLines; none returns every unseen line whole and leaves limits to the host. Narrow output in the command (cmd 2>&1 | grep ... | tail -n 50, with set -o pipefail). The log file in the status line keeps up to 64 MiB; read omitted lines with sed -n 'A,Bp' or grep.

stdin: in pipe mode stdin stays open until eof or exit, so programs reading stdin wait; pass eof: true or use < /dev/null.

PTY (pty: true): for programs that need a terminal, including password prompts such as ssh. Send a carriage return (JSON "\r") for Enter, "\u0003" for Ctrl-C, "\u0004" for Ctrl-D; these are control characters, not backslash text. Avoid full-screen programs (vim, top, less). Input, passwords included, is recorded in the transcript.

Windows: Git Bash rewrites arguments that look like POSIX paths (/c, /F, /tmp/x) for native programs; prefix MSYS_NO_PATHCONV=1 or set MSYS2_ARG_CONV_EXCL. Run PowerShell from a script file (pwsh -NoProfile -NonInteractive -File x.ps1) or with -EncodedCommand. loginShell: false skips the profile and starts faster.

U+FFFD in output means another encoding: pass encoding ("big5", "gbk").
