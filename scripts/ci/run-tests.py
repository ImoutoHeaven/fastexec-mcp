#!/usr/bin/env python3
"""Build the test suite and run every test executable serially, as the job's test log.

`cargo test` puts its children in a Job on Windows, and the daemon cases must run outside one,
so the compiler-artifact messages of `cargo test --no-run --message-format=json` name the
executables the suite emitted and each runs directly here: one at a time, each with a deadline,
all of them even after one fails, so one log names every failure. Ignored tests are listed with
the reason their attribute states, and `--note` states what a platform does not compile at all.

    scripts/ci/run-tests.py [--timeout SECONDS] [--only STEM] [--sudo] [--env KEY=VALUE]
                            [--assert-outside-job] [--note TEXT] [-- TEST ARGS...]
"""

import argparse
import ctypes
import json
import os
import re
import signal
import subprocess
import sys
import time

LOG = "ci-test.log"
CREATE_BREAKAWAY_FROM_JOB = 0x01000000
# The re-run outside a Job marks itself, so a Job that refuses breakaway fails instead of recursing.
BREAKAWAY = "FASTEXEC_CI_BREAKAWAY"


def say(line, handle):
    print(line, flush=True)
    handle.write(line + "\n")
    handle.flush()


def test_executables(handle):
    """Every executable of a `profile.test` artifact the suite built."""
    command = ["cargo", "test", "--locked", "--no-run", "--message-format=json"]
    say(f"$ {' '.join(command)}", handle)
    build = subprocess.run(command, stdout=subprocess.PIPE, text=True, errors="replace")
    if build.returncode != 0:
        say("FAIL: the test suite does not build", handle)
        sys.exit(1)
    executables = []
    for line in build.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if (
            message.get("reason") == "compiler-artifact"
            and (message.get("profile") or {}).get("test") is True
            and message.get("executable")
            and message["executable"] not in executables
        ):
            executables.append(message["executable"])
    if not executables:
        say("FAIL: no compiler-artifact message carries profile.test", handle)
        sys.exit(1)
    return executables


def run(executable, args, timeout, env, sudo, log_path, handle):
    """One test executable with a deadline; its whole process tree dies with it."""
    if sudo:
        # sudo resets the environment, so every value the executable needs travels with it.
        command = ["sudo", "env"] + [f"{key}={value}" for key, value in env.items()]
        child_env = None
    else:
        command = []
        child_env = dict(os.environ, **env)
    command += [executable] + args
    say(f"$ {' '.join(command)}", handle)
    started = time.time()
    with open(log_path, "w", errors="replace") as output:
        process = subprocess.Popen(
            command,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=child_env,
            start_new_session=(os.name != "nt"),
            creationflags=0x00000200 if os.name == "nt" else 0,  # CREATE_NEW_PROCESS_GROUP
        )
        try:
            status = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            status = None
            if os.name == "nt":
                subprocess.run(["taskkill", "/F", "/T", "/PID", str(process.pid)],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            elif sudo:
                # A sudo child is root's: only root may signal its process group.
                subprocess.run(["sudo", "kill", "-KILL", "--", f"-{process.pid}"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            else:
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except OSError:
                    pass
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
    with open(log_path, errors="replace") as output:
        say(output.read().rstrip("\n"), handle)
    say(f"exit {status} after {time.time() - started:.1f}s", handle)
    return status


def inside_job():
    """Whether this process, and so every child, is in a Job."""
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.GetCurrentProcess.restype = ctypes.c_void_p
    kernel32.IsProcessInJob.argtypes = [
        ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)
    ]
    inside = ctypes.c_int(0)
    if not kernel32.IsProcessInJob(kernel32.GetCurrentProcess(), None, ctypes.byref(inside)):
        raise OSError(ctypes.get_last_error(), "IsProcessInJob")
    return bool(inside.value)


def outside_job(handle):
    """Run the suite outside the Job CI's shell sits in, or refuse when it cannot leave."""
    if os.name != "nt":
        say("outside-Job: not a Windows host", handle)
        return
    if not inside_job():
        say("outside-Job: this shell and its children are in no Job", handle)
        return
    if os.environ.get(BREAKAWAY) == "1":
        say("FAIL: still inside a Job after breaking away; the daemon cases need one outside",
            handle)
        sys.exit(1)
    say("this shell is inside a Job; re-running the suite outside it", handle)
    try:
        child = subprocess.Popen(
            [sys.executable, *sys.argv],
            env=dict(os.environ, **{BREAKAWAY: "1"}),
            creationflags=CREATE_BREAKAWAY_FROM_JOB,
        )
    except OSError as error:
        say(f"FAIL: this shell is inside a Job that does not allow breakaway ({error})", handle)
        sys.exit(1)
    sys.exit(child.wait())


def stem_of(path):
    """`mcp-1a2b3c4d` from `.../deps/mcp-1a2b3c4d.exe`."""
    name = os.path.basename(path)
    base, extension = os.path.splitext(name)
    return base if extension in (".exe", ".dylib", ".so") else name


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=1200, help="deadline per executable")
    parser.add_argument("--only", action="append", default=[], help="run only this name stem")
    parser.add_argument("--sudo", action="store_true", help="run each executable as root")
    parser.add_argument("--env", action="append", default=[], help="KEY=VALUE per executable")
    parser.add_argument("--assert-outside-job", action="store_true",
                        help="fail when this process is inside a Job")
    parser.add_argument("--note", action="append", default=[],
                        help="a line the log carries for what this platform does not run")
    parser.add_argument("tests", nargs="*", help="arguments for every test executable")
    options = parser.parse_args(sys.argv[1:])
    if "--" in sys.argv:
        options.tests = sys.argv[sys.argv.index("--") + 1:]

    with open(LOG, "a", errors="replace") as handle:
        say(f"=== run-tests.py: {sys.platform} {' '.join(sys.argv[1:])}", handle)
        for note in options.note:
            say(f"not run here: {note}", handle)
        if options.assert_outside_job:
            outside_job(handle)
        every = test_executables(handle)
        chosen = [path for path in every
                  if not options.only or any(stem_of(path).startswith(stem)
                                             for stem in options.only)]
        if options.only and not chosen:
            say(f"FAIL: no test executable named {', '.join(options.only)}", handle)
            sys.exit(1)
        env = dict(item.split("=", 1) for item in options.env)
        logs = {path: f"ci-{stem_of(path)}.log" for path in chosen}
        failures = []
        for path in chosen:
            status = run(path, options.tests, options.timeout, env, options.sudo,
                         logs[path], handle)
            if status is None:
                failures.append(f"{os.path.basename(path)} ran past {options.timeout}s")
            elif status != 0:
                failures.append(f"{os.path.basename(path)} exit {status}")
        named = []
        for path, log_path in logs.items():
            with open(log_path, errors="replace") as log:
                named += [line.strip() for line in log
                          if re.search(r"\btest \S+ \.\.\. ignored\b", line)]
        say(f"ignored tests on {sys.platform}: {len(named)}", handle)
        for line in named:
            say(f"  {line}", handle)
        if failures:
            say(f"FAIL: {'; '.join(failures)}", handle)
            sys.exit(1)
        say("PASS", handle)


if __name__ == "__main__":
    main()
