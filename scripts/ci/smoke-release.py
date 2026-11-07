#!/usr/bin/env python3
"""Exercise a packaged Unix binary through MCP and its shared daemon."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import queue
import subprocess
import tempfile
import threading
import time
import uuid


class Client:
    def __init__(self, binary, env, version):
        self.process = subprocess.Popen(
            [str(binary)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=None, text=True, encoding="utf-8", env=env,
        )
        self.messages = queue.Queue()
        self.next_id = 0
        threading.Thread(target=self.read, daemon=True).start()
        try:
            initialized = self.request("initialize", {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "release-smoke", "version": "1"},
            })
            assert initialized["serverInfo"]["name"] == "fastexec", initialized
            assert initialized["serverInfo"]["version"] == version, initialized
            self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        except BaseException:
            self.close()
            raise

    def read(self):
        try:
            for line in self.process.stdout:
                self.messages.put(json.loads(line))
        except BaseException as error:
            self.messages.put(error)
        finally:
            self.messages.put(EOFError("fastexec closed stdout"))
            self.process.stdout.close()

    def send(self, message):
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def request(self, method, params):
        self.next_id += 1
        self.send({"jsonrpc": "2.0", "id": self.next_id, "method": method, "params": params})
        deadline = time.monotonic() + 30
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"fastexec did not answer {method} within 30 seconds")
            message = self.messages.get(timeout=remaining)
            if isinstance(message, BaseException):
                raise message
            if message.get("id") == self.next_id:
                assert "error" not in message, message
                return message["result"]

    def tool(self, name, arguments):
        result = self.request("tools/call", {"name": name, "arguments": arguments})
        assert not result.get("isError", False), result
        text = "\n".join(item.get("text", "") for item in result.get("content", []))
        return result.get("structuredContent"), text

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:
            self.process.stdin.close()
        try:
            status = self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=10)
            raise AssertionError("fastexec did not stop after stdin closed")
        assert status == 0, f"fastexec exited {status}"


def finished(result, text, marker):
    assert result["state"] == "exited" and result["exitCode"] == 0, (result, text)
    assert marker in text, (result, text)


def smoke(binary, version):
    with tempfile.TemporaryDirectory(prefix="fastexec-release-") as directory:
        env = {key: value for key, value in os.environ.items() if not key.startswith("FASTEXEC_")}
        env.update({
            "FASTEXEC_DAEMON": "off",
            "FASTEXEC_NAMESPACE": "release-smoke-" + uuid.uuid4().hex,
            "XDG_RUNTIME_DIR": directory,
        })
        with contextlib.closing(Client(binary, env, version)) as client:
            tools = client.request("tools/list", {})["tools"]
            assert {tool["name"] for tool in tools} == {"exec", "read", "write", "edit"}, tools
            path = str(Path(directory) / "probe.txt")
            client.tool("write", {"path": path, "content": "release:before\n"})
            client.tool("edit", {"path": path, "edits": [
                {"oldText": "release:before", "newText": "release:after"},
            ]})
            _, text = client.tool("read", {"path": path})
            assert text == "release:after\n", text
            assert Path(path).read_text() == "release:after\n"
            result, text = client.tool("exec", {
                "action": "start", "command": "printf '%s\\n' RELEASE_PIPE_OK",
                "cwd": directory, "loginShell": False, "waitMs": 10000, "killAfterMs": 20000,
            })
            finished(result, text, "RELEASE_PIPE_OK")

            result, text = client.tool("exec", {
                "action": "start", "pty": True, "cols": 80, "rows": 24,
                "command": "stty -echo; stty size; IFS= read -r line; stty size; printf 'PTY:%s\\n' \"$line\"",
                "cwd": directory, "loginShell": False, "waitMs": 10000, "killAfterMs": 20000,
                "returnWhen": {"outputContains": ["24 80"]},
            })
            assert result["state"] == "running" and "24 80" in text, (result, text)
            result, text = client.tool("exec", {
                "action": "poll", "taskId": result["taskId"], "cols": 100, "rows": 40,
                "input": "ready", "keys": ["Enter"], "keyDelayMs": 0, "waitMs": 10000,
            })
            finished(result, text, "PTY:ready")
            assert "40 100" in text, text
        print("PASS: initialize, tools/list, files, pipe exec, PTY input and resize", flush=True)

        def daemon(action):
            result = subprocess.run(
                [str(binary), "daemon", action], env=env, text=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30,
            )
            assert result.returncode == 0, (action, result.stdout, result.stderr)
            return result.stdout

        try:
            daemon("start")
            assert "epoch " in daemon("status")
            env["FASTEXEC_DAEMON"] = "on"
            with contextlib.closing(Client(binary, env, version)) as first:
                result, text = first.tool("exec", {
                    "action": "start", "command": "printf DAEMON_READY; IFS= read -r line; printf 'DAEMON:%s\\n' \"$line\"",
                    "cwd": directory, "loginShell": False, "waitMs": 10000, "killAfterMs": 30000,
                    "returnWhen": {"outputContains": ["DAEMON_READY"]},
                })
                assert result["state"] == "running" and "DAEMON_READY" in text, (result, text)
                task = result["taskId"]
            with contextlib.closing(Client(binary, env, version)) as second:
                listed, _ = second.tool("exec", {"action": "list"})
                assert any(item["taskId"] == task for item in listed["tasks"]), listed
                result, text = second.tool("exec", {
                    "action": "poll", "taskId": task, "input": "again\n", "waitMs": 10000,
                })
                finished(result, text, "DAEMON:again")
        finally:
            daemon("stop")
        assert daemon("status").startswith("No fastexec daemon"), "daemon still running"
        print("PASS: daemon start/status, task across sessions, stdin and stop", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("version")
    options = parser.parse_args()
    smoke(options.binary.resolve(strict=True), options.version)
