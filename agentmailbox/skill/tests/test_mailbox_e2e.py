"""End-to-end tests: real Go service over a real Unix socket, two clients.

The Go binary is built once with the same offline settings used by the module
tests. Each test starts its own service in a short private runtime directory
(Unix socket paths on macOS are limited to ~104 bytes).
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SKILL_DIR = os.path.dirname(HERE)
MODULE_ROOT = os.path.dirname(SKILL_DIR)
SCRIPTS_DIR = os.path.join(SKILL_DIR, "scripts")
sys.path.insert(0, SCRIPTS_DIR)

from client import Client, MailboxError, MailboxTransportError, new_id  # noqa: E402

_BINARY = None


def go_env():
    env = dict(os.environ)
    env.setdefault("GOFLAGS", "-mod=mod")
    env.setdefault("GOCACHE", os.path.join(tempfile.gettempdir(), "agentmailbox-gocache"))
    env.setdefault("GOSUMDB", "off")
    env.setdefault("GOPRIVATE", "*")
    env.setdefault("GOTOOLCHAIN", "local")
    return env


def build_binary():
    global _BINARY
    if _BINARY is not None:
        return _BINARY
    out_dir = tempfile.mkdtemp(prefix="amb-bin-")
    binary = os.path.join(out_dir, "agent-mailbox")
    result = subprocess.run(
        ["go", "build", "-o", binary, "./cmd/agent-mailbox"],
        cwd=MODULE_ROOT,
        env=go_env(),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError("go build failed:\n%s" % result.stderr[-2000:])
    _BINARY = binary
    return binary


class Service:
    def __init__(self, runtime_dir):
        self.runtime_dir = runtime_dir
        self.socket_path = os.path.join(runtime_dir, "api.sock")
        self.process = subprocess.Popen(
            [build_binary(), "serve", "--runtime", runtime_dir],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        self._wait_for_socket()
        with open(os.path.join(runtime_dir, "admin-token"), encoding="utf-8") as handle:
            self.admin_token = handle.read().strip()

    def _wait_for_socket(self):
        deadline = time.time() + 10
        while time.time() < deadline:
            if os.path.exists(self.socket_path):
                return
            if self.process.poll() is not None:
                error = self.process.stderr.read().decode("utf-8", "replace")
                raise AssertionError("service exited early: %s" % error)
            time.sleep(0.02)
        raise AssertionError("service socket did not appear")

    def client(self, token, timeout=12):
        return Client(self.socket_path, token, timeout=timeout)

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        if self.process.stderr:
            self.process.stderr.close()

    def admin(self):
        return self.client(self.admin_token)


class McpProcess:
    """Drives mcp_server.py over its real stdio JSON-RPC transport."""

    def __init__(self, binding_path):
        self.process = subprocess.Popen(
            [sys.executable, os.path.join(SCRIPTS_DIR, "mcp_server.py"), "--binding", binding_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.next_id = 1

    def call(self, method, params=None):
        request_id = self.next_id
        self.next_id += 1
        message = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            raise AssertionError("mcp server closed stdout")
        return json.loads(line)

    def notify(self, method, params=None):
        message = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = params
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def stop(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        for stream in (self.process.stdout, self.process.stderr):
            if stream:
                stream.close()


def tool_text(response):
    return json.loads(response["result"]["content"][0]["text"])


class MailboxE2EBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if shutil.which("go") is None:
            raise unittest.SkipTest("go toolchain not found")

    def setUp(self):
        self.runtime_dir = tempfile.mkdtemp(prefix="amb-e2e-")
        self.service = Service(self.runtime_dir)
        self.addCleanup(self.service.stop)
        self.addCleanup(shutil.rmtree, self.runtime_dir, ignore_errors=True)
        self.admin = self.service.admin()

    def register(self, key, **fields):
        payload = {
            "registration_key": key,
            "token": "token-" + key,
            "label": key,
            "kind": "test",
            "scope": "scope-1",
            "team": "team-1",
        }
        payload.update(fields)
        return self.admin.request("POST", "/v1/register", payload)["mailbox"]

    def client_for(self, key, timeout=12):
        return self.service.client("token-" + key, timeout=timeout)

    def write_binding(self, key, mailbox_id, generation=""):
        path = os.path.join(self.runtime_dir, key + "-binding.json")
        binding = {
            "schema": 1,
            "socket": self.service.socket_path,
            "token": "token-" + key,
            "mailbox_id": mailbox_id,
            "generation": generation,
        }
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(binding, handle)
        return path

    def run_cli(self, binding_path, *args, expect=0):
        result = subprocess.run(
            [sys.executable, os.path.join(SCRIPTS_DIR, "mailbox.py"), "--binding", binding_path] + list(args),
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(result.returncode, expect, "cli stderr: %s" % result.stderr)
        return result

    def assert_mailbox_error(self, code, func, *args, **kwargs):
        with self.assertRaises(MailboxError) as caught:
            func(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)


class TestConversation(MailboxE2EBase):
    def test_two_independent_clients_exchange_and_reply(self):
        alice = self.register("alice")
        bob = self.register("bob")
        alice_client = self.client_for("alice")
        bob_client = self.client_for("bob")

        # Health needs no token, proving the service stands alone.
        anonymous = self.service.client("")
        self.assertEqual(anonymous.health()["protocol"], "agent-mailbox-v1")

        bob_client.activate({"generation": "bob-gen-1"})

        body = "你好，Bob。UTF-8 原文 ✓"
        sent = alice_client.send(
            {
                "client_message_id": new_id("c"),
                "to": bob["mailbox_id"],
                "body": body,
                "task_id": "task-1",
            }
        )
        self.assertEqual(sent["body"], body)
        self.assertEqual(sent["state"], "pending")
        self.assertEqual(sent["sender_id"], alice["mailbox_id"])

        self.assertIn(bob["mailbox_id"], {peer["mailbox_id"] for peer in alice_client.peers()})
        self.assertIn(alice["mailbox_id"], {peer["mailbox_id"] for peer in bob_client.peers()})

        inbox = bob_client.inbox()
        self.assertEqual([m["message_id"] for m in inbox["messages"]], [sent["message_id"]])

        claimed = bob_client.claim({"generation": "bob-gen-1", "request_id": "req-1"})
        self.assertEqual(claimed["message"]["message_id"], sent["message_id"])
        self.assertTrue(claimed["attempt_id"])

        accepted = bob_client.receipt(
            {
                "generation": "bob-gen-1",
                "message_id": sent["message_id"],
                "attempt_id": claimed["attempt_id"],
                "state": "accepted",
            }
        )
        self.assertEqual(accepted["state"], "accepted")
        self.assertEqual(accepted["claimed_by_generation"], "bob-gen-1")
        self.assertGreater(accepted["claimed_at"], 0)
        self.assertEqual(len(accepted["receipts"]), 1)
        self.assertEqual(alice_client.get(sent["message_id"])["claimed_by_generation"], "bob-gen-1")

        reply = bob_client.send(
            {
                "client_message_id": new_id("c"),
                "to": alice["mailbox_id"],
                "body": "收到",
                "task_id": "task-1",
                "reply_to": sent["message_id"],
            }
        )
        self.assertEqual(reply["reply_to"], sent["message_id"])
        acked = alice_client.ack(reply["message_id"])
        self.assertEqual(acked["state"], "pending")
        self.assertGreater(acked["received_at"], 0)

    def test_inbox_cursor_is_monotonic(self):
        alice = self.register("alice")
        bob = self.register("bob")
        alice_client = self.client_for("alice")
        bob_client = self.client_for("bob")

        for index in range(5):
            alice_client.send(
                {
                    "client_message_id": new_id("c"),
                    "to": bob["mailbox_id"],
                    "body": "message %d" % index,
                    "task_id": "task-1",
                }
            )

        seen = []
        cursor = None
        while True:
            page = bob_client.inbox(cursor)
            if not page["messages"]:
                break
            seen.extend(message["message_id"] for message in page["messages"])
            cursor = page["cursor"]
        self.assertEqual(len(seen), 5)
        self.assertEqual(len(set(seen)), 5)


class TestReliability(MailboxE2EBase):
    def test_dedup_conflict_and_impersonation(self):
        alice = self.register("alice")
        bob = self.register("bob")
        carol = self.register("carol", team="team-2")
        dave = self.register("dave", scope="scope-2")
        alice_client = self.client_for("alice")

        client_id = new_id("c")
        payload = {"client_message_id": client_id, "to": bob["mailbox_id"], "body": "hello", "task_id": "task-1"}
        first = alice_client.send(dict(payload))
        again = alice_client.send(dict(payload))
        self.assertEqual(first["message_id"], again["message_id"])

        self.assert_mailbox_error(
            "idempotency_conflict",
            alice_client.send,
            {"client_message_id": client_id, "to": bob["mailbox_id"], "body": "different", "task_id": "task-1"},
        )
        self.assert_mailbox_error(
            "peer_not_allowed",
            alice_client.send,
            {"client_message_id": new_id("c"), "to": carol["mailbox_id"], "body": "hi", "task_id": "t"},
        )
        self.assert_mailbox_error(
            "peer_not_allowed",
            alice_client.send,
            {"client_message_id": new_id("c"), "to": dave["mailbox_id"], "body": "hi", "task_id": "t"},
        )

        raw = alice_client.request(
            "POST",
            "/v1/send",
            {
                "client_message_id": new_id("c"),
                "to": bob["mailbox_id"],
                "body": "pretend",
                "sender_id": bob["mailbox_id"],
            },
        )
        self.assertEqual(raw["message"]["sender_id"], alice["mailbox_id"])

        self.assert_mailbox_error("unauthorized", self.service.client("bad-token").me)

    def test_claim_idempotent_and_generation_change(self):
        self.register("alice")
        bob = self.register("bob")
        alice_client = self.client_for("alice")
        bob_client = self.client_for("bob")
        bob_client.activate({"generation": "gen-1"})

        empty = bob_client.claim({"generation": "gen-1", "request_id": "r-empty"})
        self.assertIsNone(empty["message"])
        self.assertIsNone(empty["attempt_id"])
        self.assertEqual(empty, bob_client.claim({"generation": "gen-1", "request_id": "r-empty"}))

        first = alice_client.send(
            {"client_message_id": new_id("c"), "to": bob["mailbox_id"], "body": "first", "task_id": "t"}
        )
        second = alice_client.send(
            {"client_message_id": new_id("c"), "to": bob["mailbox_id"], "body": "second", "task_id": "t"}
        )

        claimed = bob_client.claim({"generation": "gen-1", "request_id": "r-1"})
        self.assertEqual(claimed["message"]["message_id"], first["message_id"])
        replayed = bob_client.claim({"generation": "gen-1", "request_id": "r-1"})
        self.assertEqual(replayed["message"]["message_id"], first["message_id"])
        self.assertEqual(replayed["attempt_id"], claimed["attempt_id"])

        self.assert_mailbox_error(
            "generation_mismatch",
            bob_client.claim,
            {"generation": "gen-old", "request_id": "r-x"},
        )

        bob_client.deactivate({"generation": "gen-1"})
        bob_client.activate({"generation": "gen-2"})
        # The same request_id cannot be reused by another generation.
        self.assert_mailbox_error(
            "request_id_conflict",
            bob_client.claim,
            {"generation": "gen-2", "request_id": "r-1"},
        )
        next_claim = bob_client.claim({"generation": "gen-2", "request_id": "r-2"})
        self.assertEqual(next_claim["message"]["message_id"], second["message_id"])
        # The old dispatching message stays for reconciliation.
        self.assertEqual(alice_client.get(first["message_id"])["state"], "dispatching")

    def test_weaker_receipt_does_not_erase_accepted(self):
        self.register("alice")
        bob = self.register("bob")
        alice_client = self.service.client("token-alice")
        bob_client = self.client_for("bob")
        bob_client.activate({"generation": "g"})

        sent = alice_client.send(
            {"client_message_id": new_id("c"), "to": bob["mailbox_id"], "body": "hi", "task_id": "t"}
        )
        claimed = bob_client.claim({"generation": "g", "request_id": "r"})
        bob_client.receipt(
            {
                "generation": "g",
                "message_id": sent["message_id"],
                "attempt_id": claimed["attempt_id"],
                "state": "accepted",
            }
        )
        observed = bob_client.receipt(
            {
                "generation": "g",
                "message_id": sent["message_id"],
                "attempt_id": claimed["attempt_id"],
                "state": "offered",
            }
        )
        self.assertEqual(observed["state"], "accepted")


class TestCliAndMcp(MailboxE2EBase):
    def test_cli_send_show_ack_doctor_and_peers(self):
        alice = self.register("alice")
        bob = self.register("bob")
        bob_client = self.client_for("bob")
        bob_client.activate({"generation": "g"})

        binding_path = self.write_binding("alice", alice["mailbox_id"], generation="g")
        body_file = os.path.join(self.runtime_dir, "body.txt")
        with open(body_file, "w", encoding="utf-8") as handle:
            handle.write("来自 CLI 的正文\n")

        sent = json.loads(
            self.run_cli(
                binding_path,
                "send",
                "--to",
                bob["mailbox_id"],
                "--id",
                "cli-1",
                "--task-id",
                "task-1",
                "--file",
                body_file,
            ).stdout
        )
        message = sent["message"]
        self.assertEqual(message["body"], "来自 CLI 的正文\n")

        shown = json.loads(self.run_cli(binding_path, "show", "--id", message["message_id"]).stdout)
        self.assertEqual(shown["message"]["message_id"], message["message_id"])

        peers = json.loads(self.run_cli(binding_path, "peers").stdout)
        self.assertIn(bob["mailbox_id"], {peer["mailbox_id"] for peer in peers["peers"]})

        doctor = json.loads(self.run_cli(binding_path, "doctor").stdout)
        self.assertEqual(doctor["health"]["protocol"], "agent-mailbox-v1")
        self.assertEqual(doctor["me"]["mailbox_id"], alice["mailbox_id"])
        self.assertNotIn("token", json.dumps(doctor))

        bob_client.ack(message["message_id"])
        acked = json.loads(self.run_cli(binding_path, "show", "--id", message["message_id"]).stdout)
        self.assertEqual(acked["message"]["state"], "pending")
        self.assertGreater(acked["message"]["received_at"], 0)

        missing = os.path.join(self.runtime_dir, "no-such-binding.json")
        self.run_cli(missing, "peers", expect=2)

    def test_mcp_stdio_tool_round_trip(self):
        alice = self.register("alice")
        bob = self.register("bob")
        alice_client = self.client_for("alice")
        bob_client = self.client_for("bob")
        bob_client.activate({"generation": "g"})

        binding_path = self.write_binding("bob", bob["mailbox_id"], generation="g")
        mcp = McpProcess(binding_path)
        self.addCleanup(mcp.stop)

        initialized = mcp.call(
            "initialize",
            {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}},
        )
        self.assertEqual(initialized["result"]["protocolVersion"], "2024-11-05")
        mcp.notify("notifications/initialized")

        listed = mcp.call("tools/list")
        names = {tool["name"] for tool in listed["result"]["tools"]}
        self.assertEqual(
            names,
            {"list_peers", "send_message", "read_message", "ack_message", "message_status", "wait_for_message"},
        )

        sent = alice_client.send(
            {"client_message_id": new_id("c"), "to": bob["mailbox_id"], "body": "给 MCP 的消息", "task_id": "t"}
        )

        peers = tool_text(mcp.call("tools/call", {"name": "list_peers", "arguments": {}}))
        self.assertIn(alice["mailbox_id"], {peer["mailbox_id"] for peer in peers["peers"]})

        waited = tool_text(
            mcp.call("tools/call", {"name": "wait_for_message", "arguments": {"after": "0", "timeout": 5}})
        )
        self.assertIn(sent["message_id"], [m["message_id"] for m in waited["messages"]])

        read = tool_text(
            mcp.call("tools/call", {"name": "read_message", "arguments": {"message_id": sent["message_id"]}})
        )
        self.assertEqual(read["message"]["body"], "给 MCP 的消息")

        acked = tool_text(
            mcp.call("tools/call", {"name": "ack_message", "arguments": {"message_id": sent["message_id"]}})
        )
        self.assertEqual(acked["message"]["state"], "pending")
        self.assertGreater(acked["message"]["received_at"], 0)

        outgoing = tool_text(
            mcp.call(
                "tools/call",
                {
                    "name": "send_message",
                    "arguments": {
                        "to": alice["mailbox_id"],
                        "id": "mcp-1",
                        "task_id": "t",
                        "body": "来自 MCP 的回复",
                    },
                },
            )
        )
        self.assertEqual(outgoing["message"]["sender_id"], bob["mailbox_id"])
        self.assertEqual(outgoing["message"]["body"], "来自 MCP 的回复")

        missing_arg = mcp.call(
            "tools/call",
            {"name": "send_message", "arguments": {"to": alice["mailbox_id"], "id": "mcp-2", "task_id": "t"}},
        )
        self.assertTrue(missing_arg["result"]["isError"])
        self.assertIn("invalid_request", missing_arg["result"]["content"][0]["text"])

        unknown = mcp.call(
            "tools/call", {"name": "message_status", "arguments": {"message_id": "msg_missing"}}
        )
        self.assertTrue(unknown["result"]["isError"])
        self.assertIn("not_found", unknown["result"]["content"][0]["text"])

        empty = tool_text(
            mcp.call(
                "tools/call",
                {"name": "wait_for_message", "arguments": {"after": waited["cursor"], "timeout": 0}},
            )
        )
        self.assertEqual(empty["messages"], [])


    def test_cli_reports_bad_message_id_without_traceback(self):
        alice = self.register("alice")
        binding_path = self.write_binding("alice", alice["mailbox_id"])
        result = self.run_cli(binding_path, "show", "--id", "a b", expect=1)
        self.assertNotIn("Traceback", result.stderr)
        self.assertIn("not_found", result.stderr)

    def test_cli_rejects_non_positive_timeout(self):
        alice = self.register("alice")
        binding_path = self.write_binding("alice", alice["mailbox_id"])
        result = self.run_cli(binding_path, "--timeout", "-1", "doctor", expect=2)
        self.assertNotIn("Traceback", result.stderr)

    def test_mcp_notifications_and_param_validation(self):
        bob = self.register("bob")
        binding_path = self.write_binding("bob", bob["mailbox_id"])
        mcp = McpProcess(binding_path)
        self.addCleanup(mcp.stop)

        initialized = mcp.call(
            "initialize",
            {"protocolVersion": "2024-11-05", "capabilities": {}, "clientInfo": {"name": "t", "version": "0"}},
        )
        self.assertEqual(initialized["id"], 1)

        # A JSON-RPC notification must not be answered: the next real request
        # must receive the very next output line, not a stray id:null reply.
        mcp.notify("ping")
        listed = mcp.call("tools/list")
        self.assertEqual(listed["id"], 2)
        self.assertIn("result", listed)

        bad_args = mcp.call("tools/call", {"name": "list_peers", "arguments": 5})
        self.assertTrue(bad_args["result"]["isError"])
        self.assertIn("invalid_request", bad_args["result"]["content"][0]["text"])

        bad_params = mcp.call("tools/call", "oops")
        self.assertEqual(bad_params["error"]["code"], -32602)


    def test_ended_generation_cannot_reactivate(self):
        self.register("bob")
        bob_client = self.client_for("bob")
        bob_client.activate({"generation": "g1"})
        bob_client.deactivate({"generation": "g1"})
        bob_client.activate({"generation": "g2"})
        bob_client.deactivate({"generation": "g2"})
        self.assert_mailbox_error("generation_ended", bob_client.activate, {"generation": "g1"})

    def test_serve_creates_private_runtime_files(self):
        self.register("bob")
        self.assertEqual(os.stat(self.runtime_dir).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(self.service.socket_path).st_mode & 0o777, 0o600)
        for name in ("admin-token", "mailbox.sqlite3", "mailbox.sqlite3-wal", "mailbox.sqlite3-shm"):
            path = os.path.join(self.runtime_dir, name)
            if os.path.exists(path):
                mode = os.stat(path).st_mode & 0o777
                self.assertEqual(mode, 0o600, "%s mode is %o" % (name, mode))


if __name__ == "__main__":
    unittest.main()
