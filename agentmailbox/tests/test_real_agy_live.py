"""Ordinary submit/worker path against the real Antigravity CLI and a local fake provider.

Two integration layers are covered here:

* ``TestRealAgyLive`` runs the ordinary ``dorothy.submit`` -> ``_worker`` path with a
  trusted test-only executable wrapper. The wrapper sets the temporary
  ``modelProvider=gemini`` plus dummy ``GEMINI_API_KEY`` / ``GOOGLE_GEMINI_BASE_URL``
  and execs the real CLI. No production credential is read and
  ``DOROTHY_TEST_UNSANDBOXED`` is never used.
* ``TestMailboxIdentityFreeze`` drives three ordinary turns against the real Go
  service with a fake native executor that speaks the stream-json protocol.

Both run under the worker's normal Seatbelt boundary. Where a nested sandbox
forbids ``sandbox-exec`` (macOS exit 71) the sandbox-dependent tests skip with an
explicit reason instead of reporting a passing result.
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "skill/scripts"
DOROTHY_DIR = (
    Path(__file__).resolve().parents[1]
    / "integration/workspace/skills/cc-dorothy-collaboration/scripts"
)

for p in (SCRIPTS_DIR, DOROTHY_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import dorothy
import native
import host_runtime

PROBE_ROOT = dorothy.WORKSPACE / ".local/test-dorothy-live"

MCP_TOOL_NAMES = {
    "list_peers", "send_message", "read_message", "ack_message",
    "message_status", "wait_for_message",
}


def sandbox_available():
    """The worker's real sandbox must apply; a nested shell returns exit 71."""
    try:
        probe = subprocess.run(
            ["/usr/bin/sandbox-exec", "-p", "(version 1)(allow default)", "--", "/usr/bin/true"],
            capture_output=True, text=True, timeout=10,
        )
    except OSError:
        return False
    return probe.returncode == 0


SANDBOX_AVAILABLE = sandbox_available()


def real_mailbox_binary(dest_dir):
    explicit = os.environ.get("DOROTHY_TEST_MAILBOX_BIN")
    if explicit and Path(explicit).is_file():
        return Path(explicit).resolve()
    target = Path(dest_dir) / "agent-mailbox"
    env = dict(os.environ)
    env.setdefault("GOCACHE", str(Path(tempfile.gettempdir()) / "dorothy-go-cache"))
    module = Path(__file__).resolve().parents[1]
    try:
        subprocess.run(["go", "build", "-o", str(target), "./cmd/agent-mailbox"],
                       cwd=module, env=env, capture_output=True, text=True, timeout=300, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    return target if target.is_file() else None


def start_mailbox_service(binary, runtime_dir):
    log = open(runtime_dir / "service.log", "w")
    proc = subprocess.Popen([str(binary), "serve", "--runtime", str(runtime_dir)],
                            stdout=log, stderr=log, start_new_session=True)
    proc.service_log = log
    socket_path = runtime_dir / "api.sock"
    token_path = runtime_dir / "admin-token"
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            break
        if socket_path.exists() and token_path.exists() and host_runtime.check_mailbox_service(socket_path):
            return proc, socket_path, token_path
        time.sleep(0.05)
    raise RuntimeError("agent-mailbox service did not become ready")


def mcp_tools_from_config(config_path, archived_binding):
    """Launch the worker-produced MCP entry and list its tools."""
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    entry = config["mcpServers"]["agent-mailbox"]
    args = list(entry["args"])
    for index, value in enumerate(args):
        if value == "--binding":
            args[index + 1] = str(archived_binding)
    lines = [
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                               "clientInfo": {"name": "test", "version": "1"}}}),
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    ]
    completed = subprocess.run([entry["command"], *args], input="\n".join(lines) + "\n",
                               capture_output=True, text=True, timeout=30)
    tools = None
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if message.get("id") == 2:
            tools = message["result"]["tools"]
    return entry, tools, completed.stderr


FAKE_NATIVE_SOURCE = '''#!/usr/bin/env python3
"""Test-only native executor that speaks the stream-json protocol."""
import json, sys, uuid

args = sys.argv[1:]
conversation = None
for index, value in enumerate(args):
    if value == "--conversation" and index + 1 < len(args):
        conversation = args[index + 1]
if conversation is None:
    conversation = "conv-" + uuid.uuid4().hex

def emit(event):
    sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\\n")
    sys.stdout.flush()

emit({"event": "init", "conversation_id": conversation,
      "init": {"conversation_id": conversation, "model": "gemini-3.8-flash-high"}})
emit({"event": "step_update", "step_update": {
    "conversation_id": conversation, "step_index": 0, "state": "DONE", "step_type": "user_input"}})
emit({"event": "step_update", "step_update": {
    "conversation_id": conversation, "step_index": 1, "state": "ACTIVE",
    "step_type": "agent_response", "text_delta": "fake native reply"}})
emit({"event": "step_update", "step_update": {
    "conversation_id": conversation, "step_index": 1, "state": "DONE",
    "step_type": "agent_response", "text_delta": "\\n"}})
emit({"event": "result", "result": {
    "conversation_id": conversation, "status": "SUCCESS", "usage": {}}})
'''


def walk_texts(value, out):
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "text" and isinstance(item, str):
                out.append(item)
            else:
                walk_texts(item, out)
    elif isinstance(value, list):
        for item in value:
            walk_texts(item, out)
    elif isinstance(value, str):
        out.append(value)


def has_function_response(value):
    if isinstance(value, dict):
        if "functionResponse" in value:
            return True
        return any(has_function_response(item) for item in value.values())
    if isinstance(value, list):
        return any(has_function_response(item) for item in value)
    return False


class _FakeProvider(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        data = {"models": [{"name": "models/gemini-3.8-flash",
                            "supportedGenerationMethods": ["generateContent", "streamGenerateContent"]}]}
        encoded = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(encoded)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        script = self.server.script
        with self.server.lock:
            self.server.calls.append({"path": self.path, "body": body})
            if "/models/gemini-3.8-flash:" in self.path:
                payload = json.loads(body.decode("utf-8"))
                kind = script(payload)
            else:
                kind = {"kind": "text", "text": "helper"}
        parts = []
        if kind["kind"] == "text":
            parts = [{"text": kind["text"]}]
        else:
            parts = [{"functionCall": {"name": kind["name"], "args": kind["args"]}}]
        response = {
            "candidates": [{"content": {"role": "model", "parts": parts},
                            "finishReason": "STOP", "index": 0}],
            "usageMetadata": {"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
            "modelVersion": "gemini-3.8-flash",
        }
        encoded = json.dumps(response).encode()
        self.send_response(200)
        if "streamGenerateContent" in self.path:
            self.send_header("Content-Type", "text/event-stream")
            self.end_headers()
            self.wfile.write(b"data: " + encoded + b"\n\n")
        else:
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(encoded)
        self.wfile.flush()


class _FakeProviderServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address):
        super().__init__(address, _FakeProvider)
        self.calls = []
        self.lock = threading.Lock()
        self.script = lambda payload: {"kind": "text", "text": "idle"}


def call_mcp_tool(server_name, tool_name, arguments):
    return {
        "ServerName": server_name, "ToolName": tool_name, "Arguments": arguments,
        "toolSummary": "Handling mailbox message", "toolAction": "Handling mailbox message",
    }


@unittest.skipUnless(SANDBOX_AVAILABLE, "sandbox-exec unavailable (nested sandbox returns exit 71)")
class TestRealAgyLive(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        agy = os.environ.get("DOROTHY_TEST_AGY_BIN")
        if not agy or not Path(agy).is_file():
            raise unittest.SkipTest("set DOROTHY_TEST_AGY_BIN to the real Antigravity CLI")
        cls.agy = Path(agy).resolve()

    def setUp(self):
        temp_root = Path(tempfile.mkdtemp(prefix="agy-live-"))
        self.addCleanup(shutil.rmtree, temp_root, ignore_errors=True)
        self.root = temp_root
        self.service_proc, self.socket, self.admin = start_mailbox_service(
            real_mailbox_binary(self.root), self.root)
        self.addCleanup(self._stop_service)

        reg = self.root / "registrations"
        reg.mkdir(mode=0o700)
        atlas, atlas_tok = host_runtime.register_task_mailbox(
            self.socket, self.admin, "live-atlas", "live-scope", team="live-team",
            label="Atlas", kind="atlas", coordinator=True, task_dir=reg)
        self.atlas_id, self.atlas_tok = atlas["mailbox_id"], atlas_tok
        self.atlas_binding = self.root / "atlas-binding.json"
        host_runtime.create_binding_file(
            self.atlas_binding, self.socket, atlas_tok, self.atlas_id, "atlas-gen", scope="live-scope")

        env = {"AGENT_MAILBOX_BINDING": str(self.atlas_binding), "AGENT_MAILBOX_RUNTIME": str(self.root)}
        for key, value in env.items():
            old = os.environ.get(key)
            self.addCleanup(self._restore_env, key, old)
            os.environ[key] = value

        live_root = PROBE_ROOT / ("run-" + uuid.uuid4().hex)
        self.addCleanup(shutil.rmtree, live_root, ignore_errors=True)
        self.runtime = live_root / "runtime"
        self.runtime.mkdir(parents=True, mode=0o700)
        self.work = live_root / "work"
        self.work.mkdir(parents=True, mode=0o700)
        self.auth = self.root / "auth.token"
        self.auth.write_text("dummy-oauth-token", encoding="utf-8")

        self.server = _FakeProviderServer(("127.0.0.1", 0))
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self._stop_provider)

        self.wrapper = self.root / "agy-wrapper.py"
        self.wrapper.write_text(
            f"#!{sys.executable}\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            f"REAL = {str(self.agy)!r}\n"
            f"BASE = 'http://127.0.0.1:{self.server.server_port}'\n"
            "args = sys.argv[1:]\n"
            "gemini_dir = next(a.split('=', 1)[1] for a in args if a.startswith('--gemini_dir='))\n"
            "settings = Path(gemini_dir) / 'antigravity-cli' / 'settings.json'\n"
            "data = json.loads(settings.read_text(encoding='utf-8'))\n"
            "data['modelProvider'] = 'gemini'\n"
            "settings.write_text(json.dumps(data), encoding='utf-8')\n"
            "home = Path(gemini_dir) / 'home'\n"
            "(home / '.config').mkdir(parents=True, exist_ok=True)\n"
            "(home / '.cache').mkdir(parents=True, exist_ok=True)\n"
            "env = dict(os.environ)\n"
            "env['HOME'] = str(home)\n"
            "env['XDG_CONFIG_HOME'] = str(home / '.config')\n"
            "env['XDG_CACHE_HOME'] = str(home / '.cache')\n"
            "env['GEMINI_API_KEY'] = 'dummy-live-key'\n"
            "env['GOOGLE_GEMINI_BASE_URL'] = BASE\n"
            "os.execve(REAL, [REAL, *args], env)\n",
            encoding="utf-8")
        self.wrapper.chmod(0o700)
        (self.runtime / "config.json").write_text(json.dumps({
            "executable": str(self.wrapper), "auth_file": str(self.auth)}), encoding="utf-8")

        self.state = {"state": "idle"}
        self.state_lock = threading.Lock()
        self.ready = threading.Event()
        self.sent = []
        self.body_tokens = []
        self.server.script = self.script

    def _stop_service(self):
        try:
            self.service_proc.terminate()
            self.service_proc.wait(timeout=5)
        except Exception:
            self.service_proc.kill()
        log = getattr(self.service_proc, "service_log", None)
        if log is not None:
            log.close()

    def _stop_provider(self):
        self.server.shutdown()
        self.server.server_close()

    @staticmethod
    def _restore_env(key, old):
        if old is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = old

    def start_run(self, token):
        with self.state_lock:
            self.state = {"state": "idle"}
        self.body_tokens = [token]
        self.ready.clear()

    def script(self, payload):
        texts = []
        walk_texts(payload, texts)
        joined = "\n".join(texts)
        with self.state_lock:
            state = self.state["state"]
            if state == "idle":
                if not self.ready.wait(timeout=30):
                    return {"kind": "text", "text": "DONE"}
                self.sent.append(self.send_message(self.body_tokens[0]))
                self.state["state"] = "await_inject"
                return {"kind": "text", "text": "BEFORE"}
            if state == "await_inject":
                # A resumed conversation also contains the previous turn's mail.
                # Act only on the message sent during this turn.
                message_id = self.sent[-1]
                if f"消息编号：{message_id}" not in joined:
                    return {"kind": "text", "text": "WAIT"}
                self.state["message_id"] = message_id
                self.state["state"] = "await_read"
                return {"kind": "function", "name": "call_mcp_tool",
                        "args": call_mcp_tool("agent-mailbox", "read_message",
                                              {"message_id": self.state["message_id"]})}
            if state == "await_read":
                self.state["state"] = "await_ack"
                return {"kind": "function", "name": "call_mcp_tool",
                        "args": call_mcp_tool("agent-mailbox", "ack_message",
                                              {"message_id": self.state["message_id"]})}
            if state == "await_ack":
                self.state["state"] = "done"
                return {"kind": "text", "text": "DONE"}
        return {"kind": "text", "text": "DONE"}

    def send_message(self, token):
        client = host_runtime.Client(str(self.socket), self.atlas_tok)
        message = client.send({
            "to": self.dorothy_id, "client_message_id": "live-" + token,
            "body": token, "task_id": "task-live", "reply_to": "",
        })
        return message["message_id"]

    def wait_for(self, job, runtime, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            value = dorothy.status(job, runtime)
            if value["status"] not in dorothy.ACTIVE:
                return value
            time.sleep(0.5)
        raise AssertionError(f"job {job} did not finish: {dorothy.status(job, runtime)}")

    def submit_turn(self, job, task, parent=None, token=None):
        task_file = self.work / (job + ".md")
        task_file.write_text(task, encoding="utf-8")
        kwargs = {"cwd": self.work, "runtime": self.runtime}
        if parent:
            kwargs["parent"] = parent
        else:
            kwargs["team"] = "live-team"
        if token:
            # Arm the fake provider before the worker can make its first call.
            self.start_run(token)
        result = dorothy.submit(job, task_file, **kwargs)
        self.assertEqual(result["job_id"], job)
        dest = self.runtime / "jobs" / job
        request = json.loads((dest / "request.json").read_text(encoding="utf-8"))
        self.dorothy_id = request["mailbox_id"]
        if token:
            self.ready.set()
        value = self.wait_for(job, self.runtime)
        self.assertEqual(value["status"], "completed",
                         json.loads((dest / "state.json").read_text(encoding="utf-8")))
        return dest, request

    def test_ordinary_worker_real_agy_read_ack_and_followup(self):
        token1 = "LIVE_BODY_" + uuid.uuid4().hex
        dest1, req1 = self.submit_turn("live-turn-1", "Read and acknowledge your mail.", token=token1)

        # init wrote the root conversation id that the hook checks.
        root_id = dest1 / "native/mailbox/root-conversation-id.txt"
        self.assertTrue(root_id.is_file(), "worker did not persist the root conversation id")
        # The fake model actually read and acknowledged through MCP.
        atlas_client = host_runtime.Client(str(self.socket), self.atlas_tok)
        self.assertGreater(atlas_client.get(self.sent[-1])["received_at"], 0,
                           "fake model did not ack the message via MCP")
        provider_text = b"\n".join(call["body"] for call in self.server.calls).decode("utf-8", "replace")
        self.assertIn(token1, provider_text, "read result never reached the model")

        # Six tools load from the config the ordinary worker produced.
        entry, tools, stderr = mcp_tools_from_config(
            dest1 / "native/config/mcp_config.json", dest1 / "native/mailbox/mailbox-binding.json")
        self.assertEqual(Path(entry["args"][0]).name, "mcp_server.py")
        self.assertIsNotNone(tools, stderr)
        self.assertEqual({tool["name"] for tool in tools}, MCP_TOOL_NAMES)

        # A real followup keeps the same mailbox and opens a new generation.
        token2 = "LIVE_BODY_" + uuid.uuid4().hex
        dest2, req2 = self.submit_turn("live-turn-2", "Acknowledge the followup mail.",
                                       parent="live-turn-1", token=token2)
        self.assertEqual(req2["mailbox_id"], req1["mailbox_id"])
        binding1 = json.loads((dest1 / "native/mailbox/mailbox-binding.json").read_text(encoding="utf-8"))
        binding2 = json.loads((dest2 / "native/mailbox/mailbox-binding.json").read_text(encoding="utf-8"))
        self.assertEqual(binding2["mailbox_id"], binding1["mailbox_id"])
        self.assertNotEqual(binding2["generation"], binding1["generation"])
        self.assertGreater(atlas_client.get(self.sent[-1])["received_at"], 0,
                           "followup did not ack the new message after the invocation reset")


@unittest.skipUnless(SANDBOX_AVAILABLE, "sandbox-exec unavailable (nested sandbox returns exit 71)")
class TestMailboxIdentityFreeze(unittest.TestCase):
    """Three ordinary turns against the real Go service with a fake native executor."""

    @classmethod
    def setUpClass(cls):
        temp_root = Path(tempfile.mkdtemp(prefix="mbx-freeze-"))
        cls.tmp = temp_root
        cls.root = temp_root
        binary = real_mailbox_binary(temp_root)
        if binary is None:
            raise unittest.SkipTest("could not build the Go agent-mailbox binary")
        cls.proc, cls.socket, cls.admin = start_mailbox_service(binary, temp_root)

        cls.fake_native = temp_root / "fake-native.py"
        cls.fake_native.write_text(FAKE_NATIVE_SOURCE, encoding="utf-8")
        cls.fake_native.chmod(0o700)

        cls.reg = temp_root / "registrations"
        cls.reg.mkdir(mode=0o700)
        atlas, atlas_tok = host_runtime.register_task_mailbox(
            cls.socket, cls.admin, "freeze-atlas", "freeze-scope", team="freeze-team",
            label="Atlas", kind="atlas", coordinator=True, task_dir=cls.reg)
        cls.atlas_id, cls.atlas_tok = atlas["mailbox_id"], atlas_tok
        cls.binding = temp_root / "atlas-binding.json"
        host_runtime.create_binding_file(
            cls.binding, cls.socket, atlas_tok, cls.atlas_id, "atlas-gen", scope="freeze-scope")

        cls.old_env = {key: os.environ.get(key)
                       for key in ("AGENT_MAILBOX_BINDING", "AGENT_MAILBOX_RUNTIME")}
        os.environ["AGENT_MAILBOX_BINDING"] = str(cls.binding)
        os.environ["AGENT_MAILBOX_RUNTIME"] = str(temp_root)

        cls.runtime = PROBE_ROOT / ("freeze-" + uuid.uuid4().hex) / "runtime"
        cls.runtime.mkdir(parents=True, mode=0o700)
        cls.work = cls.runtime.parent / "work"
        cls.work.mkdir(mode=0o700)
        auth = temp_root / "auth.token"
        auth.write_text("dummy-oauth-token", encoding="utf-8")
        (cls.runtime / "config.json").write_text(json.dumps({
            "executable": str(cls.fake_native), "auth_file": str(auth)}), encoding="utf-8")

    @classmethod
    def tearDownClass(cls):
        for key, value in cls.old_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            cls.proc.terminate()
            cls.proc.wait(timeout=5)
        except Exception:
            cls.proc.kill()
        log = getattr(cls.proc, "service_log", None)
        if log is not None:
            log.close()
        shutil.rmtree(cls.tmp, ignore_errors=True)
        shutil.rmtree(cls.runtime.parent, ignore_errors=True)

    def run_turn(self, job, task, parent=None):
        task_file = self.work / (job + ".md")
        task_file.write_text(task, encoding="utf-8")
        kwargs = {"cwd": self.work, "runtime": self.runtime}
        if parent:
            kwargs["parent"] = parent
        else:
            kwargs["team"] = "freeze-team"
        dorothy.submit(job, task_file, **kwargs)
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            value = dorothy.status(job, self.runtime)
            if value["status"] not in dorothy.ACTIVE:
                break
            time.sleep(0.2)
        self.assertEqual(value["status"], "completed",
                         json.loads((self.runtime / "jobs" / job / "state.json").read_text(encoding="utf-8")))
        return (self.runtime / "jobs" / job,
                json.loads((self.runtime / "jobs" / job / "request.json").read_text(encoding="utf-8")))

    def test_three_ordinary_turns_keep_identity_and_change_generation(self):
        dest1, req1 = self.run_turn("freeze-1", "turn one")
        dest2, req2 = self.run_turn("freeze-2", "turn two", parent="freeze-1")
        dest3, req3 = self.run_turn("freeze-3", "turn three", parent="freeze-2")

        self.assertEqual(req1["mailbox_id"], req2["mailbox_id"])
        self.assertEqual(req2["mailbox_id"], req3["mailbox_id"])
        self.assertEqual(req1["components"], req2["components"])
        self.assertEqual(req2["components"], req3["components"])

        generations = []
        for dest in (dest1, dest2, dest3):
            binding = json.loads((dest / "native/mailbox/mailbox-binding.json").read_text(encoding="utf-8"))
            generations.append(binding["generation"])
        self.assertEqual(len(set(generations)), 3, generations)

        # The native session stays the same across followups.
        conversations = []
        for dest in (dest1, dest2, dest3):
            conversations.append((dest / "native/mailbox/root-conversation-id.txt").read_text(encoding="utf-8"))
        self.assertEqual(len(set(conversations)), 1, conversations)


class TestFakeNativeProtocol(unittest.TestCase):
    """The fake executor must satisfy native.Stream before it can stand in for the CLI."""

    def test_fake_native_executor_speaks_stream_protocol(self):
        with tempfile.TemporaryDirectory(prefix="fake-native-") as tmp:
            script = Path(tmp) / "fake-native.py"
            script.write_text(FAKE_NATIVE_SOURCE, encoding="utf-8")
            script.chmod(0o700)
            completed = subprocess.run([str(script), "-p", "prompt"],
                                       capture_output=True, text=True, timeout=30)
        stream = native.Stream()
        for line in completed.stdout.splitlines():
            if line.strip():
                stream.feed(line)
        self.assertIsNone(stream.error, stream.error)
        self.assertEqual(stream.report(), "fake native reply\n")


if __name__ == "__main__":
    unittest.main()
