"""Tests for Dorothy agent-mailbox integration, component freezing, and lifecycle."""

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from contextlib import contextmanager, nullcontext
from unittest.mock import MagicMock, patch

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


def file_digest(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


PROBE_ROOT = dorothy.WORKSPACE / ".local/test-dorothy-mbx"

MCP_TOOL_NAMES = {
    "list_peers",
    "send_message",
    "read_message",
    "ack_message",
    "message_status",
    "wait_for_message",
}


def real_mailbox_binary(dest_dir):
    """Use an explicit binary when provided, otherwise build this module."""
    explicit = os.environ.get("DOROTHY_TEST_MAILBOX_BIN")
    if explicit and Path(explicit).is_file():
        return Path(explicit).resolve()
    target = Path(dest_dir) / "agent-mailbox"
    env = dict(os.environ)
    env.setdefault("GOCACHE", str(Path(tempfile.gettempdir()) / "dorothy-go-cache"))
    module = Path(__file__).resolve().parents[1]
    try:
        subprocess.run(
            ["go", "build", "-o", str(target), "./cmd/agent-mailbox"],
            cwd=module, env=env, capture_output=True, text=True, timeout=300, check=True,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return target if target.is_file() else None


def start_mailbox_service(binary, runtime_dir):
    log = open(runtime_dir / "service.log", "w")
    proc = subprocess.Popen(
        [str(binary), "serve", "--runtime", str(runtime_dir)],
        stdout=log, stderr=log, start_new_session=True,
    )
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


def mcp_tools_list(config_path):
    """Launch the produced MCP entry and list its tools."""
    config = json.loads(Path(config_path).read_text(encoding="utf-8"))
    entry = config["mcpServers"]["agent-mailbox"]
    lines = [
        json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": {"protocolVersion": "2024-11-05", "capabilities": {},
                               "clientInfo": {"name": "test", "version": "1"}}}),
        json.dumps({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}),
    ]
    completed = subprocess.run(
        [entry["command"], *entry["args"]],
        input="\n".join(lines) + "\n", capture_output=True, text=True, timeout=30,
    )
    tools = None
    for line in completed.stdout.splitlines():
        try:
            message = json.loads(line)
        except ValueError:
            continue
        if message.get("id") == 2:
            tools = message["result"]["tools"]
    return entry, tools, completed


def completed_stream():
    stream = MagicMock()
    stream.accepted = True
    stream.reply_done = True
    stream.last_kind = "result"
    stream.reply_index = 0
    stream.last_index = 0
    stream.tools = {}
    stream.terminal = {"status": "SUCCESS"}
    stream.error = None
    stream.partial = []
    stream.report.return_value = "Success report"
    return stream


class TestDorothyMailbox(unittest.TestCase):
    def setUp(self):
        PROBE_ROOT.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(prefix="test-mbx-", dir=PROBE_ROOT)
        self.root = Path(self.tmp.name).resolve()

        self.runtime = self.root / "runtime"
        self.runtime.mkdir(parents=True, mode=0o700)
        self.work = self.root / "work"
        self.work.mkdir(parents=True, mode=0o700)

        self.auth = self.root / "auth.token"
        self.auth.write_text("dummy-oauth-token", encoding="utf-8")
        self.executable = str(Path(sys.executable).resolve())
        (self.runtime / "config.json").write_text(json.dumps({
            "executable": self.executable,
            "auth_file": str(self.auth),
        }), encoding="utf-8")

        self.components = dorothy.freeze_components()
        self.assertIsNotNone(self.components, "shared components must resolve for tests")
        # Keep the real function so test fixtures that patch native.prepare_store
        # can still delegate to it instead of recursing into the patch.
        self.real_prepare_store = native.prepare_store

    def tearDown(self):
        self.tmp.cleanup()

    # --- helpers ---------------------------------------------------------

    def native_dir(self, name):
        return Path(tempfile.gettempdir()).resolve() / ("dorothy-test-" + name + "-" + uuid.uuid4().hex[:8])

    def write_mailbox_job(self, key, native_dir, **overrides):
        dest = self.runtime / "jobs" / key
        dest.mkdir(parents=True, mode=0o700)
        (dest / "task.md").write_text("task " + key, encoding="utf-8")
        request = {
            "version": 1, "job_id": key, "slot": 0, "root_job": key, "parent_job": None,
            "cwd": str(self.work), "task_sha256": file_digest(dest / "task.md"),
            "timeout": 60, "delivery": "manual",
            "mailbox_id": "mbx-job-1", "mailbox_token": "tok-job-1", "scope": "scope-job",
            "team": "team-job", "service_socket": str(self.root / "mbx.sock"),
            "components": self.components, "native_dir": str(native_dir),
            "config": {"executable": self.executable, "auth_file": str(self.auth)},
            "executable_sha256": file_digest(self.executable),
            "expected_conversation_id": None, "created_at": "2026-10-15T00:00:00Z",
        }
        request.update(overrides)
        (dest / "request.json").write_text(json.dumps(request), encoding="utf-8")
        (dest / "state.json").write_text(json.dumps({"status": "queued"}), encoding="utf-8")
        return request, dest

    def run_worker(self, key, dest, request, execute, store=None, activate=None, deactivate=None):
        r1, w1 = os.pipe()
        r2, w2 = os.pipe()
        os.close(w1)
        os.close(w2)
        activate_patch = (patch.object(host_runtime, "activate_mailbox", side_effect=activate)
                          if activate is not None else
                          patch.object(host_runtime, "activate_mailbox", return_value={}))
        deactivate_patch = (patch.object(host_runtime, "deactivate_mailbox", side_effect=deactivate)
                            if deactivate is not None else
                            patch.object(host_runtime, "deactivate_mailbox", return_value={}))
        store_patch = (patch.object(native, "prepare_store", side_effect=store)
                       if store is not None else nullcontext())
        with patch.object(dorothy, "inherited_lease", return_value=None), \
             patch.object(dorothy, "execute_native", side_effect=execute), \
             activate_patch, deactivate_patch, store_patch:
            dorothy.worker(key, self.runtime, r1, r2)

    @staticmethod
    @contextmanager
    def fake_workspace_lease(*args, **kwargs):
        lease = MagicMock()
        lease.fileno.return_value = 1
        yield lease

    def submit_mocks(self):
        """Patch the host runtime so submit() runs without a real service."""
        atlas_binding = self.root / "atlas-binding.json"
        atlas_binding.write_text(json.dumps({
            "schema": 1,
            "socket": str(self.root / "mbx.sock"),
            "token": "tok-atlas",
            "mailbox_id": "mbx-atlas",
            "scope": "scope-1",
        }), encoding="utf-8")
        client = MagicMock()
        client.me.return_value = {
            "mailbox_id": "mbx-atlas", "scope": "scope-1", "coordinator": True, "active": True,
        }

        def register(*args, **kwargs):
            return {"mailbox_id": "mbx-dorothy-1"}, "tok-dorothy-1"

        def popen(*args, **kwargs):
            return MagicMock(pid=4242)

        return atlas_binding, client, register, popen

    # --- component resolution / freezing ---------------------------------

    def test_freeze_components_covers_hook_mcp_host_client(self):
        self.assertEqual(set(self.components), {"agy_hook", "mcp_server", "host_runtime", "client"})
        for record in self.components.values():
            path = Path(record["path"])
            self.assertTrue(path.is_file())
            self.assertEqual(record["sha256"], file_digest(path))

    def test_resolve_shared_script_prefers_production_layout(self):
        fake_workspace = self.root / "workspace"
        production = fake_workspace / "skills/cc-agent-ipc/scripts/agy_hook.py"
        production.parent.mkdir(parents=True)
        production.write_text("# production\n", encoding="utf-8")
        development = fake_workspace / "skill/scripts/agy_hook.py"
        development.parent.mkdir(parents=True)
        development.write_text("# development\n", encoding="utf-8")
        with patch.object(dorothy, "WORKSPACE", fake_workspace):
            self.assertEqual(dorothy.resolve_shared_script("agy_hook.py"), production.resolve())

    def test_submit_missing_components_raises_hard_failure(self):
        task_file = self.work / "task.md"
        task_file.write_text("task", encoding="utf-8")
        with patch.object(dorothy, "freeze_components", return_value=None):
            with self.assertRaises(ValueError) as ctx:
                dorothy.submit("job-missing", task_file, cwd=self.work,
                               team="team-alpha", runtime=self.runtime)
        self.assertEqual(str(ctx.exception), "agent_mailbox_components_missing")

    def test_error_code_keeps_stable_service_code(self):
        class MailboxError(Exception):
            def __init__(self, code):
                super().__init__(code)
                self.code = code

        self.assertEqual(dorothy.error_code(MailboxError("generation_mismatch")), "generation_mismatch")
        self.assertEqual(dorothy.error_code(ValueError("hook_script_tampered")), "hook_script_tampered")
        self.assertEqual(dorothy.error_code(RuntimeError("raw text /tmp/secret")), "RuntimeError")

    # --- parent binding verification -------------------------------------

    def _submit_with_binding(self, binding, me, atlas_binding=None):
        task_file = self.work / "task.md"
        task_file.write_text("task", encoding="utf-8")
        client = MagicMock()
        client.me.return_value = me
        with patch.object(host_runtime, "resolve_mailbox_service",
                          return_value=(self.root / "mbx.sock", self.root / "admin.token")), \
             patch.object(host_runtime, "check_mailbox_service", return_value=True), \
             patch.object(host_runtime, "binding_for_current", return_value=str(binding)), \
             patch.object(host_runtime, "Client", return_value=client), \
             patch.object(host_runtime, "register_task_mailbox",
                          return_value=({"mailbox_id": "mbx-dorothy-1"}, "tok-dorothy-1")), \
             patch.object(dorothy, "process_info", return_value=None), \
             patch("subprocess.Popen", return_value=MagicMock(pid=4242)):
            return dorothy.submit("job-bind", task_file, cwd=self.work,
                                  team="team-alpha", runtime=self.runtime)

    def test_parent_binding_must_be_coordinator(self):
        binding = self.root / "atlas.json"
        binding.write_text(json.dumps({
            "schema": 1, "socket": str(self.root / "mbx.sock"), "token": "tok",
            "mailbox_id": "mbx-atlas", "scope": "scope-1",
        }), encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            self._submit_with_binding(binding, {"mailbox_id": "mbx-atlas", "scope": "scope-1",
                                                "coordinator": False, "active": True})
        self.assertEqual(str(ctx.exception), "agent_mailbox_binding_invalid")

    def test_parent_binding_must_match_target_service(self):
        binding = self.root / "atlas.json"
        binding.write_text(json.dumps({
            "schema": 1, "socket": str(self.root / "other.sock"), "token": "tok",
            "mailbox_id": "mbx-atlas", "scope": "scope-1",
        }), encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            self._submit_with_binding(binding, {"mailbox_id": "mbx-atlas", "scope": "scope-1",
                                                "coordinator": True, "active": True})
        self.assertEqual(str(ctx.exception), "agent_mailbox_binding_service_mismatch")

    def test_parent_binding_rejects_identity_and_scope_mismatch(self):
        binding = self.root / "atlas.json"
        binding.write_text(json.dumps({
            "schema": 1, "socket": str(self.root / "mbx.sock"), "token": "tok",
            "mailbox_id": "mbx-atlas", "scope": "scope-1",
        }), encoding="utf-8")
        for me in ({"mailbox_id": "mbx-other", "scope": "scope-1", "coordinator": True},
                   {"mailbox_id": "mbx-atlas", "scope": "different", "coordinator": True},
                   {"mailbox_id": "mbx-atlas", "scope": "", "coordinator": True}):
            with self.assertRaises(ValueError) as ctx:
                self._submit_with_binding(binding, me)
            self.assertEqual(str(ctx.exception), "agent_mailbox_binding_invalid")

    # --- submit freezing and followup reuse ------------------------------

    def test_submit_freezes_components_and_followup_reuses_them(self):
        atlas_binding, client, register, popen = self.submit_mocks()
        task_file = self.work / "task1.md"
        task_file.write_text("turn 1", encoding="utf-8")
        with patch.object(host_runtime, "resolve_mailbox_service",
                          return_value=(self.root / "mbx.sock", self.root / "admin.token")), \
             patch.object(host_runtime, "check_mailbox_service", return_value=True), \
             patch.object(host_runtime, "binding_for_current", return_value=str(atlas_binding)), \
             patch.object(host_runtime, "Client", return_value=client), \
             patch.object(host_runtime, "register_task_mailbox", side_effect=register), \
             patch.object(dorothy, "process_info", return_value=None), \
             patch.object(dorothy.instances, "workspace_lease", side_effect=self.fake_workspace_lease), \
             patch("subprocess.Popen", side_effect=popen):
            dorothy.submit("job-turn-1", task_file, cwd=self.work, team="team-alpha", runtime=self.runtime)
        req1 = json.loads((self.runtime / "jobs/job-turn-1/request.json").read_text(encoding="utf-8"))
        self.assertEqual(set(req1["components"]), {"agy_hook", "mcp_server", "host_runtime", "client"})
        self.assertEqual(req1["mailbox_id"], "mbx-dorothy-1")

        # Complete turn 1 by hand (no worker process in this unit test).
        dest1 = self.runtime / "jobs/job-turn-1"
        archive = dest1 / "native"
        archive.mkdir(parents=True, mode=0o700)
        (archive / "state.txt").write_text("turn1", encoding="utf-8")
        (dest1 / "report.md").write_text("Report 1", encoding="utf-8")
        dorothy.write_json(dest1 / "state.json", {
            "status": "completed", "conversation_id": "conv-1",
            "native_sha256": dorothy.tree_digest(archive),
            "report_bytes": 8, "report_sha256": hashlib.sha256(b"Report 1").hexdigest(),
        })
        dorothy.write_json(dest1 / "session.json", {"latest_job": "job-turn-1"})

        task_file2 = self.work / "task2.md"
        task_file2.write_text("turn 2", encoding="utf-8")
        changed = {"agy_hook": {"path": "/nowhere", "sha256": "0"}}
        with patch.object(host_runtime, "Client", return_value=client), \
             patch.object(host_runtime, "binding_for_current", return_value=str(atlas_binding)), \
             patch.object(dorothy, "process_info", return_value=None), \
             patch.object(dorothy, "freeze_components", return_value=changed), \
             patch.object(dorothy.instances, "workspace_lease", side_effect=self.fake_workspace_lease), \
             patch("subprocess.Popen", side_effect=popen):
            dorothy.submit("job-turn-2", task_file2, parent="job-turn-1", runtime=self.runtime)
        req2 = json.loads((self.runtime / "jobs/job-turn-2/request.json").read_text(encoding="utf-8"))
        self.assertEqual(req2["components"], req1["components"])
        self.assertEqual(req2["mailbox_id"], req1["mailbox_id"])
        self.assertEqual(req2["scope"], req1["scope"])
        self.assertEqual(req2["service_socket"], req1["service_socket"])

    # --- worker: real config generation and MCP launch -------------------

    def test_worker_generates_working_mcp_config_with_six_tools(self):
        native_dir = self.native_dir("mcp")
        request, dest = self.write_mailbox_job("job-mcp", native_dir)
        captured = {}

        def execute(dest_arg, req, state, *args, **kwargs):
            dorothy.state_update(dest_arg, state, status="running", native_launch_started=True)
            config_path = Path(req["native_dir"]) / "config/mcp_config.json"
            entry, tools, completed = mcp_tools_list(config_path)
            captured["entry"] = entry
            captured["tools"] = tools
            captured["stderr"] = completed.stderr
            return completed_stream(), None, 0, True

        self.run_worker("job-mcp", dest, request, execute)

        state = json.loads((dest / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "completed")
        # The MCP command must launch the server script, never the config JSON.
        self.assertEqual(Path(captured["entry"]["args"][0]).name, "mcp_server.py")
        self.assertNotEqual(Path(captured["entry"]["args"][0]).resolve(), (dest / "native/config/mcp_config.json").resolve())
        self.assertEqual(captured["tools"] is not None, True, captured["stderr"])
        self.assertEqual({tool["name"] for tool in captured["tools"]}, MCP_TOOL_NAMES)

    def test_worker_merges_existing_hooks_and_mcp(self):
        native_dir = self.native_dir("merge")

        def store(path, cwd, auth, archive=None):
            self.real_prepare_store(path, cwd, auth, archive)
            config = Path(path) / "config"
            (config / "hooks.json").write_text(json.dumps({
                "user-hook": {"PreInvocation": [{"type": "command", "command": "echo hi"}]},
            }), encoding="utf-8")
            (config / "mcp_config.json").write_text(json.dumps({
                "mcpServers": {"sqlite": {"command": "sqlite3"}},
            }), encoding="utf-8")

        request, dest = self.write_mailbox_job("job-merge", native_dir)

        def execute(dest_arg, req, state, *args, **kwargs):
            dorothy.state_update(dest_arg, state, status="running", native_launch_started=True)
            return completed_stream(), None, 0, True

        self.run_worker("job-merge", dest, request, execute, store=store)
        config = dest / "native/config"
        hooks = json.loads((config / "hooks.json").read_text(encoding="utf-8"))
        mcp = json.loads((config / "mcp_config.json").read_text(encoding="utf-8"))
        self.assertIn("user-hook", hooks)
        self.assertIn("agent-mailbox", hooks)
        self.assertIn("sqlite", mcp["mcpServers"])
        self.assertIn("agent-mailbox", mcp["mcpServers"])
        self.assertEqual(hooks["agent-mailbox"]["PreInvocation"][0]["timeout"], 10)

    # --- worker: tamper / corrupt config / hard failure ------------------

    def test_worker_rejects_tampered_component(self):
        native_dir = self.native_dir("tamper")
        # Use a private copy so the shared component is never modified.
        hook_copy = self.root / "agy_hook_copy.py"
        hook_copy.write_bytes(Path(self.components["agy_hook"]["path"]).read_bytes())
        components = {name: dict(record) for name, record in self.components.items()}
        components["agy_hook"] = {"path": str(hook_copy), "sha256": file_digest(hook_copy)}
        request, dest = self.write_mailbox_job("job-tamper", native_dir, components=components)
        hook_copy.write_text("# modified\n", encoding="utf-8")

        def execute(*args, **kwargs):
            raise AssertionError("must not launch the model")

        self.run_worker("job-tamper", dest, request, execute)
        state = json.loads((dest / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["error"], "hook_script_tampered")

    def test_worker_hard_fails_when_host_runtime_missing(self):
        native_dir = self.native_dir("nohost")
        request, dest = self.write_mailbox_job("job-nohost", native_dir)

        def execute(*args, **kwargs):
            raise AssertionError("must not launch the model")

        r1, w1 = os.pipe()
        r2, w2 = os.pipe()
        os.close(w1)
        os.close(w2)
        with patch.object(dorothy, "inherited_lease", return_value=None), \
             patch.object(dorothy, "host_runtime", None), \
             patch.object(dorothy, "execute_native", side_effect=execute):
            dorothy.worker("job-nohost", self.runtime, r1, r2)
        state = json.loads((dest / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["error"], "agent_mailbox_components_missing")

    def test_worker_rejects_corrupt_existing_mcp_config(self):
        self._assert_corrupt_config_rejected("mcp_config.json", "mcp_config_invalid")

    def test_worker_rejects_corrupt_existing_hooks(self):
        self._assert_corrupt_config_rejected("hooks.json", "hooks_config_invalid")

    def _assert_corrupt_config_rejected(self, name, error):
        key = "job-corrupt-" + name.replace(".", "-")
        native_dir = self.native_dir(key)
        request, dest = self.write_mailbox_job(key, native_dir)

        def store(path, cwd, auth, archive=None):
            self.real_prepare_store(path, cwd, auth, archive)
            (Path(path) / "config" / name).write_text("{not json", encoding="utf-8")

        def execute(*args, **kwargs):
            raise AssertionError("must not launch the model")

        self.run_worker(key, dest, request, execute, store=store)
        state = json.loads((dest / "state.json").read_text(encoding="utf-8"))
        self.assertEqual(state["status"], "failed")
        self.assertEqual(state["error"], error)

    # --- worker: generation phase evidence -------------------------------

    def test_worker_records_activate_unknown_phase(self):
        native_dir = self.native_dir("activate")
        request, dest = self.write_mailbox_job("job-activate", native_dir)

        def execute(*args, **kwargs):
            raise AssertionError("must not launch the model")

        self.run_worker("job-activate", dest, request, execute,
                        activate=RuntimeError("response_lost"))
        record = json.loads((dest / "generation.json").read_text(encoding="utf-8"))
        self.assertEqual(record["phase"], "activate_unknown")
        self.assertEqual(record["mailbox_id"], "mbx-job-1")
        self.assertEqual(record["error"], "RuntimeError")

    def test_worker_records_deactivate_failed_phase(self):
        native_dir = self.native_dir("deactivate")
        request, dest = self.write_mailbox_job("job-deactivate", native_dir)

        def execute(dest_arg, req, state, *args, **kwargs):
            dorothy.state_update(dest_arg, state, status="running", native_launch_started=True)
            return completed_stream(), None, 0, True

        self.run_worker("job-deactivate", dest, request, execute,
                        deactivate=RuntimeError("deactivate_network_failure"))
        record = json.loads((dest / "generation.json").read_text(encoding="utf-8"))
        self.assertEqual(record["phase"], "deactivate_failed")
        self.assertTrue((dest / "pending-deactivation.json").is_file())
        self.assertEqual(
            json.loads((dest / "pending-deactivation.json").read_text(encoding="utf-8"))["generation"],
            record["generation"],
        )

    # --- native settings merge / corrupt rejection -----------------------

    def test_prepare_store_merges_settings_strictly(self):
        native_dir = self.native_dir("settings")

        native.prepare_store(native_dir, self.work, self.auth)
        settings_path = native_dir / "antigravity-cli/settings.json"
        initial = json.loads(settings_path.read_text(encoding="utf-8"))
        self.assertTrue(initial["allowNonWorkspaceAccess"])
        self.assertIn("mcp(agent-mailbox/read_message)", initial["permissions"]["allow"])
        initial["modelProvider"] = "custom-test-provider"
        initial["permissions"]["allow"].append("write_file(/etc)")
        settings_path.write_text(json.dumps(initial), encoding="utf-8")

        archive = self.root / "archive"
        native.copy_tree(native_dir, archive)
        shutil.rmtree(native_dir)
        native.prepare_store(native_dir, self.work, self.auth, archive=archive)
        merged = json.loads(settings_path.read_text(encoding="utf-8"))
        self.assertEqual(merged.get("modelProvider"), "custom-test-provider")
        self.assertFalse(any("/etc" in p for p in merged["permissions"]["allow"]))
        self.assertIn("mcp(agent-mailbox/ack_message)", merged["permissions"]["allow"])

    def test_prepare_store_rejects_corrupt_settings(self):
        native_dir = self.native_dir("corrupt-settings")
        store = self.root / "corrupt-archive"
        (store / "antigravity-cli").mkdir(parents=True)
        (store / "antigravity-cli/settings.json").write_text("{not json", encoding="utf-8")
        with self.assertRaises(ValueError) as ctx:
            native.prepare_store(native_dir, self.work, self.auth, archive=store)
        self.assertEqual(str(ctx.exception), "native_settings_invalid")


@unittest.skipUnless(
    os.environ.get("DOROTHY_TEST_MAILBOX_BIN") or shutil.which("go"),
    "requires the Go agent-mailbox binary (set DOROTHY_TEST_MAILBOX_BIN or install go)",
)
class TestGenerationRecoveryService(unittest.TestCase):
    """Reconcile leftover generations against the real service, never other executors."""

    @classmethod
    def setUpClass(cls):
        # Unix socket paths are limited to ~104 bytes, so this runtime must live
        # under the short system temp dir rather than the deep workspace tree.
        cls.tmp = tempfile.TemporaryDirectory(prefix="mbx-gen-")
        cls.root = Path(cls.tmp.name).resolve()
        binary = real_mailbox_binary(cls.root)
        if binary is None:
            raise unittest.SkipTest("could not build the Go agent-mailbox binary")
        cls.proc, cls.socket, cls.admin = start_mailbox_service(binary, cls.root)
        reg = cls.root / "registrations"
        reg.mkdir(mode=0o700)
        atlas, _ = host_runtime.register_task_mailbox(
            cls.socket, cls.admin, "gen-atlas", "scope-gen", team="team-gen",
            label="Atlas", kind="atlas", coordinator=True, task_dir=reg)
        cls.mailbox, cls.token = host_runtime.register_task_mailbox(
            cls.socket, cls.admin, "gen-dorothy", "scope-gen", team="team-gen",
            label="Dorothy", kind="dorothy", coordinator=False,
            parent_id=atlas["mailbox_id"], task_dir=reg)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.proc.terminate()
            cls.proc.wait(timeout=5)
        except Exception:
            cls.proc.kill()
        log = getattr(cls.proc, "service_log", None)
        if log is not None:
            log.close()
        cls.tmp.cleanup()

    def setUp(self):
        # Each test starts from a clean, inactive mailbox.
        try:
            me = host_runtime.request_unix(self.socket, "GET", "/v1/me", token=self.token)["mailbox"]
            if me.get("active"):
                host_runtime.deactivate_mailbox(self.socket, self.token, me["generation"])
        except Exception:
            pass

    def parent_request(self):
        return {"service_socket": str(self.socket), "mailbox_token": self.token,
                "mailbox_id": self.mailbox["mailbox_id"]}

    def write_record(self, dest, generation, phase="activated", mailbox_id=None):
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "generation.json").write_text(json.dumps({
            "mailbox_id": mailbox_id or self.mailbox["mailbox_id"],
            "generation": generation, "phase": phase, "socket": str(self.socket),
        }), encoding="utf-8")

    def test_reconcile_ends_own_leftover_generation(self):
        dest = self.root / "parent-own"
        host_runtime.activate_mailbox(self.socket, self.token, "genA", capabilities=["live_input"])
        self.write_record(dest, "genA")
        dorothy.reconcile_leftover_generation(dest, self.parent_request())
        record = json.loads((dest / "generation.json").read_text(encoding="utf-8"))
        self.assertEqual(record["phase"], "ended")
        me = host_runtime.request_unix(self.socket, "GET", "/v1/me", token=self.token)["mailbox"]
        self.assertFalse(me["active"])

    def test_reconcile_refuses_to_end_foreign_generation(self):
        dest = self.root / "parent-foreign"
        host_runtime.activate_mailbox(self.socket, self.token, "genB", capabilities=["live_input"])
        self.write_record(dest, "genA")
        with self.assertRaises(ValueError) as ctx:
            dorothy.reconcile_leftover_generation(dest, self.parent_request())
        self.assertEqual(str(ctx.exception), "generation_foreign_active")
        me = host_runtime.request_unix(self.socket, "GET", "/v1/me", token=self.token)["mailbox"]
        self.assertTrue(me["active"])
        self.assertEqual(me["generation"], "genB")

    def test_reconcile_rejects_identity_mismatch(self):
        dest = self.root / "parent-id"
        self.write_record(dest, "genA", mailbox_id="mbx_someone_else")
        with self.assertRaises(ValueError) as ctx:
            dorothy.reconcile_leftover_generation(dest, self.parent_request())
        self.assertEqual(str(ctx.exception), "generation_recovery_identity_mismatch")

    def test_reconcile_marks_already_ended(self):
        dest = self.root / "parent-ended"
        self.write_record(dest, "genA", phase="deactivate_failed")
        dorothy.reconcile_leftover_generation(dest, self.parent_request())
        record = json.loads((dest / "generation.json").read_text(encoding="utf-8"))
        self.assertEqual(record["phase"], "ended")


if __name__ == "__main__":
    unittest.main()
