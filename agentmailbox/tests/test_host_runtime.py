"""Tests for host_runtime.py control files and binding resolution."""

import json
import os
from pathlib import Path
import hashlib
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "skill/scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import host_runtime
from support import FakeMailbox


class TestHostRuntime(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="test-host-runtime-")
        self.root = Path(self.tmp.name).resolve()
        self.runtime_dir = self.root / "mailbox"
        self.runtime_dir.mkdir(parents=True, mode=0o700)
        self.bindings_dir = self.runtime_dir / "bindings"
        self.bindings_dir.mkdir(parents=True, mode=0o700)
        self.env = patch.dict(os.environ, {"AGENT_MAILBOX_RUNTIME": str(self.runtime_dir)}, clear=False)
        self.env.start()

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def test_compute_scope_format(self):
        project = "test-project"
        session_key = "feishu:chat-123"
        cwd = str(self.root / "work")
        sock = str(self.root / "run/api.sock")

        scope = host_runtime.compute_scope(project, session_key, cwd, sock)
        self.assertTrue(scope.startswith("cc:"))

        expected_raw = json.dumps(
            [project, session_key, str(Path(cwd).resolve()), str(Path(sock).resolve())],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        self.assertEqual(scope, f"cc:{hashlib.sha256(expected_raw).hexdigest()}")

    def test_default_runtime_dir_env(self):
        custom = self.root / "custom_runtime"
        with patch.dict(os.environ, {"AGENT_MAILBOX_RUNTIME": str(custom)}):
            self.assertEqual(host_runtime.default_runtime_dir(), custom.resolve())

    def test_create_binding_file_permissions_and_schema(self):
        binding_path = self.bindings_dir / "test-bind.json"
        sock_path = str(self.root / "run/mailbox.sock")
        res = host_runtime.create_binding_file(
            binding_path, sock_path, "tok-12345", "mbx-abc", "gen-1", scope="cc:testscope")
        self.assertEqual(res, binding_path)
        self.assertEqual(binding_path.stat().st_mode & 0o777, 0o600)
        data = json.loads(binding_path.read_text(encoding="utf-8"))
        self.assertEqual(data["schema"], 1)
        self.assertEqual(data["socket"], sock_path)
        self.assertEqual(data["token"], "tok-12345")
        self.assertEqual(data["mailbox_id"], "mbx-abc")
        self.assertEqual(data["generation"], "gen-1")
        self.assertEqual(data["scope"], "cc:testscope")

    def test_create_binding_file_rejects_symlink_and_non_regular(self):
        target = self.root / "real.json"
        target.write_text("{}", encoding="utf-8")
        symlink = self.bindings_dir / "symlink-bind.json"
        symlink.symlink_to(target)
        with self.assertRaises(ValueError):
            host_runtime.create_binding_file(symlink, "/sock", "tok", "mbx", "gen")

        directory = self.bindings_dir / "adir"
        directory.mkdir()
        with self.assertRaises(ValueError):
            host_runtime.create_binding_file(directory, "/sock", "tok", "mbx", "gen")

    def test_register_task_mailbox_persistence_and_conflict(self):
        reg_dir = self.root / "registrations"
        mailbox_sock = self.root / "mailbox.sock"
        with FakeMailbox(mailbox_sock, admin_token="admin-secret"):
            mbx, token = host_runtime.register_task_mailbox(
                service_socket=mailbox_sock, admin_token="admin-secret",
                registration_key="dorothy:job-1", scope="cc:scope-test",
                team="team-alpha", kind="dorothy", task_dir=reg_dir)
            self.assertTrue(mbx["mailbox_id"])
            self.assertTrue(token)

            safe_name = hashlib.sha256(b"dorothy:job-1").hexdigest()[:16]
            reg_file = reg_dir / f"reg-{safe_name}.json"
            self.assertTrue(reg_file.is_file())
            self.assertEqual(json.loads(reg_file.read_text())["token"], token)

            mbx2, token2 = host_runtime.register_task_mailbox(
                service_socket=mailbox_sock, admin_token="admin-secret",
                registration_key="dorothy:job-1", scope="cc:scope-test",
                team="team-alpha", kind="dorothy", task_dir=reg_dir)
            self.assertEqual(token2, token)
            self.assertEqual(mbx2["mailbox_id"], mbx["mailbox_id"])

            with self.assertRaises(ValueError) as cm:
                host_runtime.register_task_mailbox(
                    service_socket=mailbox_sock, admin_token="admin-secret",
                    registration_key="dorothy:job-1", scope="cc:scope-test",
                    team="team-DIFFERENT", kind="dorothy", task_dir=reg_dir)
            self.assertIn("registration_conflict", str(cm.exception))

    def test_register_task_mailbox_label_and_metadata_conflict(self):
        reg_dir = self.root / "registrations"
        mailbox_sock = self.root / "mailbox.sock"
        with FakeMailbox(mailbox_sock, admin_token="admin-secret"):
            host_runtime.register_task_mailbox(
                service_socket=mailbox_sock, admin_token="admin-secret",
                registration_key="k1", scope="cc:s", label="one", metadata={"a": 1}, task_dir=reg_dir)
            with self.assertRaises(ValueError) as cm:
                host_runtime.register_task_mailbox(
                    service_socket=mailbox_sock, admin_token="admin-secret",
                    registration_key="k1", scope="cc:s", label="two", metadata={"a": 1}, task_dir=reg_dir)
            self.assertIn("registration_conflict", str(cm.exception))
            with self.assertRaises(ValueError) as cm:
                host_runtime.register_task_mailbox(
                    service_socket=mailbox_sock, admin_token="admin-secret",
                    registration_key="k1", scope="cc:s", label="one", metadata={"a": 2}, task_dir=reg_dir)
            self.assertIn("registration_conflict", str(cm.exception))

    def test_register_lock_symlink_refused(self):
        reg_dir = self.root / "registrations"
        reg_dir.mkdir(parents=True, mode=0o700)
        safe_name = hashlib.sha256(b"k2").hexdigest()[:16]
        real_lock = self.root / "elsewhere.lock"
        real_lock.write_text("", encoding="utf-8")
        (reg_dir / f"reg-{safe_name}.lock").symlink_to(real_lock)
        with self.assertRaises(ValueError) as cm:
            host_runtime.register_task_mailbox(
                service_socket=self.root / "nope.sock", admin_token="a",
                registration_key="k2", scope="cc:s", task_dir=reg_dir)
        self.assertIn("lock", str(cm.exception))

    def test_register_admin_token_path_read_failure_is_explicit(self):
        reg_dir = self.root / "registrations"
        with self.assertRaises(ValueError) as cm:
            host_runtime.register_task_mailbox(
                service_socket=self.root / "nope.sock",
                admin_token=self.root / "missing-admin-token",
                registration_key="k3", scope="cc:s", task_dir=reg_dir)
        self.assertIn("admin_token", str(cm.exception))

    def test_binding_for_current_env_override_verifies_service(self):
        mailbox_sock = self.root / "mailbox.sock"
        with FakeMailbox(mailbox_sock, admin_token="admin") as service:
            service.mailboxes["tok-1"] = {"mailbox_id": "mbx-1"}
            binding = self.bindings_dir / "explicit.json"
            binding.write_text(json.dumps({
                "schema": 1, "socket": str(mailbox_sock), "token": "tok-1",
                "mailbox_id": "mbx-1", "generation": "g1"}), encoding="utf-8")
            binding.chmod(0o600)
            with patch.dict(os.environ, {"AGENT_MAILBOX_BINDING": str(binding)}, clear=False):
                self.assertEqual(host_runtime.binding_for_current(), str(binding.resolve()))

            # The service identity must match the binding's mailbox_id.
            binding.write_text(json.dumps({
                "schema": 1, "socket": str(mailbox_sock), "token": "tok-1",
                "mailbox_id": "mbx-OTHER", "generation": "g1"}), encoding="utf-8")
            binding.chmod(0o600)
            with patch.dict(os.environ, {"AGENT_MAILBOX_BINDING": str(binding)}, clear=False):
                with self.assertRaises(RuntimeError) as cm:
                    host_runtime.binding_for_current()
                self.assertIn("binding_mailbox_mismatch", str(cm.exception))

    def test_binding_for_current_env_override_rejects_symlink(self):
        real = self.bindings_dir / "real.json"
        real.write_text("{}", encoding="utf-8")
        real.chmod(0o600)
        link = self.bindings_dir / "link.json"
        link.symlink_to(real)
        with patch.dict(os.environ, {"AGENT_MAILBOX_BINDING": str(link)}, clear=False):
            with self.assertRaises(RuntimeError) as cm:
                host_runtime.binding_for_current()
            self.assertIn("binding_file_unsafe", str(cm.exception))

    def test_binding_for_current_implicit_requires_capture(self):
        class _FailingIpc:
            @staticmethod
            def capture():
                raise ValueError("ipc_origin_environment_missing")

        env = {"CC_PROJECT": "p", "CC_SESSION_KEY": "s", "CC_IPC_CWD": str(self.root)}
        with patch.dict(os.environ, env, clear=False):
            with patch.object(host_runtime, "get_ipc_module", return_value=_FailingIpc()):
                with self.assertRaises(RuntimeError) as cm:
                    host_runtime.binding_for_current()
                self.assertIn("cc_identity_capture_failed", str(cm.exception))

        # No helper at all must fail, never fall back to environment-built target.
        with patch.dict(os.environ, env, clear=False):
            with patch.object(host_runtime, "get_ipc_module", return_value=None):
                with self.assertRaises(RuntimeError) as cm:
                    host_runtime.binding_for_current()
                self.assertIn("cc_ipc_helper_unavailable", str(cm.exception))

    def test_binding_for_current_implicit_uses_captured_target(self):
        mailbox_sock = self.root / "mailbox.sock"
        cc_sock = str(self.root / "cc.sock")
        project, session_key = "test-project", "feishu:chat:user"
        cwd = str(self.root / "workspace")
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        binding = self.bindings_dir / f"{scope}.json"
        with FakeMailbox(mailbox_sock, admin_token="admin") as service:
            service.mailboxes["tok-9"] = {"mailbox_id": "mbx-9", "scope": scope}
            binding.write_text(json.dumps({
                "schema": 1, "socket": str(mailbox_sock), "token": "tok-9",
                "mailbox_id": "mbx-9", "generation": "g", "scope": scope}), encoding="utf-8")
            binding.chmod(0o600)

            class _Ipc:
                @staticmethod
                def capture():
                    return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                            "target": {"project": project, "session_key": session_key,
                                       "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

            with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
                self.assertEqual(host_runtime.binding_for_current(), str(binding.resolve()))

    def test_get_ipc_module_installed_layout(self):
        layout = self.root / "installed"
        scripts = layout / "skills/cc-agent-ipc/scripts"
        helper = layout / "skills/cc-dsh-collaboration/scripts"
        scripts.mkdir(parents=True)
        helper.mkdir(parents=True)
        shutil.copy(SCRIPTS_DIR / "host_runtime.py", scripts / "host_runtime.py")
        (helper / "ipc.py").write_text(
            'MARKER = "installed-layout-stub"\n\n'
            'def capture():\n    raise RuntimeError("unused")\n\n'
            'def receipt_allows_retry(receipt):\n    return False\n',
            encoding="utf-8")
        probe = self.root / "probe.py"
        probe.write_text(
            "import importlib.util, sys\n"
            "spec = importlib.util.spec_from_file_location('installed_host_runtime', sys.argv[1])\n"
            "mod = importlib.util.module_from_spec(spec)\n"
            "spec.loader.exec_module(mod)\n"
            "ipc = mod.get_ipc_module()\n"
            "assert ipc is not None and getattr(ipc, 'MARKER', '') == 'installed-layout-stub', ipc\n",
            encoding="utf-8")
        env = {**os.environ, "PYTHONPATH": ""}
        result = subprocess.run(
            [sys.executable, str(probe), str(scripts / "host_runtime.py")],
            env=env, capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_resolve_mailbox_service_rejects_symlink_token(self):
        run_dir = self.runtime_dir / "run"
        run_dir.mkdir(parents=True)
        (run_dir / "api.sock").write_text("", encoding="utf-8")
        real_token = self.root / "real.token"
        real_token.write_text("secret", encoding="utf-8")
        real_token.chmod(0o600)
        (self.runtime_dir / "admin-token").symlink_to(real_token)
        with patch.dict(os.environ, {"AGENT_MAILBOX_RUNTIME": str(self.runtime_dir)}, clear=False):
            with self.assertRaises(ValueError) as cm:
                host_runtime.resolve_mailbox_service()
            self.assertIn("admin_token_symlink", str(cm.exception))

    def test_binding_index_symlink_rejected(self):
        cc_sock = str(self.root / "cc.sock")
        project, session_key = "test-project", "feishu:chat:user"
        cwd = str(self.root / "workspace")
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        real_index = self.root / "real-index.json"
        real_index.write_text("{}", encoding="utf-8")
        real_index.chmod(0o600)
        (self.bindings_dir / "index.json").symlink_to(real_index)

        class _Ipc:
            @staticmethod
            def capture():
                return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                        "target": {"project": project, "session_key": session_key,
                                   "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

        with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
            with self.assertRaises(ValueError) as cm:
                host_runtime.binding_for_current()
            self.assertIn("binding_index_symlink", str(cm.exception))

    def test_binding_index_written_by_older_code_is_readable(self):
        """The secret-free index may be mode 0644; only binding files are private."""
        mailbox_sock = self.root / "mailbox.sock"
        cc_sock = str(self.root / "cc.sock")
        project, session_key = "test-project", "feishu:chat:user"
        cwd = str(self.root / "workspace")
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        binding = self.root / "elsewhere-binding.json"
        with FakeMailbox(mailbox_sock, admin_token="admin") as service:
            service.mailboxes["tok-x"] = {"mailbox_id": "mbx-x", "scope": scope}
            binding.write_text(json.dumps({
                "schema": 1, "socket": str(mailbox_sock), "token": "tok-x",
                "mailbox_id": "mbx-x", "generation": "g", "scope": scope}), encoding="utf-8")
            binding.chmod(0o600)
            index = self.bindings_dir / "index.json"
            index.write_text(json.dumps({scope: str(binding)}), encoding="utf-8")
            index.chmod(0o644)

            class _Ipc:
                @staticmethod
                def capture():
                    return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                            "target": {"project": project, "session_key": session_key,
                                       "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

            with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
                self.assertEqual(host_runtime.binding_for_current(), str(binding.resolve()))


    def _implicit(self, cc_sock, project, session_key, cwd, binding_data, expected_code):
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        binding = self.bindings_dir / f"{scope}.json"
        binding.write_text(json.dumps(binding_data), encoding="utf-8")
        binding.chmod(0o600)

        class _Ipc:
            @staticmethod
            def capture():
                return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                        "target": {"project": project, "session_key": session_key,
                                   "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

        with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
            with self.assertRaises(RuntimeError) as cm:
                host_runtime.binding_for_current()
            self.assertIn(expected_code, str(cm.exception))

    def test_binding_scope_mismatch_rejected(self):
        cc_sock = str(self.root / "cc.sock")
        # The file is found by the expected scope name but declares another scope.
        self._implicit(cc_sock, "test-project", "feishu:chat:user",
                       str(self.root / "workspace"),
                       {"schema": 1, "socket": str(self.root / "mailbox.sock"), "token": "t",
                        "mailbox_id": "m", "scope": "cc:somewhere-else"},
                       "binding_scope_mismatch")

    def test_binding_missing_scope_rejected_on_implicit_path(self):
        cc_sock = str(self.root / "cc.sock")
        self._implicit(cc_sock, "test-project", "feishu:chat:user",
                       str(self.root / "workspace"),
                       {"schema": 1, "socket": str(self.root / "mailbox.sock"), "token": "t",
                        "mailbox_id": "m"},
                       "binding_scope_mismatch")

    def test_binding_permissions_insecure_rejected(self):
        cc_sock = str(self.root / "cc.sock")
        project, session_key = "test-project", "feishu:chat:user"
        cwd = str(self.root / "workspace")
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        binding = self.bindings_dir / f"{scope}.json"
        binding.write_text(json.dumps({
            "schema": 1, "socket": "/tmp/x.sock", "token": "t", "mailbox_id": "m",
            "scope": scope}), encoding="utf-8")
        binding.chmod(0o644)

        class _Ipc:
            @staticmethod
            def capture():
                return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                        "target": {"project": project, "session_key": session_key,
                                   "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

        with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
            with self.assertRaises(ValueError) as cm:
                host_runtime.binding_for_current()
            self.assertIn("binding_permissions_insecure", str(cm.exception))

    def test_binding_schema_invalid_rejected(self):
        cc_sock = str(self.root / "cc.sock")
        self._implicit(cc_sock, "test-project", "feishu:chat:user",
                       str(self.root / "workspace"),
                       {"schema": 1, "socket": "/tmp/x.sock", "mailbox_id": "m"},
                       "binding_schema_invalid")

    def test_binding_relative_socket_rejected(self):
        cc_sock = str(self.root / "cc.sock")
        self._implicit(cc_sock, "test-project", "feishu:chat:user",
                       str(self.root / "workspace"),
                       {"schema": 1, "socket": "relative.sock", "token": "t", "mailbox_id": "m"},
                       "binding_schema_invalid")

    def test_binding_service_unavailable_rejected(self):
        cc_sock = str(self.root / "cc.sock")
        project, session_key = "test-project", "feishu:chat:user"
        cwd = str(self.root / "workspace")
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        binding = self.bindings_dir / f"{scope}.json"
        binding.write_text(json.dumps({
            "schema": 1, "socket": str(self.root / "missing.sock"), "token": "t",
            "mailbox_id": "m", "scope": scope}), encoding="utf-8")
        binding.chmod(0o600)

        class _Ipc:
            @staticmethod
            def capture():
                return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                        "target": {"project": project, "session_key": session_key,
                                   "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

        with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
            with self.assertRaises(RuntimeError) as cm:
                host_runtime.binding_for_current()
            self.assertIn("binding_service_unavailable", str(cm.exception))

    def test_binding_for_current_rejects_service_scope_mismatch(self):
        """The local scope field alone must not prove the mailbox's chat."""
        mailbox_sock = self.root / "mailbox.sock"
        cc_sock = str(self.root / "cc.sock")
        project, session_key = "test-project", "feishu:chat:user"
        cwd = str(self.root / "workspace")
        scope = host_runtime.compute_scope(project, session_key, cwd, cc_sock)
        binding = self.bindings_dir / f"{scope}.json"
        with FakeMailbox(mailbox_sock, admin_token="admin") as service:
            # mailbox_id and the binding's local scope agree, but the service
            # reports a different scope (the token belongs to another chat).
            service.mailboxes["tok-1"] = {"mailbox_id": "mbx-1", "scope": "cc:another-chat"}
            binding.write_text(json.dumps({
                "schema": 1, "socket": str(mailbox_sock), "token": "tok-1",
                "mailbox_id": "mbx-1", "generation": "g", "scope": scope}), encoding="utf-8")
            binding.chmod(0o600)

            class _Ipc:
                @staticmethod
                def capture():
                    return {"protocol": "cc-connect-session-ipc-v1", "socket": cc_sock,
                            "target": {"project": project, "session_key": session_key,
                                       "session_id": "s1", "agent_session_id": "a1", "cwd": cwd}}

            with patch.object(host_runtime, "get_ipc_module", return_value=_Ipc()):
                with self.assertRaises(RuntimeError) as cm:
                    host_runtime.binding_for_current()
                self.assertIn("binding_scope_mismatch", str(cm.exception))

    def test_get_ipc_module_ignores_unrelated_ambient_ipc(self):
        decoy_dir = self.root / "decoy"
        decoy_dir.mkdir()
        (decoy_dir / "ipc.py").write_text('MARKER = "decoy"\n', encoding="utf-8")
        good_dir = self.root / "good"
        good_dir.mkdir()
        (good_dir / "ipc.py").write_text(
            'MARKER = "good"\n\n'
            'def capture():\n    raise RuntimeError("unused")\n\n'
            'def receipt_allows_retry(receipt):\n    return False\n',
            encoding="utf-8")
        with patch.object(host_runtime, "_HELPER_MODULE_CACHE", {}), \
             patch("sys.path", [str(decoy_dir)] + list(sys.path)):
            # An explicit trusted helper always wins over an importable decoy.
            helper = host_runtime.get_ipc_module(helper_dir=str(good_dir))
            self.assertEqual(getattr(helper, "MARKER", None), "good")
            # Without a helper dir, an unrelated "ipc" is never trusted.
            helper = host_runtime.get_ipc_module()
            self.assertNotEqual(getattr(helper, "MARKER", None), "decoy")


if __name__ == "__main__":
    unittest.main()
