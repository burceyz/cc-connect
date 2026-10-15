"""Tests for cc_adapter protocol handling, recovery, and lifecycle."""

import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "skill/scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cc_adapter
import host_runtime


class MailboxStub:
    """A tiny in-memory stand-in for the subset of agent-mailbox the adapter uses."""

    def __init__(self, messages=None):
        self.messages = []
        for i, message in enumerate(messages or [], start=1):
            entry = dict(message)
            entry.setdefault("seq", i)
            entry.setdefault("state", "pending")
            entry.setdefault("received_at", 0)
            self.messages.append(entry)
        self.claim_results = []
        self.claims = {}
        self.claim_calls = []
        self.receipts = []
        self.lossy_claim = False

    def _inbox(self, after):
        after = int(after or 0)
        visible = [m for m in self.messages if m["seq"] > after]
        cursor = str(max((m["seq"] for m in visible), default=after))
        return {"messages": [dict(m) for m in visible], "cursor": cursor}

    def __call__(self, sock, method, path, token=None, payload=None, timeout=None):
        if method == "GET" and path.startswith("/v1/inbox"):
            after = 0
            if "after=" in path:
                after = path.split("after=", 1)[1]
            return self._inbox(after)
        if method == "POST" and path == "/v1/claim":
            request_id = payload["request_id"]
            self.claim_calls.append(request_id)
            if request_id in self.claims:
                message, attempt = self.claims[request_id]
                return {"message": message, "attempt_id": attempt}
            if not self.claim_results:
                self.claims[request_id] = (None, None)
                return {"message": None, "attempt_id": None}
            message, attempt = self.claim_results.pop(0)
            self.claims[request_id] = (message, attempt)
            for entry in self.messages:
                if entry["message_id"] == message["message_id"]:
                    entry["state"] = "dispatching"
            if self.lossy_claim:
                self.lossy_claim = False
                raise host_runtime.MailboxTransportError("simulated lost claim response")
            return {"message": message, "attempt_id": attempt}
        if method == "POST" and path == "/v1/receipt":
            self.receipts.append(payload)
            return {"message": {"message_id": payload["message_id"]}}
        raise AssertionError(f"unexpected mailbox call {method} {path}")


def message(msg_id, task="t1", sender="mbx-sender"):
    return {"message_id": msg_id, "task_id": task, "sender_id": sender, "body": "x"}


class TestCCAdapter(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="test-cc-adapter-")
        self.root = Path(self.tmp.name).resolve()
        self.runtime_dir = self.root / "runtime"
        self.runtime_dir.mkdir(parents=True, mode=0o700)
        self.env = patch.dict(os.environ, {"AGENT_MAILBOX_RUNTIME": str(self.runtime_dir)}, clear=False)
        self.env.start()

        self.admin_token_file = self.root / "admin.token"
        self.admin_token_file.write_text("admin-secret-token\n", encoding="utf-8")
        self.admin_token_file.chmod(0o600)

        self.config_data = {
            "service_socket": str(self.root / "mailbox.sock"),
            "admin_token_path": str(self.admin_token_file),
            "cc_socket": str(self.root / "cc_api.sock"),
            "session_index_path": str(self.root / "sessions.json"),
            "runtime_dir": str(self.runtime_dir),
            "cc_project": "agentmailbox",
            "allowed_projects": ["agentmailbox"],
            "cc_cwd": str(self.root / "proj"),
            "poll_interval": 0.1,
        }
        self.config_file = self.root / "config.json"
        self.config_file.write_text(json.dumps(self.config_data), encoding="utf-8")

    def tearDown(self):
        self.env.stop()
        self.tmp.cleanup()

    def _adapter(self):
        return cc_adapter.CcAdapter(self.config_file)

    # ------------------------------------------------------------- discovery

    def test_discover_active_sessions_real_shape(self):
        self.config_data["cc_cwd"] = str(self.root / "proj")
        (self.root / "sessions.json").write_text(json.dumps({
            "active_session": {"feishu:chat:user": "s1", "telegram:chat:other": "s2"},
            "sessions": {"s1": {"id": "s1", "agent_session_id": "native-id",
                                "agent_type": "codex", "history": []}},
        }), encoding="utf-8")
        adapter = self._adapter()
        try:
            found = adapter.discover_active_sessions()
            self.assertEqual(found, [
                ("agentmailbox", "feishu:chat:user", str(self.root / "proj")),
                ("agentmailbox", "telegram:chat:other", str(self.root / "proj")),
            ])
        finally:
            adapter.close()

    # ------------------------------------------------------------ chat sync

    def _chat_target(self, session_id="cc-s1", agent_id="native-1"):
        return 200, {
            "protocol": cc_adapter.CC_PROTOCOL,
            "supports_live_input": True,
            "supports_chat_routing": True,
            "target": {"project": "agentmailbox", "session_key": "feishu:chat:user",
                       "session_id": session_id, "agent_session_id": agent_id,
                       "cwd": str(self.root / "proj")},
        }

    def test_sync_chat_rejects_returned_other_chat(self):
        adapter = self._adapter()
        try:
            def fake_cc(sock, method, path, payload=None, timeout=None):
                status, res = self._chat_target()
                res["target"]["session_key"] = "feishu:chat:ATTACKER"
                return status, res

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.register_task_mailbox") as reg:
                scope = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
                self.assertIsNone(scope)
                reg.assert_not_called()
        finally:
            adapter.close()

    def test_sync_chat_registers_then_switches_generation(self):
        adapter = self._adapter()
        try:
            with patch("cc_adapter.call_cc_socket", side_effect=lambda *a, **k: self._chat_target()), \
                 patch("cc_adapter.register_task_mailbox", return_value=({"mailbox_id": "mbx-1"}, "tok-1")), \
                 patch("cc_adapter.activate_mailbox") as activate, \
                 patch("cc_adapter.deactivate_mailbox") as deactivate:
                scope = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
                self.assertIsNotNone(scope)
                gen1 = adapter.atlas_mailboxes[scope]["generation"]
                self.assertTrue(gen1)
                activate.assert_called_once()
                binding = json.loads((adapter.bindings_dir / f"{scope}.json").read_text())
                self.assertEqual(binding["generation"], gen1)

                # /new: a new CC session id -> a genuinely new generation.
                activate.reset_mock()
                with patch("cc_adapter.call_cc_socket",
                           side_effect=lambda *a, **k: self._chat_target("cc-s2", "native-2")):
                    scope2 = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
                self.assertEqual(scope2, scope)
                deactivate.assert_called_once_with(adapter.service_socket, "tok-1", gen1)
                activate.assert_called_once()
                gen2 = adapter.atlas_mailboxes[scope]["generation"]
                self.assertNotEqual(gen1, gen2)
                self.assertEqual(adapter.atlas_mailboxes[scope]["phase"], "active")
                binding = json.loads((adapter.bindings_dir / f"{scope}.json").read_text())
                self.assertEqual(binding["generation"], gen2)
        finally:
            adapter.close()

    def test_activate_failure_does_not_publish_new_binding(self):
        adapter = self._adapter()
        try:
            with patch("cc_adapter.call_cc_socket", side_effect=lambda *a, **k: self._chat_target()), \
                 patch("cc_adapter.register_task_mailbox", return_value=({"mailbox_id": "mbx-1"}, "tok-1")), \
                 patch("cc_adapter.activate_mailbox"):
                scope = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
            gen1 = adapter.atlas_mailboxes[scope]["generation"]

            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda *a, **k: self._chat_target("cc-s2", "native-2")), \
                 patch("cc_adapter.deactivate_mailbox") as deactivate, \
                 patch("cc_adapter.activate_mailbox", side_effect=RuntimeError("service down")):
                result = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
            self.assertIsNone(result)
            deactivate.assert_called_once()
            state = adapter.atlas_mailboxes[scope]
            self.assertEqual(state["phase"], "switching")
            self.assertEqual(state["generation"], gen1)
            self.assertTrue(state["pending_generation"])
            binding = json.loads((adapter.bindings_dir / f"{scope}.json").read_text())
            self.assertEqual(binding["generation"], gen1)
        finally:
            adapter.close()

    def test_native_id_change_on_same_session_advances_generation(self):
        adapter = self._adapter()
        try:
            with patch("cc_adapter.call_cc_socket", side_effect=lambda *a, **k: self._chat_target()), \
                 patch("cc_adapter.register_task_mailbox", return_value=({"mailbox_id": "mbx-1"}, "tok-1")), \
                 patch("cc_adapter.activate_mailbox"):
                scope = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
            gen1 = adapter.atlas_mailboxes[scope]["generation"]

            # Same CC session id, but the trusted native thread changed.
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda *a, **k: self._chat_target("cc-s1", "native-999")), \
                 patch("cc_adapter.heartbeat_mailbox") as heartbeat, \
                 patch("cc_adapter.deactivate_mailbox") as deactivate, \
                 patch("cc_adapter.activate_mailbox") as activate:
                result = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))

            self.assertEqual(result, scope)
            heartbeat.assert_not_called()          # never force-write the old generation
            deactivate.assert_called_once()        # the old generation is ended, no process killed
            activate.assert_called_once()
            state = adapter.atlas_mailboxes[scope]
            self.assertEqual(state["phase"], "active")
            self.assertNotEqual(state["generation"], gen1)
            self.assertEqual(state["agent_session_id"], "native-999")
            binding = json.loads((adapter.bindings_dir / f"{scope}.json").read_text())
            self.assertEqual(binding["generation"], state["generation"])
        finally:
            adapter.close()

    def test_empty_native_id_is_filled_by_heartbeat(self):
        adapter = self._adapter()
        try:
            with patch("cc_adapter.call_cc_socket", side_effect=lambda *a, **k: self._chat_target(agent_id="")), \
                 patch("cc_adapter.register_task_mailbox", return_value=({"mailbox_id": "mbx-1"}, "tok-1")), \
                 patch("cc_adapter.activate_mailbox"):
                scope = adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
            self.assertEqual(adapter.atlas_mailboxes[scope]["agent_session_id"], "")

            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda *a, **k: self._chat_target(agent_id="native-new")), \
                 patch("cc_adapter.heartbeat_mailbox") as heartbeat:
                adapter.sync_chat("agentmailbox", "feishu:chat:user", str(self.root / "proj"))
            heartbeat.assert_called_once()
            self.assertEqual(adapter.atlas_mailboxes[scope]["agent_session_id"], "native-new")
        finally:
            adapter.close()

    def _retire(self, adapter, scope, generation, withdrawn=True, ended=None):
        folder = adapter.runtime_dir / 'withdrawals'
        folder.mkdir(exist_ok=True)
        cc_adapter.atomic_write_json(folder / (scope + '.json'), {
            'schema': 1, 'scope': scope, 'mailbox_id': 'mbx-1', 'generation': generation,
            'withdrawn': withdrawn, 'retired_generations': ended or [generation]}, 'withdrawal')

    def test_leave_rejoin_and_late_old_departure_preserve_the_new_generation(self):
        adapter = self._adapter()
        try:
            with patch('cc_adapter.call_cc_socket', side_effect=lambda *a, **k: self._chat_target()), \
                 patch('cc_adapter.register_task_mailbox', return_value=({'mailbox_id': 'mbx-1'}, 'tok-1')), \
                 patch('cc_adapter.activate_mailbox') as activate, \
                 patch('cc_adapter.deactivate_mailbox') as deactivate, \
                 patch('cc_adapter.heartbeat_mailbox'):
                scope = adapter.sync_chat('agentmailbox', 'feishu:chat:user', str(self.root / 'proj'))
                old = adapter.atlas_mailboxes[scope]['generation']
                self._retire(adapter, scope, old)
                self.assertIsNone(adapter.sync_chat('agentmailbox', 'feishu:chat:user', str(self.root / 'proj')))
                self.assertEqual(adapter.atlas_mailboxes[scope]['phase'], 'retired')
                self.assertNotIn(scope, adapter.active_targets)
                self._retire(adapter, scope, old, withdrawn=False)
                self.assertEqual(adapter.sync_chat('agentmailbox', 'feishu:chat:user', str(self.root / 'proj')), scope)
                current = adapter.atlas_mailboxes[scope]['generation']
                self.assertNotEqual(old, current)
                activate.reset_mock()
                deactivate.reset_mock()
                self._retire(adapter, scope, old)
                self.assertEqual(adapter.sync_chat('agentmailbox', 'feishu:chat:user', str(self.root / 'proj')), scope)
                self.assertEqual(adapter.atlas_mailboxes[scope]['generation'], current)
                activate.assert_not_called()
                deactivate.assert_not_called()
                self.assertTrue(adapter._withdrawn(scope, old))
                self.assertFalse(adapter._withdrawn(scope, current))
        finally:
            adapter.close()

    def test_retired_delivery_is_query_only_even_after_rejoin(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch('cc_adapter.call_cc_socket', side_effect=lambda s, m, p, payload=None, **k:
                       (200, {'input': payload, 'status': 'unknown'})), patch('cc_adapter.request_unix'):
                adapter._deliver_to_cc(scope, message('msg-retired'), 'att-1', 'tok-1', 'gen-1',
                                       adapter.active_targets[scope])
            path = next(adapter.events_dir.glob('*.json'))
            saved = json.loads(path.read_bytes())
            original_input = saved['input']
            self._retire(adapter, scope, 'gen-1', withdrawn=False)
            adapter.atlas_mailboxes[scope]['generation'] = 'gen-2'
            adapter.active_targets[scope]['session_id'] = 'cc-s2'
            for status in ('delivered', 'unknown', 'waiting'):
                with self.subTest(status=status):
                    calls = []
                    def cc(sock, method, route, payload=None, **kwargs):
                        calls.append(method)
                        return 200, {'input': original_input, 'status': status,
                                     'code': 'turn_not_ready', 'boot': 'a' * 32,
                                     'delivery_target': {'session_id': 'cc-s1'}}
                    with patch('cc_adapter.call_cc_socket', side_effect=cc), patch('cc_adapter.request_unix'):
                        adapter._query_saved_event(path, json.loads(json.dumps(saved)))
                    self.assertEqual(calls, ['GET'])
                    self.assertEqual(json.loads(path.read_bytes())['input'], original_input)
            with patch('cc_adapter.call_cc_socket') as cc:
                self.assertIsNone(adapter._deliver_to_cc(scope, message('new-old'), 'att-2', 'tok-1', 'gen-1',
                                                        adapter.active_targets[scope]))
                cc.assert_not_called()
        finally:
            adapter.close()

    # ------------------------------------------------------------- delivery

    def _scope_target(self, adapter):
        scope = "cc:scope-1"
        adapter.atlas_mailboxes[scope] = {
            "scope": scope, "mailbox_id": "mbx-1", "token": "tok-1", "generation": "gen-1",
            "phase": "active", "cc_session_id": "cc-s1", "agent_session_id": "native-1"}
        adapter.active_targets[scope] = {
            "project": "agentmailbox", "session_key": "feishu:chat:user",
            "session_id": "cc-s1", "agent_session_id": "native-1", "cwd": str(self.root / "proj")}
        return scope

    def test_deliver_event_id_prompt_and_accepted_receipt(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            captured = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                captured.append((method, path, payload))
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix") as mailbox:
                adapter._deliver_to_cc(scope, message("msg-007"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])

            self.assertEqual([c[:2] for c in captured], [("POST", "/ipc/event")])
            payload = captured[0][2]
            self.assertEqual(payload["routing"], "chat")
            self.assertTrue(re.fullmatch(r"[a-f0-9]{64}", payload["event_id"]))
            self.assertIn("mailbox.py show --id msg-007", payload["prompt"])
            self.assertEqual(set(payload["target"]),
                             {"project", "session_key", "session_id", "agent_session_id", "cwd"})
            mailbox.assert_called_once()
            receipt = mailbox.call_args.kwargs["payload"]
            self.assertEqual(receipt["state"], "accepted")
            self.assertEqual(receipt["attempt_id"], "att-1")
        finally:
            adapter.close()

    def test_deliver_rejects_missing_or_wrong_receipt_input(self):
        for mode in ("omit", "corrupt"):
            adapter = self._adapter()
            try:
                scope = self._scope_target(adapter)

                def fake_cc(sock, method, path, payload=None, timeout=None, mode=mode):
                    res = {"status": "delivered", "input": payload,
                           "delivery_target": {"session_id": "cc-s1"}}
                    if mode == "omit":
                        res.pop("input")
                    else:
                        res["input"] = {**payload, "prompt": "tampered"}
                    return 200, res

                with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                     patch("cc_adapter.request_unix") as mailbox:
                    adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                           adapter.active_targets[scope])
                mailbox.assert_not_called()
                event_id = __import__("hashlib").sha256(
                    f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
                rec = json.loads((adapter.events_dir / f"{event_id}.json").read_text())
                self.assertNotEqual(rec["status"], "accepted")
            finally:
                adapter.close()

    def test_existing_event_is_replayed_not_rebuilt(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda s, m, p, payload=None, timeout=None:
                       (200, {"status": "waiting", "code": "turn_not_ready",
                              "input": payload, "boot": "a" * 32})), \
                 patch("cc_adapter.request_unix"):
                # Build the first event with the original target.
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       dict(adapter.active_targets[scope]))
            event_id = hashlib.sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            original = json.loads((adapter.events_dir / f"{event_id}.json").read_text())["input"]

            # A later call with a different (post-/new) target must reuse the saved input.
            changed_target = dict(adapter.active_targets[scope])
            changed_target.update({"session_id": "cc-s2", "agent_session_id": "native-2"})
            captured = []
            methods = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                methods.append(method)
                if method == "GET":
                    return 200, {"status": "waiting", "code": "turn_not_ready",
                                 "input": original, "boot": "a" * 32}
                captured.append(payload)
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s2"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       changed_target)
            self.assertEqual(methods, ["GET", "POST"])  # query first, then retry the saved input
            self.assertEqual(captured[0], original)
            saved = json.loads((adapter.events_dir / f"{event_id}.json").read_text())
            self.assertEqual(saved["input"], original)
        finally:
            adapter.close()

    def test_existing_event_is_queried_not_reposted(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            # First delivery creates and POSTs the event (CC answered unknown).
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda s, m, p, payload=None, timeout=None:
                       (200, {"status": "unknown", "code": "x", "input": payload})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            event_id = __import__("hashlib").sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            saved = json.loads((adapter.events_dir / f"{event_id}.json").read_text())
            self.assertEqual(saved["status"], "unknown")

            calls = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                calls.append(method)
                if method == "GET":
                    return 200, {"status": "unknown", "code": "x", "input": saved["input"]}
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            self.assertEqual(calls, ["GET"])  # an existing event is never re-POSTed
        finally:
            adapter.close()

    def test_existing_event_get_failure_is_not_reposted(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda s, m, p, payload=None, timeout=None:
                       (200, {"status": "dispatching", "input": payload})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            calls = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                calls.append(method)
                raise OSError("CC socket unavailable")

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            self.assertEqual(calls, ["GET"])
        finally:
            adapter.close()

    def test_existing_event_get_404_is_not_reposted(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda s, m, p, payload=None, timeout=None:
                       (200, {"status": "dispatching", "input": payload})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            calls = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                calls.append(method)
                return 404, {"code": "ipc_receipt_unavailable"}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            self.assertEqual(calls, ["GET"])  # no receipt is not proof of "unsent"
        finally:
            adapter.close()

    def test_existing_event_get_input_mismatch_is_not_reposted(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda s, m, p, payload=None, timeout=None:
                       (200, {"status": "dispatching", "input": payload})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            calls = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                calls.append(method)
                return 200, {"status": "waiting", "code": "turn_not_ready",
                             "input": {"event_id": "0" * 64}, "boot": "a" * 32}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            self.assertEqual(calls, ["GET"])  # a different input never authorizes a retry
        finally:
            adapter.close()

    def test_missing_event_file_rebuilds_from_saved_claim_target(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            original_target = {"project": "agentmailbox", "session_key": "feishu:chat:user",
                               "session_id": "cc-old", "agent_session_id": "native-old",
                               "cwd": str(self.root / "proj")}
            claim_path = adapter._claim_path(scope, "gen-1")
            adapter._save_claim(claim_path, {
                "scope": scope, "generation": "gen-1", "request_id": "claim-atlas-x-gen-1-1",
                "seq": 1, "status": "claimed", "target": original_target,
                "message_id": "msg-1", "attempt_id": "att-1", "task_id": "t1",
                "sender_id": "mbx-sender", "at": 0,
            })
            # The active target moved on after a /new; recovery must use the saved one.
            adapter.active_targets[scope] = {**original_target, "session_id": "cc-new",
                                             "agent_session_id": "native-new"}
            posts = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                posts.append(payload)
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-old"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter.reconcile_events()

            self.assertEqual(len(posts), 1)
            self.assertEqual(posts[0]["target"]["session_id"], "cc-old")
            record = json.loads(claim_path.read_text())
            self.assertEqual(record["status"], "delivered")
        finally:
            adapter.close()

    def test_retry_post_non_200_is_not_applied_as_receipt(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       side_effect=lambda s, m, p, payload=None, timeout=None:
                       (200, {"status": "waiting", "code": "turn_not_ready",
                              "input": payload, "boot": "a" * 32})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            event_id = __import__("hashlib").sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            saved = json.loads((adapter.events_dir / f"{event_id}.json").read_text())["input"]

            def fake_cc(sock, method, path, payload=None, timeout=None):
                if method == "GET":
                    return 200, {"status": "waiting", "code": "turn_not_ready",
                                 "input": saved, "boot": "a" * 32}
                return 400, {"code": "ipc_input_invalid"}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix") as mailbox:
                adapter.reconcile_events()
            record = json.loads((adapter.events_dir / f"{event_id}.json").read_text())
            self.assertEqual(record["status"], "unknown")
            states = [c.kwargs["payload"]["state"] for c in mailbox.call_args_list]
            self.assertNotIn("accepted", states)
        finally:
            adapter.close()

    def test_reconcile_get_failure_does_not_retry(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       return_value=(200, {"status": "waiting", "code": "turn_not_ready",
                                           "input": {"event_id": "x"}, "boot": "a" * 32})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            event_id = __import__("hashlib").sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            posts = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                posts.append((method, path))
                if method == "GET":
                    return 500, {"code": "ipc_query_failed"}
                return 200, {"status": "waiting", "code": "turn_not_ready", "input": payload}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter.reconcile_events()
            self.assertEqual(posts, [("GET", f"/ipc/event?event_id={event_id}")])
        finally:
            adapter.close()

    def test_reconcile_retries_only_with_matching_input(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       return_value=(200, {"status": "waiting", "code": "turn_not_ready",
                                           "input": {"event_id": "x"}, "boot": "a" * 32})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            event_id = __import__("hashlib").sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            saved_input = json.loads(
                (adapter.events_dir / f"{event_id}.json").read_text())["input"]
            posts = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                if method == "GET":
                    return 200, {"status": "waiting", "code": "turn_not_ready",
                                 "input": saved_input, "boot": "a" * 32}
                posts.append(payload)
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix") as mailbox:
                adapter.reconcile_events()
            self.assertEqual(posts, [saved_input])
            states = [c.kwargs["payload"]["state"] for c in mailbox.call_args_list]
            self.assertIn("accepted", states)
        finally:
            adapter.close()

    def test_prompt_is_byte_bounded_for_long_cjk_task(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            captured = {}

            def fake_cc(sock, method, path, payload=None, timeout=None):
                captured.update(payload)
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1", task="任" * 4000), "att-1",
                                       "tok-1", "gen-1", adapter.active_targets[scope])
            prompt = captured["prompt"]
            self.assertLessEqual(len(prompt.encode("utf-8")), 8192)
            self.assertIn("mailbox.py show --id msg-1", prompt)
        finally:
            adapter.close()

    def test_non_200_post_records_unknown_receipt(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket", return_value=(400, {"code": "ipc_input_invalid"})), \
                 patch("cc_adapter.request_unix") as mailbox:
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            mailbox.assert_called_once()
            self.assertEqual(mailbox.call_args.kwargs["payload"]["state"], "unknown")
            event_id = hashlib.sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            record = json.loads((adapter.events_dir / f"{event_id}.json").read_text())
            self.assertEqual(record["status"], "unknown")
        finally:
            adapter.close()

    def test_reconcile_survives_foreign_ipc_helper(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            with patch("cc_adapter.call_cc_socket",
                       return_value=(200, {"status": "waiting", "code": "turn_not_ready",
                                           "input": {"event_id": "x"}})), \
                 patch("cc_adapter.request_unix"):
                adapter._deliver_to_cc(scope, message("msg-1"), "att-1", "tok-1", "gen-1",
                                       adapter.active_targets[scope])
            event_id = hashlib.sha256(f"cc:{scope}:msg-1:att-1".encode()).hexdigest()
            saved = json.loads((adapter.events_dir / f"{event_id}.json").read_text())["input"]
            posts = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                if method == "GET":
                    return 200, {"status": "waiting", "code": "turn_not_ready",
                                 "input": saved, "boot": "a" * 32}
                posts.append(payload)
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            class _Foreign:
                pass  # no receipt_allows_retry

            with patch("cc_adapter.get_ipc_module", return_value=_Foreign()), \
                 patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix"):
                adapter.reconcile_events()  # must not raise AttributeError
            self.assertEqual(posts, [])  # unsent evidence cannot be proven
        finally:
            adapter.close()

    # --------------------------------------------------------------- claims

    def test_process_inbox_delivers_backlog(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            stub = MailboxStub([message("msg-1"), message("msg-2")])
            stub.claim_results = [(stub.messages[0], "att-1"), (stub.messages[1], "att-2")]
            delivered = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                delivered.append(payload["event_id"])
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix", side_effect=stub):
                adapter.process_inbox(scope)
            self.assertEqual(len(delivered), 2)
            self.assertEqual(len(set(delivered)), 2)
            self.assertEqual(len(stub.claim_calls), 2)
            for call in stub.claim_calls:
                self.assertIn("gen-1", call)
        finally:
            adapter.close()

    def test_claim_response_loss_is_replayed(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            stub = MailboxStub([message("msg-1")])
            stub.claim_results = [(stub.messages[0], "att-1")]
            stub.lossy_claim = True
            delivered = []

            def fake_cc(sock, method, path, payload=None, timeout=None):
                delivered.append(payload["event_id"])
                return 200, {"status": "delivered", "input": payload,
                             "delivery_target": {"session_id": "cc-s1"}}

            with patch("cc_adapter.call_cc_socket", side_effect=fake_cc), \
                 patch("cc_adapter.request_unix", side_effect=stub):
                adapter.process_inbox(scope)
                self.assertEqual(delivered, [])
                claim_file = adapter._claim_path(scope, "gen-1")
                record = json.loads(claim_file.read_text())
                self.assertEqual(record["status"], "requested")
                request_id = record["request_id"]

                adapter.reconcile_events()

            self.assertEqual(delivered and len(delivered), 1)
            self.assertEqual(stub.claim_calls[0], request_id)
            self.assertEqual(stub.claim_calls[1], request_id)
            record = json.loads(adapter._claim_path(scope, "gen-1").read_text())
            self.assertEqual(record["status"], "delivered")
            self.assertEqual(record["attempt_id"], "att-1")
            self.assertEqual(record["message_id"], "msg-1")
        finally:
            adapter.close()

    def test_acknowledged_pending_does_not_claim(self):
        adapter = self._adapter()
        try:
            scope = self._scope_target(adapter)
            acked = message("msg-acked")
            acked["received_at"] = 123
            stub = MailboxStub([acked])
            with patch("cc_adapter.call_cc_socket") as cc, \
                 patch("cc_adapter.request_unix", side_effect=stub):
                adapter.process_inbox(scope)
            cc.assert_not_called()
            self.assertEqual(stub.claim_calls, [])
        finally:
            adapter.close()

    # ---------------------------------------------------------------- locks

    def test_exclusive_daemon_lock(self):
        adapter1 = self._adapter()
        try:
            with self.assertRaises(RuntimeError) as cm:
                self._adapter()
            self.assertIn("cc_adapter_already_running", str(cm.exception))
        finally:
            adapter1.close()

    def test_config_symlink_rejected(self):
        link = self.root / "link-config.json"
        link.symlink_to(self.config_file)
        with self.assertRaises(RuntimeError) as cm:
            cc_adapter.CcAdapter(link)
        self.assertIn("config_file_symlink", str(cm.exception))

    def test_lock_symlink_rejected(self):
        real_lock = self.root / "elsewhere.lock"
        real_lock.write_text("", encoding="utf-8")
        (self.runtime_dir / "cc_adapter.lock").symlink_to(real_lock)
        with self.assertRaises(RuntimeError) as cm:
            self._adapter()
        self.assertIn("cc_adapter_lock_symlink", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
