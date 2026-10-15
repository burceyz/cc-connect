"""End-to-end tests against the real Go agent-mailbox binary and a fake CC.

Only ephemeral runtime directories under the system temp area are mutated; no
production socket, database, or config is touched. The fake CC mirrors
``core/session_ipc.go`` and the mailbox is the real service binary.
"""

import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "skill/scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from client import Client
import cc_adapter
import host_runtime
from support import FakeCC, wait_for

MODULE_ROOT = Path(__file__).resolve().parents[1]


def _resolve_binary():
    """AGENT_MAILBOX_BIN, else a module-relative build path.

    No host-specific absolute path is published; a caller-supplied environment
    variable carries any machine-local binary location.
    """
    override = os.environ.get("AGENT_MAILBOX_BIN")
    if override:
        return Path(override)
    for candidate in (MODULE_ROOT / "build" / "agent-mailbox",
                      MODULE_ROOT / "bin" / "agent-mailbox"):
        if candidate.is_file():
            return candidate
    return MODULE_ROOT / "build" / "agent-mailbox"


BINARY = _resolve_binary()


class TestRealServiceIntegration(unittest.TestCase):
    def setUp(self):
        if not BINARY.is_file():
            self.skipTest(f"agent-mailbox binary not found: {BINARY}")
        # /tmp keeps Unix socket paths short and is the same ephemeral area the
        # contract's own probe uses; no production path is touched.
        self.tmp = tempfile.TemporaryDirectory(prefix="mbx-int-", dir="/tmp")
        self.root = Path(self.tmp.name).resolve()
        self.project = "agentmailbox"
        self.session_key = "feishu:chat:user"
        self.cwd = self.root / "proj"
        self.cwd.mkdir(parents=True)
        self.service_dir = self.root / "service"
        self.service_dir.mkdir()
        self.log = open(self.root / "service.log", "w")
        self.proc = subprocess.Popen(
            [str(BINARY), "serve", "--runtime", str(self.service_dir)],
            stdout=self.log, stderr=self.log)
        sock = self.service_dir / "api.sock"
        self.assertTrue(wait_for(sock.exists, timeout=15), "service socket did not appear")
        self.service_socket = sock
        self.admin_token = (self.service_dir / "admin-token").read_text().strip()

        self.cc = FakeCC(self.root / "cc.sock", project=self.project,
                         session_key=self.session_key, cwd=str(self.cwd),
                         session_id="cc-s1", agent_session_id="native-1").start()

        (self.root / "sessions.json").write_text(json.dumps({
            "active_session": {self.session_key: "s1"},
            "sessions": {"s1": {"id": "s1", "agent_session_id": "native-1",
                                "agent_type": "codex", "history": []}},
        }), encoding="utf-8")

        self.config_file = self.root / "adapter.json"
        self.config_file.write_text(json.dumps({
            "service_socket": str(self.service_socket),
            "admin_token_path": str(self.service_dir / "admin-token"),
            "cc_socket": str(self.root / "cc.sock"),
            "session_index_path": str(self.root / "sessions.json"),
            "runtime_dir": str(self.root / "adapter"),
            "cc_project": self.project,
            "allowed_projects": [self.project],
            "cc_cwd": str(self.cwd),
            "poll_interval": 0.1,
        }), encoding="utf-8")

        self.env = patch.dict(os.environ, {"AGENT_MAILBOX_RUNTIME": str(self.root / "adapter")}, clear=False)
        self.env.start()
        self.adapter = cc_adapter.CcAdapter(self.config_file)
        self.scope = self.adapter.sync_chat(self.project, self.session_key, str(self.cwd))
        self.assertIsNotNone(self.scope)
        self.atlas = self.adapter.atlas_mailboxes[self.scope]
        self.atlas_client = Client(str(self.service_socket), self.atlas["token"])

    def tearDown(self):
        try:
            self.adapter.close()
        except Exception:
            pass
        self.env.stop()
        self.cc.stop()
        self.proc.terminate()
        try:
            self.proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.log.close()
        self.tmp.cleanup()

    def _send(self, body):
        token = secrets.token_hex(24)
        admin = Client(str(self.service_socket), self.admin_token)
        worker = admin.request("POST", "/v1/register", {
            "registration_key": f"worker-{token[:8]}",
            "token": token,
            "scope": self.scope,
            "team": "",
            "label": "worker",
            "kind": "probe",
            "coordinator": False,
            "parent_id": self.atlas["mailbox_id"],
            "metadata": {},
        })["mailbox"]
        client = Client(str(self.service_socket), token)
        return client.send({
            "client_message_id": f"cmid-{secrets.token_hex(6)}",
            "to": self.atlas["mailbox_id"],
            "body": body,
            "task_id": "task-1",
            "reply_to": "",
        })

    def _event_records(self):
        records = []
        for path in sorted(self.adapter.events_dir.glob("*.json")):
            records.append((path, json.loads(path.read_text(encoding="utf-8"))))
        return records

    # ------------------------------------------------------------------ tests

    def test_discovery_registration_and_first_delivery(self):
        self.adapter.run_once()
        self.assertEqual(self.cc.state.post_calls, [])

        sent = self._send("hello")
        self.adapter.run_once()

        self.assertEqual(len(self.cc.state.post_calls), 1)
        payload = self.cc.state.post_calls[0]
        self.assertEqual(payload["routing"], "chat")
        self.assertRegex(payload["event_id"], r"^[a-f0-9]{64}$")
        self.assertEqual(payload["prompt"].count("mailbox.py show --id"), 1)

        stored = self.atlas_client.get(sent["message_id"])
        self.assertEqual(stored["state"], "accepted")
        self.assertTrue(stored["receipts"])

        binding = json.loads((self.adapter.bindings_dir / f"{self.scope}.json").read_text())
        self.assertEqual(binding["generation"], self.atlas["generation"])
        self.assertEqual((self.adapter.bindings_dir / f"{self.scope}.json").stat().st_mode & 0o777, 0o600)

    def test_two_backlog_messages_are_both_delivered_once(self):
        first = self._send("one")
        second = self._send("two")
        self.adapter.run_once()
        self.adapter.run_once()

        event_ids = [p["event_id"] for p in self.cc.state.post_calls]
        self.assertEqual(len(event_ids), 2)
        self.assertEqual(len(set(event_ids)), 2)
        for sent in (first, second):
            self.assertEqual(self.atlas_client.get(sent["message_id"])["state"], "accepted")

    def test_claim_response_loss_is_replayed_with_same_request(self):
        sent = self._send("lost-response")
        real_request = cc_adapter.request_unix
        state = {"lost": False}

        def lossy(sock, method, path, token=None, payload=None, timeout=None):
            result = real_request(sock, method, path, token=token, payload=payload, timeout=timeout)
            if method == "POST" and path == "/v1/claim" and not state["lost"]:
                state["lost"] = True
                raise host_runtime.MailboxTransportError("simulated lost claim response")
            return result

        with patch("cc_adapter.request_unix", side_effect=lossy):
            self.adapter.run_once()
        self.assertEqual(self.cc.state.post_calls, [])

        claim_path = self.adapter._claim_path(self.scope, self.atlas["generation"])
        record = json.loads(claim_path.read_text())
        self.assertEqual(record["status"], "requested")
        request_id = record["request_id"]

        self.adapter.run_once()
        self.assertEqual(len(self.cc.state.post_calls), 1)
        record = json.loads(claim_path.read_text())
        self.assertEqual(record["status"], "delivered")
        self.assertEqual(record["request_id"], request_id)
        stored = self.atlas_client.get(sent["message_id"])
        self.assertEqual(stored["state"], "accepted")
        self.assertEqual(stored["claimed_by_generation"], self.atlas["generation"])

    def test_query_failure_does_not_retry(self):
        self.cc.state.deliver_mode = "waiting_retry"
        self._send("retry-later")
        self.adapter.run_once()
        self.assertEqual(len(self.cc.state.post_calls), 1)

        self.cc.state.get_fail = True
        self.adapter.run_once()
        self.assertEqual(len(self.cc.state.post_calls), 1)

    def test_explicit_unsent_receipt_retries_saved_input(self):
        self.cc.state.deliver_mode = "waiting_retry"
        self._send("retry-me")
        self.adapter.run_once()
        self.assertEqual(len(self.cc.state.post_calls), 1)
        saved = self.cc.state.post_calls[0]

        self.cc.state.deliver_mode = "delivered"
        self.adapter.run_once()
        self.assertEqual(len(self.cc.state.post_calls), 2)
        self.assertEqual(self.cc.state.post_calls[1], saved)

    def test_receipt_without_input_is_not_accepted_then_reconciled(self):
        self.cc.state.omit_input = True
        sent = self._send("no-input")
        self.adapter.run_once()
        (event_path, record) = self._event_records()[0]
        self.assertNotEqual(record["status"], "accepted")
        self.assertNotEqual(self.atlas_client.get(sent["message_id"])["state"], "accepted")

        self.cc.state.omit_input = False
        self.adapter.run_once()
        self.assertEqual(self.atlas_client.get(sent["message_id"])["state"], "accepted")
        record = json.loads(event_path.read_text())
        self.assertEqual(record["status"], "accepted")

    def test_new_session_advances_generation_and_reconciles_old_event(self):
        self.cc.state.deliver_mode = "waiting_retry"
        sent = self._send("old-event")
        self.adapter.run_once()
        gen1 = self.adapter.atlas_mailboxes[self.scope]["generation"]
        old_input = self.cc.state.post_calls[0]

        # /new: a new CC session id; the old event must keep its original address.
        self.cc.state.target["session_id"] = "cc-s2"
        self.cc.state.target["agent_session_id"] = "native-2"
        self.cc.state.deliver_mode = "delivered"
        self.assertIsNotNone(
            self.adapter.sync_chat(self.project, self.session_key, str(self.cwd)))
        gen2 = self.adapter.atlas_mailboxes[self.scope]["generation"]
        self.assertNotEqual(gen1, gen2)
        self.assertEqual(self.adapter.atlas_mailboxes[self.scope]["phase"], "active")

        self.adapter.run_once()
        self.assertEqual(len(self.cc.state.post_calls), 2)
        self.assertEqual(self.cc.state.post_calls[1], old_input)
        self.assertEqual(self.cc.state.post_calls[1]["target"]["session_id"], "cc-s1")
        self.assertEqual(self.atlas_client.get(sent["message_id"])["state"], "accepted")

        binding = json.loads((self.adapter.bindings_dir / f"{self.scope}.json").read_text())
        self.assertEqual(binding["generation"], gen2)


    def test_native_thread_change_advances_generation(self):
        # An in-flight event from the old native thread must still be reconciled
        # by its original payload after the generation switch.
        self.cc.state.deliver_mode = "waiting_retry"
        sent = self._send("in-flight-old-event")
        self.adapter.run_once()
        old_input = self.cc.state.post_calls[0]

        before = self.atlas_client.me()
        # Same CC session id, but the trusted chat-target reports a new native thread.
        self.cc.state.target["agent_session_id"] = "native-new-same-cc-session"
        self.cc.state.deliver_mode = "delivered"
        self.adapter.run_once()

        after = self.atlas_client.me()
        self.assertNotEqual(after["generation"], before["generation"])
        self.assertEqual(after["native_session_id"], "native-new-same-cc-session")
        self.assertEqual(len(self.cc.state.post_calls), 2)
        self.assertEqual(self.cc.state.post_calls[1], old_input)
        self.assertEqual(self.cc.state.post_calls[1]["target"]["agent_session_id"], "native-1")
        self.assertEqual(self.atlas_client.get(sent["message_id"])["state"], "accepted")

    def test_crash_after_cc_response_does_not_repost(self):
        for mode in ("unknown", "delivered"):
            with self.subTest(mode=mode):
                self.adapter.close()
                self.adapter = cc_adapter.CcAdapter(self.config_file)
                self.cc.state.post_calls.clear()
                self.cc.state.deliver_mode = mode
                self._send("crash after CC response, before claim completion")
                original_save = self.adapter._save_claim

                def crash_before_claim_completion(path, record, original_save=original_save):
                    if record.get("status") == "delivered":
                        raise OSError("test: process lost before local claim completion")
                    return original_save(path, record)

                with patch.object(self.adapter, "_save_claim", side_effect=crash_before_claim_completion):
                    self.adapter.run_once()
                before = len(self.cc.state.post_calls)
                self.assertEqual(before, 1)

                # A fresh adapter after the interruption must query, not re-POST.
                self.adapter.close()
                self.adapter = cc_adapter.CcAdapter(self.config_file)
                self.adapter.run_once()
                self.assertEqual(len(self.cc.state.post_calls), before)

    def test_lost_chat_state_recovers_without_deleting_data(self):
        sent = self._send("survives-state-loss")
        gen_before = self.adapter.atlas_mailboxes[self.scope]["generation"]

        # Drop only the local chat state; the registration and service data stay.
        (self.adapter.chats_dir / f"{self.scope}.json").unlink()
        self.adapter.close()
        self.adapter = cc_adapter.CcAdapter(self.config_file)
        self.scope = self.adapter.sync_chat(self.project, self.session_key, str(self.cwd))

        self.assertIsNotNone(self.scope)
        state = self.adapter.atlas_mailboxes[self.scope]
        self.assertEqual(state["phase"], "active")
        self.assertNotEqual(state["generation"], gen_before)
        self.assertEqual(self.atlas_client.get(sent["message_id"])["state"], "pending")

        self.adapter.run_once()
        self.assertEqual(self.atlas_client.get(sent["message_id"])["state"], "accepted")


if __name__ == "__main__":
    unittest.main()
