"""Tests for agy_hook.py official invocation hook adapter."""

import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "skill/scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import agy_hook


class TestAgyHook(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="test-agy-hook-")
        self.root = Path(self.tmp.name).resolve()
        self.task_dir = self.root / "task"
        self.task_dir.mkdir(parents=True, mode=0o700)

        self.hook_script = SCRIPTS_DIR / "agy_hook.py"
        self.root_id_file = self.task_dir / "root-conversation-id.txt"
        self.root_id_file.write_text("conv-root-12345\n", encoding="utf-8")

        self.binding_file = self.task_dir / "mailbox-binding.json"
        self.binding_file.write_text(
            json.dumps({
                "schema": 1,
                "socket": str(self.root / "mailbox.sock"),
                "token": "tok-worker-secret",
                "mailbox_id": "mbx-worker-1",
                "generation": "gen-worker-1",
            }),
            encoding="utf-8",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def run_hook_cli(self, phase, stdin_data):
        cmd = [
            sys.executable,
            str(self.hook_script),
            "--phase",
            phase,
            "--task-dir",
            str(self.task_dir),
        ]
        res = subprocess.run(
            cmd,
            input=json.dumps(stdin_data),
            capture_output=True,
            text=True,
            timeout=5,
        )
        self.assertEqual(res.returncode, 0, f"Hook failed: {res.stderr}")
        try:
            return json.loads(res.stdout), res.stderr
        except json.JSONDecodeError:
            self.fail(f"Hook stdout is not JSON: {res.stdout}")

    def run_hook_in_process(self, phase, stdin_data):
        stdin_str = json.dumps(stdin_data)
        stdout_io = io.StringIO()
        stderr_io = io.StringIO()

        test_argv = [
            "agy_hook.py",
            "--phase",
            phase,
            "--task-dir",
            str(self.task_dir),
        ]

        with patch("sys.stdin", io.StringIO(stdin_str)), \
             patch("sys.stdout", stdout_io), \
             patch("sys.stderr", stderr_io), \
             patch("sys.argv", test_argv):
            code = agy_hook.main()
            self.assertEqual(code, 0)

        raw_out = stdout_io.getvalue()
        try:
            return json.loads(raw_out), stderr_io.getvalue()
        except json.JSONDecodeError:
            self.fail(f"Output is not JSON: {raw_out}")

    def test_subagent_rejection(self):
        # Conversation ID does not match root-conversation-id.txt -> subagent
        payload = {
            "conversationId": "subagent-child-conv-999",
            "invocationNum": 1,
        }
        output, stderr = self.run_hook_cli("pre", payload)
        self.assertEqual(output, {})
        self.assertIn("subagent claim rejected", stderr)

    def test_inbox_empty_skips_claim(self):
        # Root conversation ID matches, but inbox has no pending messages
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {"messages": [], "cursor": "100"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            output, stderr = self.run_hook_in_process("pre", payload)
            self.assertEqual(output, {})

    def test_claim_and_inject_post_invocation(self):
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 2,
        }

        claimed_msg = {
            "message_id": "msg-collab-777",
            "from": "mbx-atlas",
            "task_id": "task-alpha",
            "body": "collaborative instructions",
        }

        unix_calls = []

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            unix_calls.append((method, path, payload))
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-collab-777", "state": "pending", "received_at": 0}],
                    "cursor": "200",
                }
            if path == "/v1/claim":
                return {
                    "message": claimed_msg,
                    "attempt_id": "att-hook-1",
                }
            if path == "/v1/receipt":
                return {"receipt_id": "rcpt-1", "state": "offered"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            output, stderr = self.run_hook_in_process("post", payload)

            # In post phase, force_continue must be set when message claimed
            self.assertEqual(output.get("terminationBehavior"), "force_continue")
            self.assertIn("injectSteps", output)
            steps = output["injectSteps"]
            self.assertEqual(len(steps), 1)
            msg_text = steps[0].get("userMessage", "")
            self.assertIn("msg-collab-777", msg_text)
            self.assertIn("mailbox.py show --id", msg_text)
            self.assertIn("read_message", msg_text)
            self.assertIn("ack_message", msg_text)

            # Check that receipt was sent with state offered
            receipt_calls = [c for c in unix_calls if c[1] == "/v1/receipt"]
            self.assertEqual(len(receipt_calls), 1)
            self.assertEqual(receipt_calls[0][2]["state"], "offered")
            self.assertEqual(receipt_calls[0][2]["message_id"], "msg-collab-777")

    def test_duplicate_invocation_suppressed(self):
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        claimed_msg = {
            "message_id": "msg-101",
            "from": "mbx-atlas",
            "task_id": "task-1",
            "body": "test",
        }

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-101", "state": "pending", "received_at": 0}],
                    "cursor": "10",
                }
            if path == "/v1/claim":
                return {"message": claimed_msg, "attempt_id": "att-101"}
            if path == "/v1/receipt":
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out1, _ = self.run_hook_in_process("pre", payload)
            self.assertIn("injectSteps", out1)

            # Call again with the same invocationNum and phase
            out2, _ = self.run_hook_in_process("pre", payload)
            self.assertEqual(out2, {})

    def test_generation_isolation(self):
        # Gen 1 invocation 0 sets a marker
        gen1_marker = self.task_dir / "hook-injected-gen-worker-1-pre-0.json"
        gen1_marker.write_text(json.dumps({"message_id": "msg-gen1"}), encoding="utf-8")

        # Now advance binding to Gen 2
        self.binding_file.write_text(
            json.dumps({
                "schema": 1,
                "socket": str(self.root / "mailbox.sock"),
                "token": "tok-worker-secret",
                "mailbox_id": "mbx-worker-1",
                "generation": "gen-worker-2",
            }),
            encoding="utf-8",
        )

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 0,  # Turn resets invocationNum to 0
        }

        claimed_msg = {
            "message_id": "msg-gen2-001",
            "from": "mbx-atlas",
            "task_id": "task-gen2",
            "body": "followup msg",
        }

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-gen2-001", "state": "pending", "received_at": 0}],
                    "cursor": "30",
                }
            if path == "/v1/claim":
                return {"message": claimed_msg, "attempt_id": "att-gen2-1"}
            if path == "/v1/receipt":
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            # Gen 2 invocation 0 must succeed and not be suppressed by Gen 1 marker!
            self.assertIn("injectSteps", out)
            self.assertIn("msg-gen2-001", out["injectSteps"][0]["userMessage"])

    def test_lost_response_recovery(self):
        # Simulate a previous attempt that wrote "claiming" state but crashed before receiving response
        claim_marker = self.task_dir / "claim-gen-worker-1-pre-3.json"
        expected_request_id = "claim-agy-gen-worker-1-pre-3"
        claim_marker.write_text(json.dumps({
            "status": "claiming",
            "request_id": expected_request_id,
            "generation": "gen-worker-1",
            "phase": "pre",
            "invocation_num": 3,
            "timestamp": 12345.0,
        }), encoding="utf-8")

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 3,
        }

        claimed_msg = {
            "message_id": "msg-recovered-99",
            "from": "mbx-hermes",
            "task_id": "task-rec",
            "body": "recovered body",
        }

        replayed_request_id = None

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            nonlocal replayed_request_id
            if path == "/v1/claim":
                replayed_request_id = payload.get("request_id")
                return {"message": claimed_msg, "attempt_id": "att-rec-99"}
            if path == "/v1/receipt":
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            # Must replay claim with identical request_id to recover
            self.assertEqual(replayed_request_id, expected_request_id)
            self.assertIn("injectSteps", out)
            self.assertIn("msg-recovered-99", out["injectSteps"][0]["userMessage"])

            # Verify claim marker was updated to "claimed"
            claim_data = json.loads(claim_marker.read_text(encoding="utf-8"))
            self.assertEqual(claim_data["status"], "claimed")
            self.assertEqual(claim_data["attempt_id"], "att-rec-99")

    def test_inbox_failure_aborts_cleanly(self):
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        claim_called = False

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            nonlocal claim_called
            if path.startswith("/v1/inbox"):
                raise ConnectionRefusedError("socket unavailable")
            if path == "/v1/claim":
                claim_called = True
                return {}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, stderr = self.run_hook_in_process("pre", payload)
            self.assertEqual(out, {})
            self.assertIn("inbox fetch failed", stderr)
            self.assertFalse(claim_called, "Must not attempt claim when inbox query fails")

    def test_inbox_filters_received_at(self):
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        claim_called = False

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            nonlocal claim_called
            if path.startswith("/v1/inbox"):
                # All messages have received_at > 0
                return {
                    "messages": [
                        {"message_id": "msg-old-1", "state": "pending", "received_at": 1728900000},
                        {"message_id": "msg-old-2", "state": "pending", "received_at": 1728900010},
                    ],
                    "cursor": "50",
                }
            if path == "/v1/claim":
                claim_called = True
                return {}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            self.assertEqual(out, {})
            self.assertFalse(claim_called, "Must not claim when all pending messages have received_at > 0")

    def test_receipt_failure_does_not_abort(self):
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        claimed_msg = {
            "message_id": "msg-receipt-fail",
            "from": "mbx-atlas",
            "task_id": "task-rf",
            "body": "receipt fail test",
        }

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-receipt-fail", "state": "pending", "received_at": 0}],
                    "cursor": "60",
                }
            if path == "/v1/claim":
                return {"message": claimed_msg, "attempt_id": "att-rf-1"}
            if path == "/v1/receipt":
                raise RuntimeError("receipt endpoint 500 error")
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, stderr = self.run_hook_in_process("pre", payload)
            # Output must still succeed and contain message
            self.assertIn("injectSteps", out)
            self.assertIn("msg-receipt-fail", out["injectSteps"][0]["userMessage"])
            self.assertIn("receipt recording failed", stderr)

            # Markers must still be persisted
            attempt_file = self.task_dir / "attempted-gen-worker-1-msg-receipt-fail.txt"
            self.assertTrue(attempt_file.is_file())

    def test_lost_response_recovered_by_different_hook_invocation(self):
        # Gap A: Pre0 claim lost response; next hook is Post0, which recovers it
        claim_marker = self.task_dir / "claim-gen-worker-1-pre-0.json"
        expected_request_id = "claim-agy-gen-worker-1-pre-0"
        claim_marker.write_text(json.dumps({
            "status": "claiming",
            "request_id": expected_request_id,
            "generation": "gen-worker-1",
            "phase": "pre",
            "invocation_num": 0,
            "timestamp": 12345.0,
        }), encoding="utf-8")

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 0,
        }

        claimed_msg = {
            "message_id": "msg-recovered-cross-hook",
            "from": "mbx-atlas",
            "task_id": "task-cross",
            "body": "recovered by post hook",
        }

        replayed_request_id = None
        replayed_generation = None
        receipt_calls = []

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            nonlocal replayed_request_id, replayed_generation
            if path == "/v1/claim":
                replayed_request_id = payload.get("request_id")
                replayed_generation = payload.get("generation")
                return {"message": claimed_msg, "attempt_id": "att-cross-0"}
            if path == "/v1/receipt":
                receipt_calls.append(payload)
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            # Run Post0 hook
            out, _ = self.run_hook_in_process("post", payload)
            # Must replay with Pre0's original generation and request_id
            self.assertEqual(replayed_request_id, expected_request_id)
            self.assertEqual(replayed_generation, "gen-worker-1")
            # Post0 must adopt and inject the message
            self.assertIn("injectSteps", out)
            self.assertIn("msg-recovered-cross-hook", out["injectSteps"][0]["userMessage"])
            self.assertEqual(out.get("terminationBehavior"), "force_continue")

            # Pre0's claim file must be updated to claimed
            claim_data = json.loads(claim_marker.read_text(encoding="utf-8"))
            self.assertEqual(claim_data["status"], "claimed")
            self.assertEqual(claim_data["attempt_id"], "att-cross-0")

            # Receipt must be sent with state offered
            self.assertEqual(len(receipt_calls), 1)
            self.assertEqual(receipt_calls[0]["state"], "offered")
            self.assertEqual(receipt_calls[0]["message_id"], "msg-recovered-cross-hook")

    def test_old_generation_claim_not_injected_in_new_generation(self):
        # Gap A: Gen1 left an unresolved claim; Gen2 must resolve it but NOT adopt/inject it
        claim_marker = self.task_dir / "claim-gen-worker-1-pre-0.json"
        claim_marker.write_text(json.dumps({
            "status": "claiming",
            "request_id": "claim-agy-gen-worker-1-pre-0",
            "generation": "gen-worker-1",
            "phase": "pre",
            "invocation_num": 0,
            "timestamp": 12345.0,
        }), encoding="utf-8")

        # Update binding to Gen2
        self.binding_file.write_text(
            json.dumps({
                "schema": 1,
                "socket": str(self.root / "mailbox.sock"),
                "token": "tok-worker-secret",
                "mailbox_id": "mbx-worker-1",
                "generation": "gen-worker-2",
            }),
            encoding="utf-8",
        )

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 0,
        }

        old_msg = {
            "message_id": "msg-old-gen1",
            "from": "mbx-atlas",
            "task_id": "task-old",
            "body": "old generation message",
        }

        replayed = []

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path == "/v1/claim":
                replayed.append(payload)
                if payload.get("generation") == "gen-worker-1":
                    return {"message": old_msg, "attempt_id": "att-old-1"}
                return {"message": None, "attempt_id": None}
            if path.startswith("/v1/inbox"):
                return {"messages": [], "cursor": "10"}
            if path == "/v1/receipt":
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            # Gen2 must NOT inject the old generation's message!
            self.assertEqual(out, {})

            # Old claim file was resolved
            claim_data = json.loads(claim_marker.read_text(encoding="utf-8"))
            self.assertEqual(claim_data["status"], "claimed")
            self.assertEqual(claim_data["attempt_id"], "att-old-1")

            # Old message must be marked attempted so it cannot be delivered in any generation
            self.assertTrue(agy_hook._is_message_attempted(self.task_dir, "msg-old-gen1"))

    def test_legacy_invocation_marker_does_not_block_new_generation(self):
        # Gap B: legacy hook-injected-pre-0.json must not block a new generation at invoc 0
        legacy_marker = self.task_dir / "hook-injected-pre-0.json"
        legacy_marker.write_text(json.dumps({"message_id": "msg-legacy"}), encoding="utf-8")

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 0,
        }

        claimed_msg = {
            "message_id": "msg-fresh-0",
            "from": "mbx-atlas",
            "task_id": "task-fresh",
            "body": "fresh message",
        }

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-fresh-0", "state": "pending", "received_at": 0}],
                    "cursor": "20",
                }
            if path == "/v1/claim":
                return {"message": claimed_msg, "attempt_id": "att-fresh-0"}
            if path == "/v1/receipt":
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            # Must NOT be blocked by legacy marker!
            self.assertIn("injectSteps", out)
            self.assertIn("msg-fresh-0", out["injectSteps"][0]["userMessage"])

    def test_attempted_message_never_re_injected_across_generations(self):
        # Gap B: attempt in any generation prevents re-injecting even if generation changes
        prev_att = self.task_dir / "attempted-gen-worker-old-msg-dup.json"
        prev_att.write_text(json.dumps({
            "message_id": "msg-dup",
            "attempt_id": "att-old-dup",
            "generation": "gen-worker-old",
            "status": "attempted",
        }), encoding="utf-8")

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        dup_msg = {
            "message_id": "msg-dup",
            "from": "mbx-atlas",
            "task_id": "task-dup",
            "body": "duplicate body",
        }

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-dup", "state": "pending", "received_at": 0}],
                    "cursor": "25",
                }
            if path == "/v1/claim":
                return {"message": dup_msg, "attempt_id": "att-new-dup"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            # Already attempted message must be suppressed!
            self.assertEqual(out, {})

    def test_receipt_retry_on_subsequent_hook(self):
        # Gap C: pending receipt from previous failed hook is retried on next hook
        pending_receipt = self.task_dir / "receipt-gen-worker-1-msg-ret-att-ret.json"
        pending_receipt.write_text(json.dumps({
            "generation": "gen-worker-1",
            "message_id": "msg-ret",
            "attempt_id": "att-ret",
            "state": "offered",
            "evidence": "agy_hook_pre_injected",
            "status": "pending",
            "timestamp": 12345.0,
        }), encoding="utf-8")

        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 2,
        }

        receipt_calls = []

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {"messages": [], "cursor": "30"}
            if path == "/v1/receipt":
                receipt_calls.append(payload)
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            # Next hook retried the pending receipt
            self.assertEqual(len(receipt_calls), 1)
            self.assertEqual(receipt_calls[0]["message_id"], "msg-ret")
            self.assertEqual(receipt_calls[0]["state"], "offered")

            # Receipt file updated to sent
            r_data = json.loads(pending_receipt.read_text(encoding="utf-8"))
            self.assertEqual(r_data["status"], "sent")

    def test_output_flushed_before_offered_receipt(self):
        # Gap C: verify attempt file exists, receipt file state is offered, and exactly one JSON is output
        payload = {
            "conversationId": "conv-root-12345",
            "invocationNum": 1,
        }

        claimed_msg = {
            "message_id": "msg-order-test",
            "from": "mbx-atlas",
            "task_id": "task-order",
            "body": "order test message",
        }

        receipt_payloads = []

        def mock_request(sock, method, path, payload=None, token=None, timeout=None):
            if path.startswith("/v1/inbox"):
                return {
                    "messages": [{"message_id": "msg-order-test", "state": "pending", "received_at": 0}],
                    "cursor": "40",
                }
            if path == "/v1/claim":
                return {"message": claimed_msg, "attempt_id": "att-order-1"}
            if path == "/v1/receipt":
                receipt_payloads.append(dict(payload))
                return {"status": "ok"}
            return {}

        with patch("agy_hook.request_unix", side_effect=mock_request):
            out, _ = self.run_hook_in_process("pre", payload)
            self.assertIn("injectSteps", out)

            # Check attempt marker persisted
            att_file = self.task_dir / "attempted-gen-worker-1-msg-order-test.json"
            self.assertTrue(att_file.is_file())
            att_data = json.loads(att_file.read_text(encoding="utf-8"))
            self.assertEqual(att_data["status"], "attempted")

            # Check receipt marker persisted with offered and sent
            r_file = self.task_dir / "receipt-gen-worker-1-msg-order-test-att-order-1.json"
            self.assertTrue(r_file.is_file())
            r_data = json.loads(r_file.read_text(encoding="utf-8"))
            self.assertEqual(r_data["state"], "offered")
            self.assertEqual(r_data["status"], "sent")

            self.assertEqual(len(receipt_payloads), 1)
            self.assertEqual(receipt_payloads[0]["state"], "offered")


if __name__ == "__main__":
    unittest.main()
