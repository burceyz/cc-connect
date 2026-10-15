#!/usr/bin/env python3
"""Independent live integration tests for agy_hook.py with real Go agent-mailbox service.

Tests all 10 required real service and hook behaviors from Gap D:
1. 服务claim成功后丢返回 -> 下一不同hook恢复 (Pre0 claim lost response recovered by Post0)
2. 两条积压 (Two pending messages delivered in sequence, cursor maintained)
3. 同hook重复/夹入新信不重注入 (Duplicate invocation suppressed even if new message arrives in inbox)
4. 不同generation invoc0 (New generation invocation 0 is not blocked by old generation or legacy marker)
5. 输出尝试后重建不重复 (Attempted message identity prevents duplicate injection after reconstruction)
6. receipt丢响应后恢复 (Pending receipt retried and committed on subsequent hook invocation)
7. 已ack pending不空claim (Messages with received_at > 0 are filtered; no empty claims created)
8. inbox失败不claim (Inbox transport error aborts cleanly without claim attempt)
9. 错误子会话拒绝 (Non-root subagent conversationId rejected with empty output)
10. Post仅在确实输出新信才force_continue (PostInvocation sets force_continue only when message claimed)
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

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "skill/scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import host_runtime

REAL_GO_MBX_BIN = Path(os.environ.get(
    "AGENT_MAILBOX_BIN", str(SCRIPTS_DIR.parents[1] / "build/agent-mailbox")
))


@unittest.skipUnless(
    REAL_GO_MBX_BIN.is_file(),
    "Requires real agent-mailbox Go binary",
)
class TestAgyHookLive(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="test-agy-hook-live-")
        self.root = Path(self.tmp.name).resolve()

        # Start real Go agent-mailbox service
        self.mbx_sock = self.root / "api.sock"
        self.admin_token_path = self.root / "admin-token"
        self.service_log = open(self.root / "service.log", "w")

        env = dict(os.environ)
        env["AGENT_MAILBOX_RUNTIME"] = str(self.root)

        mbx_cmd = [
            str(REAL_GO_MBX_BIN),
            "serve",
            "--runtime",
            str(self.root),
        ]
        self.mbx_proc = subprocess.Popen(
            mbx_cmd,
            env=env,
            stdout=self.service_log,
            stderr=self.service_log,
            start_new_session=True,
        )

        # Wait for service readiness
        deadline = time.monotonic() + 5.0
        ready = False
        while time.monotonic() < deadline:
            time.sleep(0.05)
            if self.mbx_proc.poll() is not None:
                break
            if self.mbx_sock.exists() and self.admin_token_path.exists():
                if host_runtime.check_mailbox_service(self.mbx_sock):
                    ready = True
                    break
        self.assertTrue(ready, "Go agent-mailbox service failed to start")

        self.admin_token = self.admin_token_path.read_text(encoding="utf-8").strip()

        # Register Atlas (coordinator) and Dorothy (worker)
        reg_dir = self.root / "registrations"
        reg_dir.mkdir(parents=True, mode=0o700)

        self.atlas_reg, self.atlas_tok = host_runtime.register_task_mailbox(
            self.mbx_sock,
            self.admin_token_path,
            "reg-atlas-live",
            "scope-hook-live",
            team="live-hook-team",
            label="Atlas",
            kind="atlas",
            coordinator=True,
            task_dir=reg_dir,
        )
        self.dorothy_reg, self.dorothy_tok = host_runtime.register_task_mailbox(
            self.mbx_sock,
            self.admin_token_path,
            "reg-dorothy-live",
            "scope-hook-live",
            team="live-hook-team",
            label="Dorothy",
            kind="dorothy",
            coordinator=False,
            parent_id=self.atlas_reg["mailbox_id"],
            task_dir=reg_dir,
        )

        # Activate Dorothy mailbox
        self.generation = "gen-live-001"
        host_runtime.activate_mailbox(
            self.mbx_sock,
            self.dorothy_tok,
            self.generation,
            capabilities=["live_hook"],
        )

        # Create Dorothy task directory
        self.dorothy_task_dir = self.root / "dorothy-task"
        self.dorothy_task_dir.mkdir(parents=True, mode=0o700)
        self.root_conversation_id = "conv-live-root-1"
        (self.dorothy_task_dir / "root-conversation-id.txt").write_text(
            self.root_conversation_id + "\n", encoding="utf-8"
        )
        self.binding_file = self.dorothy_task_dir / "mailbox-binding.json"
        self.binding_file.write_text(
            json.dumps({
                "schema": 1,
                "socket": str(self.mbx_sock),
                "token": self.dorothy_tok,
                "mailbox_id": self.dorothy_reg["mailbox_id"],
                "generation": self.generation,
            }),
            encoding="utf-8",
        )

        self.hook_script = SCRIPTS_DIR / "agy_hook.py"

    def tearDown(self):
        try:
            self.mbx_proc.terminate()
            self.mbx_proc.wait(timeout=2)
        except Exception:
            self.mbx_proc.kill()
        try:
            self.service_log.close()
        except Exception:
            pass
        self.tmp.cleanup()

    def run_hook(self, phase, stdin_payload=None, task_dir=None):
        if stdin_payload is None:
            stdin_payload = {
                "conversationId": self.root_conversation_id,
                "invocationNum": 0,
            }
        t_dir = task_dir or self.dorothy_task_dir
        cmd = [
            sys.executable,
            str(self.hook_script),
            "--phase",
            phase,
            "--task-dir",
            str(t_dir),
        ]
        res = subprocess.run(
            cmd,
            input=json.dumps(stdin_payload),
            capture_output=True,
            text=True,
            timeout=8,
        )
        self.assertEqual(res.returncode, 0, f"Hook failed: stderr={res.stderr}")
        try:
            return json.loads(res.stdout), res.stderr
        except json.JSONDecodeError:
            self.fail(f"Hook stdout is not JSON: {res.stdout}")

    def send_message_from_atlas(self, body="test message", task_id="task-live-1"):
        cid = f"msg-cid-{secrets.token_hex(4)}"
        res = host_runtime.request_unix(
            self.mbx_sock,
            "POST",
            "/v1/send",
            token=self.atlas_tok,
            payload={
                "client_message_id": cid,
                "to": self.dorothy_reg["mailbox_id"],
                "body": body,
                "task_id": task_id,
            },
        )
        return res["message"]

    def get_message(self, message_id):
        res = host_runtime.request_unix(
            self.mbx_sock,
            "GET",
            f"/v1/messages/{message_id}",
            token=self.atlas_tok,
        )
        return res["message"]

    # 1. 服务claim成功后丢返回 -> 下一不同hook恢复
    def test_01_claim_lost_response_recovered_by_next_hook(self):
        msg = self.send_message_from_atlas(body="critical instruction 1")
        msg_id = msg["message_id"]

        # Simulate Pre0 claim submitted to server and response lost
        pre_req_id = f"claim-agy-{self.generation}-pre-0"
        claim_file = self.dorothy_task_dir / f"claim-{self.generation}-pre-0.json"
        claim_file.write_text(json.dumps({
            "status": "claiming",
            "request_id": pre_req_id,
            "generation": self.generation,
            "phase": "pre",
            "invocation_num": 0,
            "timestamp": time.time(),
        }), encoding="utf-8")

        # Directly execute claim on Go service to commit state to 'dispatching'
        claim_res = host_runtime.request_unix(
            self.mbx_sock,
            "POST",
            "/v1/claim",
            token=self.dorothy_tok,
            payload={"generation": self.generation, "request_id": pre_req_id},
        )
        att_id = claim_res["attempt_id"]
        self.assertTrue(att_id)

        # Verify server state is dispatching
        srv_msg = self.get_message(msg_id)
        self.assertEqual(srv_msg["state"], "dispatching")
        self.assertEqual(srv_msg["attempt_id"], att_id)

        # Now execute NEXT hook: Post0 (different phase/invocation)
        output, _ = self.run_hook("post", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })

        # Next hook must recover Pre0's claimed message and inject it
        self.assertIn("injectSteps", output)
        self.assertIn(msg_id, output["injectSteps"][0]["userMessage"])
        self.assertEqual(output.get("terminationBehavior"), "force_continue")

        # Verify Pre0 claim marker was resolved to claimed
        c_data = json.loads(claim_file.read_text(encoding="utf-8"))
        self.assertEqual(c_data["status"], "claimed")
        self.assertEqual(c_data["attempt_id"], att_id)

        # Verify server state has advanced to offered
        srv_msg_after = self.get_message(msg_id)
        self.assertEqual(srv_msg_after["state"], "offered")
        receipt_states = [r["state"] for r in srv_msg_after.get("receipts", [])]
        self.assertIn("offered", receipt_states)

    # 2. 两条积压
    def test_02_two_pending_backlog(self):
        m1 = self.send_message_from_atlas(body="backlog item 1")
        m2 = self.send_message_from_atlas(body="backlog item 2")

        # Hook 1: Pre0 processes m1
        out1, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertIn("injectSteps", out1)
        self.assertIn(m1["message_id"], out1["injectSteps"][0]["userMessage"])

        s1 = self.get_message(m1["message_id"])
        self.assertEqual(s1["state"], "offered")
        s2 = self.get_message(m2["message_id"])
        self.assertEqual(s2["state"], "pending")

        # Hook 2: Post0 processes m2
        out2, _ = self.run_hook("post", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertIn("injectSteps", out2)
        self.assertIn(m2["message_id"], out2["injectSteps"][0]["userMessage"])
        self.assertEqual(out2.get("terminationBehavior"), "force_continue")

        s2_after = self.get_message(m2["message_id"])
        self.assertEqual(s2_after["state"], "offered")

    # 3. 同hook重复/夹入新信不重注入
    def test_03_duplicate_hook_suppressed_even_with_interleaved_new_message(self):
        m1 = self.send_message_from_atlas(body="first message")

        # Run Pre0: claims and injects m1
        out1, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertIn("injectSteps", out1)

        # Interleave new message m2 before Pre0 is called again
        m2 = self.send_message_from_atlas(body="interleaved message")

        # Run Pre0 again (same invocation)
        out2, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        # Duplicate hook must output {} and NOT inject m2
        self.assertEqual(out2, {})

        # Verify m2 is still pending on server, waiting for next invocation
        s2 = self.get_message(m2["message_id"])
        self.assertEqual(s2["state"], "pending")

    # 4. 不同generation invoc0
    def test_04_different_generation_invoc0_not_blocked(self):
        m1 = self.send_message_from_atlas(body="gen 1 message")
        out1, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertIn("injectSteps", out1)

        # Leave a legacy hook-injected-pre-0.json in task_dir as well
        (self.dorothy_task_dir / "hook-injected-pre-0.json").write_text(
            json.dumps({"message_id": "msg-legacy"}), encoding="utf-8"
        )

        # Switch to Generation 2
        gen2 = "gen-live-002"
        host_runtime.request_unix(
            self.mbx_sock,
            "POST",
            "/v1/deactivate",
            token=self.dorothy_tok,
            payload={"generation": self.generation},
        )
        host_runtime.activate_mailbox(
            self.mbx_sock,
            self.dorothy_tok,
            gen2,
            capabilities=["live_hook"],
        )
        self.binding_file.write_text(
            json.dumps({
                "schema": 1,
                "socket": str(self.mbx_sock),
                "token": self.dorothy_tok,
                "mailbox_id": self.dorothy_reg["mailbox_id"],
                "generation": gen2,
            }),
            encoding="utf-8",
        )

        m2 = self.send_message_from_atlas(body="gen 2 message")

        # Run Pre0 in Gen 2 (invocationNum resets to 0)
        out2, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        # Gen 2 Pre0 must succeed and claim m2, not blocked by Gen 1 or legacy marker
        self.assertIn("injectSteps", out2)
        self.assertIn(m2["message_id"], out2["injectSteps"][0]["userMessage"])

        s2 = self.get_message(m2["message_id"])
        self.assertEqual(s2["state"], "offered")

    # 5. 输出尝试后重建不重复
    def test_05_attempted_message_not_repeated(self):
        m1 = self.send_message_from_atlas(body="message for deduplication")
        msg_id = m1["message_id"]

        out1, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertIn("injectSteps", out1)

        # Simulate turn reconstruction where invocation marker is lost but attempt marker remains
        invoc_file = self.dorothy_task_dir / f"hook-injected-{self.generation}-pre-0.json"
        if invoc_file.exists():
            invoc_file.unlink()

        # Run hook again for same step: attempted marker must prevent re-output
        out2, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertEqual(out2, {})

    # 6. receipt丢响应后恢复
    def test_06_receipt_retry_on_subsequent_hook(self):
        m1 = self.send_message_from_atlas(body="receipt drop test")
        msg_id = m1["message_id"]

        # Claim message on service directly
        req_id = "claim-manual-rcpt-test"
        claim_res = host_runtime.request_unix(
            self.mbx_sock,
            "POST",
            "/v1/claim",
            token=self.dorothy_tok,
            payload={"generation": self.generation, "request_id": req_id},
        )
        att_id = claim_res["attempt_id"]

        # Server has message in dispatching
        srv_before = self.get_message(msg_id)
        self.assertEqual(srv_before["state"], "dispatching")

        # Record attempt marker and pending offered receipt in task_dir
        (self.dorothy_task_dir / f"attempted-{self.generation}-{msg_id}.json").write_text(
            json.dumps({
                "message_id": msg_id,
                "attempt_id": att_id,
                "generation": self.generation,
                "status": "attempted",
            }), encoding="utf-8"
        )
        rcpt_file = self.dorothy_task_dir / f"receipt-{self.generation}-{msg_id}-{att_id}.json"
        rcpt_file.write_text(json.dumps({
            "generation": self.generation,
            "message_id": msg_id,
            "attempt_id": att_id,
            "state": "offered",
            "evidence": "agy_hook_pre_injected",
            "status": "pending",
            "timestamp": time.time(),
        }), encoding="utf-8")

        # Next hook runs (empty inbox)
        out, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 1,
        })
        self.assertEqual(out, {})

        # Receipt must be retried and marked sent
        rcpt_data = json.loads(rcpt_file.read_text(encoding="utf-8"))
        self.assertEqual(rcpt_data["status"], "sent")

        # Server state must now be offered
        srv_after = self.get_message(msg_id)
        self.assertEqual(srv_after["state"], "offered")

    # 7. 已ack pending不空claim
    def test_07_acked_pending_message_does_not_trigger_claim(self):
        m1 = self.send_message_from_atlas(body="already acked message")
        msg_id = m1["message_id"]

        # Dorothy acks the message directly before hook runs
        host_runtime.request_unix(
            self.mbx_sock,
            "POST",
            "/v1/ack",
            token=self.dorothy_tok,
            payload={"message_id": msg_id},
        )
        srv_msg = self.get_message(msg_id)
        self.assertEqual(srv_msg["state"], "pending")
        self.assertGreater(srv_msg.get("received_at", 0), 0)

        # Hook runs
        out, _ = self.run_hook("pre", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        # Must output {}
        self.assertEqual(out, {})

        # Server state must still be pending with NO attempt_id (no claim created)
        srv_after = self.get_message(msg_id)
        self.assertEqual(srv_after["state"], "pending")
        self.assertFalse(srv_after.get("attempt_id"))

    # 8. inbox失败不claim
    def test_08_inbox_failure_aborts_cleanly_without_claim(self):
        # Point binding to invalid socket
        bad_task_dir = self.root / "bad-task"
        bad_task_dir.mkdir(parents=True, mode=0o700)
        (bad_task_dir / "root-conversation-id.txt").write_text(
            self.root_conversation_id + "\n", encoding="utf-8"
        )
        (bad_task_dir / "mailbox-binding.json").write_text(
            json.dumps({
                "schema": 1,
                "socket": str(self.root / "nonexistent.sock"),
                "token": self.dorothy_tok,
                "mailbox_id": self.dorothy_reg["mailbox_id"],
                "generation": self.generation,
            }),
            encoding="utf-8",
        )

        out, stderr = self.run_hook("pre", task_dir=bad_task_dir)
        self.assertEqual(out, {})
        self.assertIn("inbox fetch failed", stderr)

    # 9. 错误子会话拒绝
    def test_09_subagent_conversation_rejected(self):
        out, stderr = self.run_hook("pre", {
            "conversationId": "conv-subagent-child-unauthorized",
            "invocationNum": 0,
        })
        self.assertEqual(out, {})
        self.assertIn("subagent claim rejected", stderr)

    # 10. Post仅在确实输出新信才force_continue
    def test_10_post_only_sets_force_continue_when_message_claimed(self):
        # Case A: message claimed in post -> terminationBehavior is force_continue
        m1 = self.send_message_from_atlas(body="post phase message")
        out1, _ = self.run_hook("post", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 0,
        })
        self.assertIn("injectSteps", out1)
        self.assertEqual(out1.get("terminationBehavior"), "force_continue")

        # Case B: no message claimed in post -> outputs {} without terminationBehavior
        out2, _ = self.run_hook("post", {
            "conversationId": self.root_conversation_id,
            "invocationNum": 1,
        })
        self.assertEqual(out2, {})
        self.assertNotIn("terminationBehavior", out2)


if __name__ == "__main__":
    unittest.main()
