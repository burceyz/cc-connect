#!/usr/bin/env python3
"""Antigravity 1.3.3 official invocation hook adapter for agent-mailbox.

Handles PreInvocation and PostInvocation hook events from agy CLI:
- Reads official hook JSON from stdin.
- Validates root conversation against the task's root-conversation-id.txt.
- Rejects subagents / secondary conversations from claiming root mailbox messages.
- Checks inbox cursor and received_at before claiming to avoid empty claims.
- Binds all invocation and attempt markers to the mailbox activation generation.
- Scans and recovers unresolved claims across invocations and generations.
- Recovers lost claim responses by replaying identical generation and request_id.
- Persists output attempt identity before stdout output; never re-outputs attempted message.
- Persists pending receipts before submission; retries failed receipts on next hook.
- Updates receipt intent to 'offered' only after stdout is written and flushed.
- In PostInvocation, sets terminationBehavior='force_continue' only when a message was claimed.
- If no message or any failure: outputs {} and exits cleanly without hanging the runner.
- All operations are bounded by an 8.0s deadline and 2.5s per-request timeout.
- Stderr is used for all diagnostics; stdout outputs ONLY official hook JSON.
"""

import argparse
import json
import os
from pathlib import Path
import secrets
import shlex
import sys
import time

try:
    from host_runtime import request_unix, MailboxError
except ImportError:
    from skill.scripts.host_runtime import request_unix, MailboxError


def _safe_write_text(path, content):
    """Write private regular file atomically with mode 0600."""
    path = Path(path)
    tmp = path.parent / f".tmp-{path.name}-{secrets.token_hex(4)}"
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as h:
            h.write(content)
            h.flush()
            os.fsync(h.fileno())
        os.replace(tmp, path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _is_message_attempted(task_dir: Path, message_id: str) -> bool:
    """Check if message_id was attempted in any generation or legacy marker."""
    if not message_id:
        return False
    if (task_dir / f"attempted-{message_id}.txt").is_file():
        return True
    if (task_dir / f"attempted-{message_id}.json").is_file():
        return True
    if (task_dir / f"output-attempt-{message_id}.json").is_file():
        return True
    for p in task_dir.glob(f"attempted-*-{message_id}.*"):
        if p.is_file():
            return True
    for p in task_dir.glob(f"output-attempt-*-{message_id}.*"):
        if p.is_file():
            return True
    return False


def _recover_pending_receipts(task_dir: Path, service_socket: str, token: str, time_remaining_fn, remaining_timeout_fn):
    """Retry any pending receipts that failed in previous hooks."""
    for r_file in sorted(task_dir.glob("receipt-*.json")):
        if time_remaining_fn() <= 0.2:
            break
        try:
            data = json.loads(r_file.read_text(encoding="utf-8"))
        except Exception:
            continue
        if data.get("status") != "pending":
            continue

        r_gen = data.get("generation")
        r_msg_id = data.get("message_id")
        r_att_id = data.get("attempt_id")
        r_state = data.get("state")
        r_evidence = data.get("evidence", "")
        if not (r_gen and r_msg_id and r_att_id and r_state):
            continue

        try:
            request_unix(
                service_socket,
                "POST",
                "/v1/receipt",
                token=token,
                payload={
                    "generation": r_gen,
                    "message_id": r_msg_id,
                    "attempt_id": r_att_id,
                    "state": r_state,
                    "evidence": r_evidence,
                },
                timeout=remaining_timeout_fn(2.5),
            )
            data["status"] = "sent"
            data["sent_at"] = time.time()
            _safe_write_text(r_file, json.dumps(data))
        except Exception as exc:
            # Preserve pending status and existing state; never overwrite stronger evidence
            sys.stderr.write(f"[agy_hook] retrying receipt for {r_msg_id} failed: {exc}\n")


def _recover_unresolved_claims(task_dir: Path, service_socket: str, token: str, current_generation: str, time_remaining_fn, remaining_timeout_fn):
    """Scan and resolve in-flight claims across previous invocations/generations.

    Returns (recovered_message, recovered_attempt_id) if an unattempted claimed message
    belonging to current_generation is found, else (None, None).
    """
    recovered_for_current = None
    recovered_attempt_id = None

    claim_files = sorted(task_dir.glob("claim-*.json"), key=lambda p: p.stat().st_mtime if p.exists() else 0)

    for c_file in claim_files:
        if time_remaining_fn() <= 0.2:
            break
        try:
            data = json.loads(c_file.read_text(encoding="utf-8"))
        except Exception:
            continue

        c_status = data.get("status")
        c_gen = data.get("generation")
        c_req_id = data.get("request_id")

        if c_status == "claiming" and c_gen and c_req_id:
            # Replay claim with original generation and original request_id
            try:
                claim_res = request_unix(
                    service_socket,
                    "POST",
                    "/v1/claim",
                    token=token,
                    payload={"generation": c_gen, "request_id": c_req_id},
                    timeout=remaining_timeout_fn(2.5),
                )
                msg = claim_res.get("message")
                att = claim_res.get("attempt_id")
                if msg and att:
                    data["status"] = "claimed"
                    data["message"] = msg
                    data["attempt_id"] = att
                    data["resolved_at"] = time.time()
                    _safe_write_text(c_file, json.dumps(data))
                else:
                    data["status"] = "empty"
                    data["resolved_at"] = time.time()
                    _safe_write_text(c_file, json.dumps(data))
            except Exception as exc:
                sys.stderr.write(f"[agy_hook] replaying claim {c_req_id} failed: {exc}\n")
                # Remain "claiming" for next attempt

        # Re-check status if claimed
        c_status = data.get("status")
        if c_status == "claimed":
            msg = data.get("message")
            att = data.get("attempt_id")
            if msg and att:
                msg_id = msg.get("message_id")
                if c_gen == current_generation and not _is_message_attempted(task_dir, msg_id):
                    if recovered_for_current is None:
                        recovered_for_current = msg
                        recovered_attempt_id = att
                elif c_gen != current_generation:
                    # Historical claim from a previous generation:
                    # Do not treat as new claim for current generation.
                    # Ensure attempted marker exists so it is never re-injected.
                    if msg_id and not _is_message_attempted(task_dir, msg_id):
                        att_marker = task_dir / f"attempted-{c_gen}-{msg_id}.json"
                        _safe_write_text(att_marker, json.dumps({
                            "message_id": msg_id,
                            "attempt_id": att,
                            "generation": c_gen,
                            "status": "historical",
                            "timestamp": time.time(),
                        }))
                    # If no receipt exists, record unknown receipt so server knows it was not delivered
                    r_file = task_dir / f"receipt-{c_gen}-{msg_id}-{att}.json"
                    if not r_file.exists():
                        _safe_write_text(r_file, json.dumps({
                            "generation": c_gen,
                            "message_id": msg_id,
                            "attempt_id": att,
                            "state": "unknown",
                            "evidence": "historical_unattempted",
                            "status": "pending",
                            "timestamp": time.time(),
                        }))

    return recovered_for_current, recovered_attempt_id


def main():
    start_time = time.monotonic()
    deadline = start_time + 8.0

    def time_remaining():
        return deadline - time.monotonic()

    def remaining_timeout(cap=2.5):
        rem = time_remaining()
        return max(0.1, min(cap, rem))

    parser = argparse.ArgumentParser(description="Antigravity mailbox invocation hook")
    parser.add_argument("--phase", choices=["pre", "post"], required=True, help="Hook phase: pre or post invocation")
    parser.add_argument("--task-dir", required=True, help="Task execution directory containing binding and root identity")
    args = parser.parse_args()

    task_dir = Path(args.task_dir).resolve()
    phase = args.phase

    # Parse hook input from stdin
    try:
        raw_input = sys.stdin.read()
        payload = json.loads(raw_input) if raw_input.strip() else {}
    except Exception as exc:
        sys.stderr.write(f"[agy_hook] invalid stdin json: {exc}\n")
        print(json.dumps({}))
        return 0

    conversation_id = payload.get("conversationId")
    invocation_num = payload.get("invocationNum", 0)

    # 1. Verify root conversation identity
    root_id_file = task_dir / "root-conversation-id.txt"
    root_conversation_id = None
    if root_id_file.is_file():
        root_conversation_id = root_id_file.read_text(encoding="utf-8").strip()
    else:
        # Give host a brief bounded window (up to 2s) to observe 'init' and persist root conversation ID
        wait_deadline = min(deadline, time.monotonic() + 2.0)
        while time.monotonic() < wait_deadline:
            time.sleep(0.05)
            if root_id_file.is_file():
                root_conversation_id = root_id_file.read_text(encoding="utf-8").strip()
                break

    if not root_conversation_id:
        sys.stderr.write("[agy_hook] root conversation identity not visible; skipping claim\n")
        print(json.dumps({}))
        return 0

    if conversation_id != root_conversation_id:
        sys.stderr.write(
            f"[agy_hook] conversationId mismatch ({conversation_id} != {root_conversation_id}); subagent claim rejected\n"
        )
        print(json.dumps({}))
        return 0

    # 2. Read mailbox binding
    binding_file = task_dir / "mailbox-binding.json"
    if not binding_file.is_file():
        print(json.dumps({}))
        return 0

    try:
        binding = json.loads(binding_file.read_text(encoding="utf-8"))
        service_socket = binding["socket"]
        token = binding["token"]
        generation = binding["generation"]
    except Exception as exc:
        sys.stderr.write(f"[agy_hook] unreadable binding file: {exc}\n")
        print(json.dumps({}))
        return 0

    # Check duplicate hook invocation for this generation and step
    # Note: Do NOT check legacy un-versioned marker to avoid blocking new generation invocation 0
    invoc_marker = task_dir / f"hook-injected-{generation}-{phase}-{invocation_num}.json"
    if invoc_marker.is_file():
        # Duplicate hook invocation: recover any pending receipts opportunistically then exit cleanly
        _recover_pending_receipts(task_dir, service_socket, token, time_remaining, remaining_timeout)
        print(json.dumps({}))
        return 0

    # 3. Recover any pending receipts from previous hooks
    _recover_pending_receipts(task_dir, service_socket, token, time_remaining, remaining_timeout)

    # 4. Check for in-flight or recovered claim across invocations and generations
    recovered_msg, recovered_att = _recover_unresolved_claims(
        task_dir, service_socket, token, generation, time_remaining, remaining_timeout
    )

    message = None
    attempt_id = None
    recovering = False

    claim_marker = task_dir / f"claim-{generation}-{phase}-{invocation_num}.json"
    request_id = f"claim-agy-{generation}-{phase}-{invocation_num}"

    if recovered_msg and recovered_att:
        message = recovered_msg
        attempt_id = recovered_att
        recovering = True
    elif claim_marker.is_file():
        try:
            prev_claim = json.loads(claim_marker.read_text(encoding="utf-8"))
        except Exception:
            prev_claim = {}
        status = prev_claim.get("status")
        if status == "claimed" and prev_claim.get("message") and prev_claim.get("attempt_id"):
            msg_candidate = prev_claim["message"]
            if not _is_message_attempted(task_dir, msg_candidate.get("message_id")):
                message = msg_candidate
                attempt_id = prev_claim["attempt_id"]
                recovering = True
        elif status == "empty":
            print(json.dumps({}))
            return 0

    cursor_file = task_dir / "inbox-cursor.txt"
    inbox_cursor = None
    pending_count = 0

    if not recovering:
        # Check budget
        if time_remaining() <= 0.2:
            sys.stderr.write("[agy_hook] budget exhausted before inbox fetch; skipping\n")
            print(json.dumps({}))
            return 0

        # 5. Check inbox cursor before claiming to avoid empty claims
        cursor = cursor_file.read_text(encoding="utf-8").strip() if cursor_file.is_file() else None

        inbox_path = "/v1/inbox"
        if cursor:
            inbox_path += f"?after={cursor}"

        try:
            inbox_res = request_unix(
                service_socket,
                "GET",
                inbox_path,
                token=token,
                timeout=remaining_timeout(2.5),
            )
        except Exception as exc:
            sys.stderr.write(f"[agy_hook] inbox fetch failed: {exc}\n")
            print(json.dumps({}))
            return 0

        inbox_cursor = inbox_res.get("cursor")
        raw_msgs = inbox_res.get("messages", [])
        pending = [
            m for m in raw_msgs
            if m.get("state") == "pending" and (m.get("received_at", 0) == 0 or m.get("received_at") is None)
        ]
        pending_count = len(pending)
        if not pending:
            # Batch has no pending unreceived messages: safe to advance cursor
            if inbox_cursor is not None:
                _safe_write_text(cursor_file, str(inbox_cursor))
            print(json.dumps({}))
            return 0

        # Check budget before claiming
        if time_remaining() <= 0.2:
            sys.stderr.write("[agy_hook] budget exhausted before claim POST; skipping\n")
            print(json.dumps({}))
            return 0

        # 6. Persist claim intent before POST /v1/claim
        _safe_write_text(claim_marker, json.dumps({
            "status": "claiming",
            "request_id": request_id,
            "generation": generation,
            "phase": phase,
            "invocation_num": invocation_num,
            "timestamp": time.time(),
        }))

        try:
            claim_res = request_unix(
                service_socket,
                "POST",
                "/v1/claim",
                token=token,
                payload={"generation": generation, "request_id": request_id},
                timeout=remaining_timeout(2.5),
            )
        except Exception as exc:
            sys.stderr.write(f"[agy_hook] claim request failed: {exc}\n")
            print(json.dumps({}))
            return 0

        message = claim_res.get("message")
        attempt_id = claim_res.get("attempt_id")

        if not message or not attempt_id:
            _safe_write_text(claim_marker, json.dumps({
                "status": "empty",
                "request_id": request_id,
                "generation": generation,
                "timestamp": time.time(),
            }))
            print(json.dumps({}))
            return 0

        _safe_write_text(claim_marker, json.dumps({
            "status": "claimed",
            "message": message,
            "attempt_id": attempt_id,
            "request_id": request_id,
            "generation": generation,
            "timestamp": time.time(),
        }))

    message_id = message["message_id"]

    # 7. Check if message was already attempted across ANY generation
    if _is_message_attempted(task_dir, message_id):
        print(json.dumps({}))
        return 0

    # 8. Persist output attempt identity before stdout output
    msg_marker = task_dir / f"attempted-{generation}-{message_id}.json"
    _safe_write_text(msg_marker, json.dumps({
        "message_id": message_id,
        "attempt_id": attempt_id,
        "generation": generation,
        "status": "attempted",
        "timestamp": time.time(),
    }))
    legacy_msg_marker = task_dir / f"attempted-{generation}-{message_id}.txt"
    _safe_write_text(legacy_msg_marker, f"{attempt_id}\n")

    # 9. Persist pending receipt file (initially 'unknown' before stdout write)
    receipt_marker = task_dir / f"receipt-{generation}-{message_id}-{attempt_id}.json"
    _safe_write_text(receipt_marker, json.dumps({
        "generation": generation,
        "message_id": message_id,
        "attempt_id": attempt_id,
        "state": "unknown",
        "evidence": f"agy_hook_{phase}_attempted",
        "status": "pending",
        "timestamp": time.time(),
    }))

    # 10. Build safe prompt notification
    sender_id = message.get("sender_id") or message.get("from") or "unknown"
    task_id = message.get("task_id", "")
    mailbox_cli = Path(__file__).resolve().parent / "mailbox.py"
    cli_cmd = f"python3 {shlex.quote(str(mailbox_cli))} show --id {shlex.quote(message_id)}"

    user_text = (
        f"【通信服务收到协作信件｜任务关联：{task_id}】\n"
        f"消息编号：{message_id}\n"
        f"发件人：{sender_id}\n"
        f"这是原任务协作信件，请使用 read_message 工具（或运行 `{cli_cmd}`）查阅完整正文，"
        f"并在处理后使用 ack_message 确认收件：\n"
        f"message_id: {message_id}\n"
    )

    response = {
        "injectSteps": [
            {"userMessage": user_text}
        ]
    }
    if phase == "post":
        response["terminationBehavior"] = "force_continue"

    # 11. Write official hook JSON to stdout and flush
    out_json = json.dumps(response)
    sys.stdout.write(out_json + "\n")
    sys.stdout.flush()

    # 12. Only after stdout write and flush succeed, record 'offered' receipt intent
    _safe_write_text(receipt_marker, json.dumps({
        "generation": generation,
        "message_id": message_id,
        "attempt_id": attempt_id,
        "state": "offered",
        "evidence": f"agy_hook_{phase}_injected",
        "status": "pending",
        "timestamp": time.time(),
    }))

    # 13. Record invocation marker
    _safe_write_text(invoc_marker, json.dumps({
        "message_id": message_id,
        "attempt_id": attempt_id,
        "request_id": request_id,
        "generation": generation,
        "at": time.time(),
    }))

    # 14. Advance cursor only if no more pending messages remain in current batch
    if not recovering and pending_count <= 1 and inbox_cursor is not None:
        _safe_write_text(cursor_file, str(inbox_cursor))

    # 15. Submit 'offered' receipt to service
    try:
        request_unix(
            service_socket,
            "POST",
            "/v1/receipt",
            token=token,
            payload={
                "generation": generation,
                "message_id": message_id,
                "attempt_id": attempt_id,
                "state": "offered",
                "evidence": f"agy_hook_{phase}_injected",
            },
            timeout=remaining_timeout(2.5),
        )
        _safe_write_text(receipt_marker, json.dumps({
            "generation": generation,
            "message_id": message_id,
            "attempt_id": attempt_id,
            "state": "offered",
            "evidence": f"agy_hook_{phase}_injected",
            "status": "sent",
            "sent_at": time.time(),
        }))
    except Exception as exc:
        sys.stderr.write(f"[agy_hook] receipt recording failed: {exc}\n")
        # Receipt marker remains with status='pending', state='offered' for next hook to retry

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        sys.stderr.write(f"[agy_hook] unhandled error: {e}\n")
        print(json.dumps({}))
        sys.exit(0)
