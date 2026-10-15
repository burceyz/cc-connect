"""Shared test fixtures: a local Unix fake CC Connect and small helpers.

The fake CC mirrors the real ``core/session_ipc.go`` wire shape:
- ``POST /ipc/chat-target`` -> protocol + the requested address with live ids.
- ``POST /ipc/event`` -> a JSON receipt whose ``input`` is the exact request.
- ``GET /ipc/event?event_id=`` -> the stored receipt (or 404).
No production socket or state is touched.
"""

import copy
import http.server
import json
import os
from pathlib import Path
import socketserver
import threading
import time


def wait_for(predicate, timeout=10.0, interval=0.02):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


class FakeCCState:
    def __init__(self, project="test-project", session_key="feishu:chat:user",
                 cwd="/tmp/workspace", session_id="cc-session-1",
                 agent_session_id="native-1"):
        self.target = {
            "project": project,
            "session_key": session_key,
            "cwd": str(Path(cwd).resolve()),
            "session_id": session_id,
            "agent_session_id": agent_session_id,
        }
        self.chat_target_override = None
        self.supports_live_input = True
        self.supports_chat_routing = True
        self.events = {}
        self.post_calls = []
        self.get_calls = []
        self.deliver_mode = "delivered"  # delivered | waiting_retry | unknown | dispatching
        self.omit_input = False
        self.corrupt_input = False
        self.get_fail = False
        self.get_corrupt_input = False
        self.fail_post = False

    def live_target(self):
        return dict(self.target)


class _Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _json(self, status, value):
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _body(self):
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

    def do_GET(self):
        state = self.server.state
        if self.path == "/ipc/health":
            self._json(200, {"protocol": "cc-connect-session-ipc-v1", "transport": "unix",
                             "capabilities": ["chat_routing"]})
            return
        if self.path.startswith("/ipc/event?"):
            event_id = self.path.split("event_id=", 1)[-1]
            state.get_calls.append(event_id)
            if state.get_fail:
                self._json(500, {"code": "ipc_query_failed"})
                return
            receipt = state.events.get(event_id)
            if receipt is None:
                self._json(404, {"code": "ipc_receipt_unavailable"})
                return
            body = copy.deepcopy(receipt)
            if state.get_corrupt_input:
                body["input"] = {**body["input"], "prompt": body["input"]["prompt"] + "-corrupt"}
            self._json(200, body)
            return
        self._json(404, {"code": "not_found"})

    def do_POST(self):
        state = self.server.state
        if self.path == "/ipc/chat-target":
            body = self._body()
            if state.chat_target_override is not None:
                target = copy.deepcopy(state.chat_target_override)
            else:
                target = {
                    "project": body.get("project", ""),
                    "session_key": body.get("session_key", ""),
                    "session_id": state.target["session_id"],
                    "agent_session_id": state.target["agent_session_id"],
                    "cwd": body.get("cwd", ""),
                }
            self._json(200, {
                "protocol": "cc-connect-session-ipc-v1",
                "target": target,
                "busy": False,
                "supports_live_input": state.supports_live_input,
                "supports_chat_routing": state.supports_chat_routing,
            })
            return
        if self.path == "/ipc/event":
            body = self._body()
            state.post_calls.append(copy.deepcopy(body))
            if state.fail_post:
                self.close_connection = True
                return
            # Same byte cap the real core applies (core/session_ipc.go).
            if len(str(body.get("prompt", "")).encode("utf-8")) > 8192:
                self._json(400, {"code": "ipc_input_invalid"})
                return
            event_id = body.get("event_id", "")
            prior = state.events.get(event_id)
            if prior is not None and prior["input"] != body:
                self._json(409, {"code": "ipc_event_conflict"})
                return
            if prior is not None and prior.get("status") != "dispatching" and prior.get("status") != "waiting":
                self._json(200, copy.deepcopy(prior))
                return
            receipt = {"input": body, "sha256": "0" * 64, "status": "dispatching",
                       "boot": "b" * 32, "updated_at": "2026-10-15T00:00:00Z"}
            if state.deliver_mode == "delivered":
                receipt["status"] = "delivered"
                receipt["method"] = "start"
                receipt["delivery_target"] = state.live_target()
            elif state.deliver_mode == "waiting_retry":
                receipt["status"] = "waiting"
                receipt["code"] = "turn_not_ready"
            elif state.deliver_mode == "unknown":
                receipt["status"] = "unknown"
                receipt["code"] = "agent_input_failed_or_unconfirmed"
            state.events[event_id] = receipt
            body_out = copy.deepcopy(receipt)
            if state.omit_input:
                body_out.pop("input", None)
            if state.corrupt_input:
                body_out["input"] = {**body, "prompt": body["prompt"] + "-corrupt"}
            self._json(200, body_out)
            return
        self._json(404, {"code": "not_found"})


class FakeCC:
    """A private Unix-socket CC Connect double."""

    def __init__(self, socket_path, **state_kwargs):
        self.socket_path = Path(socket_path)
        self.state = FakeCCState(**state_kwargs)
        self._server = None
        self._thread = None

    def start(self):
        if self.socket_path.exists():
            self.socket_path.unlink()
        self._server = socketserver.UnixStreamServer(str(self.socket_path), _Handler)
        self._server.state = self.state
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        os.chmod(self.socket_path, 0o600)
        return self

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self.socket_path.exists():
            self.socket_path.unlink()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()


class _MailboxHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _json(self, status, value):
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _authorized(self):
        return self.headers.get("Authorization") == "Bearer " + self.server.state.admin_token

    def do_GET(self):
        state = self.server.state
        if self.path == "/v1/health":
            self._json(200, {"protocol": "agent-mailbox-v1", "version": "1", "instance_id": "inst_test"})
            return
        if self.path == "/v1/me":
            auth = self.headers.get("Authorization", "")
            token = auth[len("Bearer "):] if auth.startswith("Bearer ") else ""
            mailbox = state.mailboxes.get(token)
            if mailbox is None:
                self._json(401, {"error": "unauthorized"})
                return
            self._json(200, {"mailbox": mailbox})
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self):
        state = self.server.state
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        if self.path == "/v1/register":
            if not self._authorized():
                self._json(401, {"error": "unauthorized"})
                return
            token = body.get("token", "")
            mailbox = state.mailboxes.get(token)
            if mailbox is None:
                mailbox = {
                    "mailbox_id": f"mbx_{len(state.mailboxes) + 1:032x}",
                    "label": body.get("label", ""),
                    "kind": body.get("kind", ""),
                    "scope": body.get("scope", ""),
                    "team": body.get("team", ""),
                }
                state.mailboxes[token] = mailbox
            self._json(200, {"mailbox": mailbox})
            return
        self._json(404, {"error": "not_found"})


class _MailboxServer(socketserver.UnixStreamServer):
    pass


class FakeMailbox:
    """A minimal private agent-mailbox double for /v1/health, /v1/me, /v1/register."""

    def __init__(self, socket_path, admin_token="admin-token"):
        self.socket_path = Path(socket_path)
        self.admin_token = admin_token
        self.mailboxes = {}
        self._server = None
        self._thread = None

    def start(self):
        if self.socket_path.exists():
            self.socket_path.unlink()
        self._server = _MailboxServer(str(self.socket_path), _MailboxHandler)
        self._server.state = self
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        os.chmod(self.socket_path, 0o600)
        return self

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self.socket_path.exists():
            self.socket_path.unlink()

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.stop()
