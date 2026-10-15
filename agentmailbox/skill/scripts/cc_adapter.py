#!/usr/bin/env python3
"""Atlas out-of-process adapter for CC Connect.

Discovers active CC Connect chat sessions without restarting CC Connect or
modifying its binary/config. Registers/updates Atlas coordinator mailboxes in
the agent-mailbox service, claims messages for Atlas mailboxes, formats safe
CLI retrieval commands, and injects them into CC Connect over the private Unix
``/ipc/event`` route with ``routing='chat'`` and deterministic 64-hex event IDs.

Recovery rules:
- Claims persist the exact request id, returned message and attempt before any
  delivery; a lost claim response is replayed with the same request id.
- CC events persist their exact immutable input before the first POST. An
  existing event file is always read back, never rebuilt from today's target.
- A 200 receipt is accepted only when its full ``input`` equals the saved input.
  Only explicit "unsent" receipts (via ``ipc.receipt_allows_retry``) authorize
  re-POSTing the same input; unknown/delivered/query failures never do.
"""

import argparse
import errno
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import signal
import socket
import stat
import sys
import time

try:
    from host_runtime import (
        request_unix,
        compute_scope,
        default_runtime_dir,
        register_task_mailbox,
        create_binding_file,
        activate_mailbox,
        heartbeat_mailbox,
        deactivate_mailbox,
        get_ipc_module,
        read_control_json,
        read_admin_token,
        atomic_write_json,
        MailboxError,
    )
except ImportError:
    from skill.scripts.host_runtime import (
        request_unix,
        compute_scope,
        default_runtime_dir,
        register_task_mailbox,
        create_binding_file,
        activate_mailbox,
        heartbeat_mailbox,
        deactivate_mailbox,
        get_ipc_module,
        read_control_json,
        read_admin_token,
        atomic_write_json,
        MailboxError,
    )

CC_PROTOCOL = "cc-connect-session-ipc-v1"
CC_EVENT_ID_RE = re.compile(r"^[a-f0-9]{64}$")
ADAPTER_CAPABILITIES = ["coordinator", "cc_ipc"]
MAX_CLAIMS_PER_CYCLE = 200


def call_cc_socket(socket_path, method, path, payload=None, timeout=12):
    """Make an HTTP request to the CC Connect private Unix socket."""
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    conn = http.client.HTTPConnection("localhost", timeout=timeout)
    try:
        sock.connect(str(socket_path))
        conn.sock = sock
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        body = (
            json.dumps(payload, ensure_ascii=False).encode("utf-8")
            if payload is not None
            else None
        )
        conn.request(method, path, body, headers)
        res = conn.getresponse()
        raw = res.read()
        data = json.loads(raw.decode("utf-8")) if raw else {}
        return res.status, data
    finally:
        conn.close()
        sock.close()


def _read_config(path):
    """Read the adapter's private JSON config, refusing a linked final path."""
    try:
        st = os.lstat(path)
    except OSError as exc:
        raise RuntimeError(f"config_file_not_found: {path}") from exc
    if stat.S_ISLNK(st.st_mode):
        raise RuntimeError("config_file_symlink")
    if not stat.S_ISREG(st.st_mode):
        raise RuntimeError("config_file_not_regular_file")
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"config_file_unreadable: {exc}") from exc


def _clip_utf8(text, max_bytes):
    """Cut text at a UTF-8 character boundary so it fits ``max_bytes`` bytes."""
    raw = text.encode("utf-8")
    if len(raw) <= max_bytes:
        return text
    return raw[:max_bytes].decode("utf-8", "ignore")


def _open_lock(lock_file):
    """Open the single-instance lock without following a final symlink."""
    try:
        fd = os.open(lock_file, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.EMLINK):
            raise RuntimeError("cc_adapter_lock_symlink") from exc
        if exc.errno == errno.EISDIR:
            raise RuntimeError("cc_adapter_lock_not_regular_file") from exc
        raise
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise RuntimeError("cc_adapter_lock_not_regular_file")
    os.fchmod(fd, 0o600)
    return os.fdopen(fd, "a+", encoding="utf-8")


def _write_new_json(path, data):
    """Atomically create ``path``; return False if it already existed."""
    tmp = path.parent / f".tmp-{path.name}-{secrets.token_hex(6)}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(tmp, path)
            return True
        except FileExistsError:
            return False
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


class CcAdapter:
    def __init__(self, config_path):
        config_path = Path(config_path)
        if not config_path.is_file():
            raise RuntimeError(f"config_file_not_found: {config_path}")
        self.config = _read_config(config_path)
        self.config_path = config_path.resolve()

        service_sock_path = self.config.get("service_socket")
        if not service_sock_path:
            raise KeyError("missing required config key: service_socket")
        self.service_socket = Path(service_sock_path).resolve()

        admin_token_path = self.config.get("admin_token_path")
        if not admin_token_path:
            raise KeyError("missing required config key: admin_token_path")
        self.admin_token_path = Path(admin_token_path)

        cc_sock_path = self.config.get("cc_socket")
        if not cc_sock_path:
            raise KeyError("missing required config key: cc_socket")
        self.cc_socket = Path(cc_sock_path).resolve()

        self.cc_project = str(self.config.get("cc_project", "")).strip()
        self.allowed_projects = set(self.config.get("allowed_projects", []))
        if self.cc_project:
            self.allowed_projects.add(self.cc_project)

        self.cc_cwd = str(Path(self.config.get("cc_cwd", os.getcwd())).resolve())
        self.session_index_path = (
            Path(self.config["session_index_path"]).resolve()
            if self.config.get("session_index_path")
            else None
        )

        self.runtime_dir = Path(
            self.config.get("runtime_dir") or (default_runtime_dir() / "adapter")
        ).resolve()
        self.bindings_dir = default_runtime_dir() / "bindings"
        self.claims_dir = self.runtime_dir / "claims"
        self.events_dir = self.runtime_dir / "events"
        self.chats_dir = self.runtime_dir / "chats"
        self.registrations_dir = self.runtime_dir / "registrations"

        for d in (self.runtime_dir, self.bindings_dir, self.claims_dir, self.events_dir,
                  self.chats_dir, self.registrations_dir):
            d.mkdir(parents=True, exist_ok=True, mode=0o700)

        # Single daemon lock
        self.lock_file = self.runtime_dir / "cc_adapter.lock"
        self._lock_handle = _open_lock(self.lock_file)
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock_handle.close()
            raise RuntimeError("cc_adapter_already_running") from None

        self.poll_interval = float(
            self.config.get("poll_interval") or self.config.get("poll_interval_seconds") or 2.0
        )
        self.cli_name = self.config.get("mailbox_cli_path")
        if not self.cli_name:
            candidate = Path(__file__).resolve().parent / "mailbox.py"
            self.cli_name = str(candidate) if candidate.is_file() else "mailbox.py"

        self.active_targets = {}  # scope -> verified target dict
        self.atlas_mailboxes = {}  # scope -> persisted chat state
        self.inbox_cursors = {}  # scope -> last read inbox cursor (int)

        self.admin_token = read_admin_token(self.admin_token_path)
        self._load_saved_chats()

    def close(self):
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
            self._lock_handle.close()
        except Exception:
            pass

    # ------------------------------------------------------------------ state

    def _load_saved_chats(self):
        for f in sorted(self.chats_dir.glob("*.json")):
            if f.is_symlink() or not f.is_file():
                continue
            try:
                data = read_control_json(f, "chat_state")
            except (OSError, ValueError):
                continue
            if isinstance(data, dict) and isinstance(data.get("scope"), str):
                self.atlas_mailboxes[data["scope"]] = data

    def _save_chat_state(self, scope):
        data = self.atlas_mailboxes.get(scope)
        if data:
            atomic_write_json(self.chats_dir / f"{scope}.json", data, "chat_state")

    def _publish_binding(self, scope):
        """Persist the binding file and its index entry for an active generation."""
        mbx = self.atlas_mailboxes[scope]
        binding_file = self.bindings_dir / f"{scope}.json"
        create_binding_file(
            binding_file,
            self.service_socket,
            mbx["token"],
            mbx["mailbox_id"],
            mbx["generation"],
            scope=scope,
        )
        index_file = self.bindings_dir / "index.json"
        try:
            index = read_control_json(index_file, "binding_index", require_private=False)
        except FileNotFoundError:
            index = {}
        if not isinstance(index, dict):
            index = {}
        index[scope] = str(binding_file)
        atomic_write_json(index_file, index, "binding_index")

    # ------------------------------------------------------------- discovery

    def discover_active_sessions(self):
        """Read the CC session index and return (project, session_key, cwd).

        The real index is ``active_session: {full chat key: CC session id}`` and
        its ``sessions`` entries carry no project/cwd; both come from this
        adapter's trusted config. Only configured/allowed projects are used.
        """
        discovered = []
        if self.session_index_path and self.session_index_path.is_file():
            try:
                data = json.loads(self.session_index_path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                sys.stderr.write(f"[cc_adapter] failed reading session index: {exc}\n")
                data = {}
            active = data.get("active_session", {}) if isinstance(data, dict) else {}
            if isinstance(active, dict):
                project = self.cc_project
                if project and (not self.allowed_projects or project in self.allowed_projects):
                    for session_key in active:
                        if isinstance(session_key, str) and session_key:
                            discovered.append((project, session_key, self.cc_cwd))

        for session_key in self.config.get("session_keys", []):
            project = self.cc_project
            if project and (not self.allowed_projects or project in self.allowed_projects):
                discovered.append((project, str(session_key).strip(), self.cc_cwd))
        return discovered

    # ----------------------------------------------------------- chat syncing

    def sync_chat(self, project, session_key, cwd):
        """Verify the chat address with CC and ensure the Atlas mailbox matches."""
        req = {
            "project": project,
            "session_key": session_key,
            "session_id": "",
            "agent_session_id": "",
            "cwd": cwd,
        }
        try:
            status, res = call_cc_socket(self.cc_socket, "POST", "/ipc/chat-target", req)
        except OSError as exc:
            sys.stderr.write(f"[cc_adapter] CC socket unavailable: {exc}\n")
            return None

        if status != 200 or not isinstance(res, dict) or res.get("protocol") != CC_PROTOCOL:
            return None
        if res.get("supports_live_input") is not True or res.get("supports_chat_routing") is not True:
            return None

        target = res.get("target")
        if not isinstance(target, dict):
            return None
        if (target.get("project") != project or target.get("session_key") != session_key
                or target.get("cwd") != cwd):
            sys.stderr.write("[cc_adapter] chat-target identity mismatch; refusing to register another chat\n")
            return None
        if not isinstance(target.get("session_id"), str) or not target["session_id"]:
            return None
        if not isinstance(target.get("agent_session_id"), str):
            return None

        scope = compute_scope(project, session_key, cwd, self.cc_socket)
        verified = {
            "project": project,
            "session_key": session_key,
            "session_id": target["session_id"],
            "agent_session_id": target["agent_session_id"],
            "cwd": cwd,
        }
        self.active_targets[scope] = verified

        if scope not in self.atlas_mailboxes:
            return self._register_new(scope, verified)
        return self._sync_existing(scope, verified)

    def _register_new(self, scope, target):
        reg_key = f"atlas:{scope}"
        mbx_info, token = register_task_mailbox(
            service_socket=self.service_socket,
            admin_token=self.admin_token,
            registration_key=reg_key,
            scope=scope,
            team="",
            label=f"atlas-{self.cc_project or 'cc'}",
            kind="atlas",
            coordinator=True,
            parent_id="",
            metadata={"project": target["project"], "session_key": target["session_key"]},
            task_dir=self.registrations_dir,
        )
        mailbox_id = mbx_info.get("mailbox_id")
        if not mailbox_id:
            raise RuntimeError("registration_missing_mailbox_id")

        self.atlas_mailboxes[scope] = {
            "scope": scope,
            "mailbox_id": mailbox_id,
            "token": token,
            "generation": "",
            "pending_generation": secrets.token_hex(16),
            "cc_session_id": "",
            "agent_session_id": "",
            "phase": "switching",
        }
        self._save_chat_state(scope)
        return self._resume_switch(scope, target)

    def _sync_existing(self, scope, target):
        mbx = self.atlas_mailboxes[scope]
        if mbx.get("phase", "active") != "active":
            return self._resume_switch(scope, target)

        old_cc = mbx.get("cc_session_id", "")
        if target["session_id"] != old_cc:
            # /new (or a rebuilt chat session): a genuinely new execution.
            mbx["pending_generation"] = secrets.token_hex(16)
            mbx["phase"] = "switching"
            self._save_chat_state(scope)
            return self._resume_switch(scope, target)

        old_agent = mbx.get("agent_session_id", "")
        new_agent = target["agent_session_id"]
        if new_agent and not old_agent:
            # Fill an empty native id on the same generation.
            try:
                heartbeat_mailbox(self.service_socket, mbx["token"], mbx["generation"],
                                  native_session_id=new_agent)
            except Exception as exc:
                sys.stderr.write(f"[cc_adapter] heartbeat native-id fill error for {scope}: {exc}\n")
                return None
            mbx["agent_session_id"] = new_agent
            self._save_chat_state(scope)
            return scope

        if new_agent and old_agent and new_agent != old_agent:
            # A bound native id changed without a /new: this is still a genuinely
            # new execution, so advance the generation through the switch phase
            # instead of force-writing the old generation's native id.
            mbx["pending_generation"] = secrets.token_hex(16)
            mbx["phase"] = "switching"
            self._save_chat_state(scope)
            return self._resume_switch(scope, target)

        try:
            heartbeat_mailbox(self.service_socket, mbx["token"], mbx["generation"],
                              native_session_id=old_agent)
        except Exception:
            pass
        return scope

    def _resume_switch(self, scope, target):
        """Finish a generation switch; publish the binding only after success."""
        mbx = self.atlas_mailboxes[scope]
        new_gen = mbx.get("pending_generation") or secrets.token_hex(16)
        mbx["pending_generation"] = new_gen
        mbx["phase"] = "switching"
        self._save_chat_state(scope)

        old_gen = mbx.get("generation") or ""
        if old_gen and old_gen != new_gen:
            if not self._end_generation(mbx, old_gen):
                return None

        try:
            activate_mailbox(self.service_socket, mbx["token"], new_gen,
                             native_session_id=target["agent_session_id"],
                             capabilities=ADAPTER_CAPABILITIES)
        except MailboxError as exc:
            if exc.code != "generation_mismatch":
                sys.stderr.write(f"[cc_adapter] activate new generation failed on {scope}: {exc}\n")
                return None
            # The service may still hold a generation we did not record (for
            # example after local chat state was lost). End only that
            # generation; stored messages are never deleted.
            current = self._service_generation(mbx["token"])
            if not current or current == new_gen or not self._end_generation(mbx, current):
                sys.stderr.write(f"[cc_adapter] cannot clear the service generation on {scope}\n")
                return None
            try:
                activate_mailbox(self.service_socket, mbx["token"], new_gen,
                                 native_session_id=target["agent_session_id"],
                                 capabilities=ADAPTER_CAPABILITIES)
            except Exception as exc2:
                sys.stderr.write(f"[cc_adapter] activate after recovery failed on {scope}: {exc2}\n")
                return None
        except Exception as exc:
            sys.stderr.write(f"[cc_adapter] activate new generation failed on {scope}: {exc}\n")
            return None

        mbx["generation"] = new_gen
        mbx["pending_generation"] = ""
        mbx["cc_session_id"] = target["session_id"]
        mbx["agent_session_id"] = target["agent_session_id"]
        mbx["phase"] = "active"
        self._publish_binding(scope)
        self._save_chat_state(scope)
        sys.stderr.write(f"[cc_adapter] active generation for {scope}: {new_gen}\n")
        return scope

    def _end_generation(self, mbx, generation):
        """End one generation; 'already ended' is success, never delete data."""
        try:
            deactivate_mailbox(self.service_socket, mbx["token"], generation)
            return True
        except MailboxError as exc:
            if exc.code == "generation_mismatch":
                return True
            sys.stderr.write(f"[cc_adapter] deactivate {generation} failed: {exc}\n")
            return False
        except Exception as exc:
            sys.stderr.write(f"[cc_adapter] deactivate {generation} failed: {exc}\n")
            return False

    def _service_generation(self, token):
        """Read the service's currently active generation for this mailbox."""
        try:
            res = request_unix(self.service_socket, "GET", "/v1/me", token=token, timeout=5)
        except Exception:
            return None
        mailbox = res.get("mailbox") if isinstance(res, dict) else None
        if not isinstance(mailbox, dict) or not mailbox.get("active"):
            return None
        generation = mailbox.get("generation")
        return generation if isinstance(generation, str) and generation else None

    # ------------------------------------------------------------ claim/inbox

    def _claim_path(self, scope, generation):
        key = hashlib.sha256(f"{scope}|{generation}".encode("utf-8")).hexdigest()[:16]
        return self.claims_dir / f"claim-{key}.json"

    def _load_claim(self, path):
        if not path.exists() or path.is_symlink():
            return None
        try:
            record = read_control_json(path, "claim")
        except (OSError, ValueError):
            return None
        return record if isinstance(record, dict) else None

    def _save_claim(self, path, record):
        atomic_write_json(path, record, "claim")

    def _inbox_candidates(self, scope, token):
        after = self.inbox_cursors.get(scope)
        path = "/v1/inbox" + (f"?after={after}" if after is not None else "")
        try:
            res = request_unix(self.service_socket, "GET", path, token=token, timeout=5)
        except Exception as exc:
            sys.stderr.write(f"[cc_adapter] inbox read error for {scope}: {exc}\n")
            return [], after
        messages = res.get("messages", []) if isinstance(res, dict) else []
        cursor = res.get("cursor", after) if isinstance(res, dict) else after
        candidates = [
            m for m in messages
            if isinstance(m, dict) and m.get("state") == "pending" and not m.get("received_at")
        ]
        return candidates, cursor

    def _new_claim(self, scope, mbx, target, seq):
        record = {
            "scope": scope,
            "generation": mbx["generation"],
            "request_id": f"claim-atlas-{hashlib.sha256(scope.encode()).hexdigest()[:12]}-{mbx['generation']}-{seq}",
            "seq": seq,
            "status": "requested",
            "target": target,
            "message_id": "",
            "attempt_id": "",
            "task_id": "",
            "sender_id": "",
            "at": time.time(),
        }
        self._save_claim(self._claim_path(scope, mbx["generation"]), record)
        return record

    def _call_claim(self, mbx, record):
        try:
            res = request_unix(
                self.service_socket,
                "POST",
                "/v1/claim",
                token=mbx["token"],
                payload={"generation": record["generation"], "request_id": record["request_id"]},
                timeout=5,
            )
        except Exception as exc:
            sys.stderr.write(f"[cc_adapter] claim replay failed for {record.get('request_id')}: {exc}\n")
            return None
        if not isinstance(res, dict):
            return None
        return res.get("message"), res.get("attempt_id")

    def process_inbox(self, scope):
        """Claim and deliver pending mail for one active scope."""
        mbx = self.atlas_mailboxes.get(scope)
        target = self.active_targets.get(scope)
        if not mbx or not target or mbx.get("phase", "active") != "active":
            return

        token = mbx["token"]
        generation = mbx["generation"]
        path = self._claim_path(scope, generation)
        record = self._load_claim(path)

        for _ in range(MAX_CLAIMS_PER_CYCLE):
            if record is None or record.get("status") in ("delivered", "empty"):
                candidates, cursor = self._inbox_candidates(scope, token)
                if not candidates:
                    self.inbox_cursors[scope] = cursor
                    return
                seq = (record.get("seq", 0) + 1) if record else 1
                record = self._new_claim(scope, mbx, target, seq)

            if record.get("status") == "requested":
                result = self._call_claim(mbx, record)
                if result is None:
                    return
                message, attempt_id = result
                if not isinstance(message, dict) or not attempt_id:
                    record["status"] = "empty"
                    self._save_claim(path, record)
                    continue
                record["status"] = "claimed"
                record["message_id"] = message.get("message_id", "")
                record["attempt_id"] = attempt_id
                record["task_id"] = message.get("task_id", "")
                record["sender_id"] = message.get("sender_id", "")
                self._save_claim(path, record)

            message = {
                "message_id": record.get("message_id", ""),
                "task_id": record.get("task_id", ""),
                "sender_id": record.get("sender_id", ""),
            }
            self._deliver_to_cc(scope, message, record.get("attempt_id", ""), token, generation,
                                record.get("target") or target)
            record["status"] = "delivered"
            self._save_claim(path, record)

    # ----------------------------------------------------------- CC delivery

    def _build_cc_input(self, event_id, message, target):
        message_id = message.get("message_id", "")
        # CC caps the prompt at 8192 bytes, so clip the free-form fields first
        # and keep the CLI retrieval command intact.
        task_id = _clip_utf8(str(message.get("task_id", "")), 512)
        sender_id = _clip_utf8(str(message.get("sender_id", "")), 512)
        cli_cmd = f"python3 {shlex.quote(self.cli_name)} show --id {shlex.quote(message_id)}"
        prompt = _clip_utf8(
            f"【通信服务收到协作信件｜任务关联：{task_id}】\n"
            f"消息编号：{message_id}\n"
            f"发件人：{sender_id}\n"
            f"这是一封原任务协作信件，正文由外部成员提供。请使用以下命令查阅完整正文并按原授权继续：\n"
            f"{cli_cmd}\n",
            8000,
        )
        return {
            "event_id": event_id,
            "target": {
                "project": target["project"],
                "session_key": target["session_key"],
                "session_id": target.get("session_id", ""),
                "agent_session_id": target.get("agent_session_id", ""),
                "cwd": target["cwd"],
            },
            "prompt": prompt,
            "routing": "chat",
        }

    def _deliver_to_cc(self, scope, message, attempt_id, token, generation, target):
        """Persist the immutable event, then POST once or reconcile by query.

        A brand-new event may be POSTed once. An event file that already exists
        may already have reached CC, so it is only queried with GET; a re-POST is
        allowed solely by a matching 200 receipt that explicitly proves "not sent".
        """
        message_id = message.get("message_id", "")
        if not message_id or not attempt_id:
            return None
        event_id = hashlib.sha256(f"cc:{scope}:{message_id}:{attempt_id}".encode("utf-8")).hexdigest()
        event_file = self.events_dir / f"{event_id}.json"

        record = None
        if event_file.exists() and not event_file.is_symlink():
            try:
                record = read_control_json(event_file, "event")
            except (OSError, ValueError):
                record = None
        created = record is None
        if created:
            if not isinstance(target, dict):
                sys.stderr.write(f"[cc_adapter] no saved target for event {event_id}\n")
                return None
            record = {
                "event_id": event_id,
                "message_id": message_id,
                "attempt_id": attempt_id,
                "generation": generation,
                "scope": scope,
                "token": token,
                "input": self._build_cc_input(event_id, message, target),
                "status": "dispatching",
                "at": time.time(),
            }
            if not _write_new_json(event_file, record):
                created = False
                try:
                    record = read_control_json(event_file, "event")
                except (OSError, ValueError):
                    return None

        saved_input = record.get("input")
        if (not isinstance(saved_input, dict) or saved_input.get("event_id") != event_id
                or record.get("message_id") != message_id or record.get("attempt_id") != attempt_id):
            sys.stderr.write(f"[cc_adapter] saved event mismatch for {event_id}; refusing to overwrite\n")
            return record

        if not created:
            self._query_saved_event(event_file, record)
            return record

        try:
            status, res = call_cc_socket(self.cc_socket, "POST", "/ipc/event", saved_input)
        except Exception as exc:
            sys.stderr.write(f"[cc_adapter] CC delivery error for {event_id}: {exc}\n")
            record["status"] = "dispatching"
            self._save_event(event_file, record)
            return record
        if status != 200:
            self._record_cc_rejection(event_file, record, status, res)
            return record
        self._apply_receipt(event_file, record, res, allow_retry=False)
        return record

    def _query_saved_event(self, event_file, record):
        """GET one saved event and apply the receipt; this never POSTs."""
        if record.get("status") == "accepted":
            return
        event_id = record.get("event_id")
        if not isinstance(event_id, str) or not CC_EVENT_ID_RE.match(event_id):
            return
        try:
            status, res = call_cc_socket(self.cc_socket, "GET", f"/ipc/event?event_id={event_id}")
        except Exception:
            return
        if status != 200 or not isinstance(res, dict):
            return
        self._apply_receipt(event_file, record, res, allow_retry=True)

    def _record_cc_rejection(self, event_file, record, status, res):
        """A non-200 POST means no receipt was accepted; make it observable."""
        code = res.get("code", "") if isinstance(res, dict) else ""
        sys.stderr.write(f"[cc_adapter] CC rejected event {record.get('event_id')} (HTTP {status} {code})\n")
        try:
            request_unix(
                self.service_socket,
                "POST",
                "/v1/receipt",
                token=record["token"],
                payload={
                    "generation": record["generation"],
                    "message_id": record["message_id"],
                    "attempt_id": record["attempt_id"],
                    "state": "unknown",
                    "evidence": f"cc_event_rejected:{record['event_id']}:{status}:{code}",
                },
                timeout=5,
            )
        except Exception as exc:
            sys.stderr.write(f"[cc_adapter] rejected event receipt write failed: {exc}\n")
        record["status"] = "unknown"
        record["cc_status"] = f"http_{status}"
        self._save_event(event_file, record)

    def _save_event(self, event_file, record):
        atomic_write_json(event_file, record, "event")

    def _apply_receipt(self, event_file, record, res, allow_retry):
        """Apply one CC receipt only when its full input matches the saved input."""
        if not isinstance(res, dict):
            return
        saved_input = record.get("input")
        if not isinstance(saved_input, dict) or res.get("input") != saved_input:
            sys.stderr.write(f"[cc_adapter] CC receipt input mismatch for {record.get('event_id')}\n")
            return

        cc_status = res.get("status")
        if cc_status == "delivered":
            delivery = res.get("delivery_target")
            session_id = delivery.get("session_id", "") if isinstance(delivery, dict) else ""
            evidence = f"cc_event:{record['event_id']}:{session_id}"
            try:
                request_unix(
                    self.service_socket,
                    "POST",
                    "/v1/receipt",
                    token=record["token"],
                    payload={
                        "generation": record["generation"],
                        "message_id": record["message_id"],
                        "attempt_id": record["attempt_id"],
                        "state": "accepted",
                        "evidence": evidence,
                    },
                    timeout=5,
                )
            except Exception as exc:
                sys.stderr.write(f"[cc_adapter] accepted receipt write failed: {exc}\n")
                record["status"] = "unknown"
                record["cc_status"] = "delivered"
                self._save_event(event_file, record)
                return
            record["status"] = "accepted"
            record["cc_status"] = "delivered"
            self._save_event(event_file, record)
            return

        ipc_mod = get_ipc_module()
        allows_retry = getattr(ipc_mod, "receipt_allows_retry", None) if ipc_mod is not None else None
        if allows_retry is None:
            sys.stderr.write("[cc_adapter] CC IPC helper unavailable; only querying\n")
        elif allows_retry(res):
            if allow_retry:
                try:
                    retry_status, retry_res = call_cc_socket(
                        self.cc_socket, "POST", "/ipc/event", record["input"])
                except Exception as exc:
                    sys.stderr.write(f"[cc_adapter] retry POST failed for {record['event_id']}: {exc}\n")
                    record["status"] = "waiting_retry"
                    self._save_event(event_file, record)
                    return
                record["status"] = "waiting_retry"
                self._save_event(event_file, record)
                if retry_status == 200:
                    self._apply_receipt(event_file, record, retry_res, allow_retry=False)
                else:
                    self._record_cc_rejection(event_file, record, retry_status, retry_res)
                return
            record["status"] = "waiting_retry"
            self._save_event(event_file, record)
            return

        if cc_status == "unknown":
            try:
                request_unix(
                    self.service_socket,
                    "POST",
                    "/v1/receipt",
                    token=record["token"],
                    payload={
                        "generation": record["generation"],
                        "message_id": record["message_id"],
                        "attempt_id": record["attempt_id"],
                        "state": "unknown",
                        "evidence": f"cc_event_unknown:{record['event_id']}",
                    },
                    timeout=5,
                )
            except Exception as exc:
                sys.stderr.write(f"[cc_adapter] unknown receipt write failed: {exc}\n")
            record["status"] = "unknown"
            record["cc_status"] = "unknown"
            self._save_event(event_file, record)
            return

        # dispatching / blocked / waiting without retry evidence: only query later.
        record["status"] = "dispatching"
        self._save_event(event_file, record)

    # -------------------------------------------------------- reconciliation

    def reconcile_events(self):
        """Replay saved claims first, then query saved CC events (GET only)."""
        self._recover_claims()
        self._reconcile_deliveries()

    def _recover_claims(self):
        for path in sorted(self.claims_dir.glob("*.json")):
            if path.is_symlink() or not path.is_file():
                continue
            try:
                record = read_control_json(path, "claim")
            except (OSError, ValueError):
                continue
            if not isinstance(record, dict):
                continue
            scope = record.get("scope")
            mbx = self.atlas_mailboxes.get(scope) if isinstance(scope, str) else None
            if not mbx or not record.get("generation") or not record.get("request_id"):
                continue
            if not isinstance(record.get("target"), dict):
                record["target"] = self.active_targets.get(scope)
            if record.get("status") == "requested":
                result = self._call_claim(mbx, record)
                if result is None:
                    continue
                message, attempt_id = result
                if not isinstance(message, dict) or not attempt_id:
                    record["status"] = "empty"
                    self._save_claim(path, record)
                    continue
                record["status"] = "claimed"
                record["message_id"] = message.get("message_id", "")
                record["attempt_id"] = attempt_id
                record["task_id"] = message.get("task_id", "")
                record["sender_id"] = message.get("sender_id", "")
                self._save_claim(path, record)
            if record.get("status") == "claimed":
                message = {
                    "message_id": record.get("message_id", ""),
                    "task_id": record.get("task_id", ""),
                    "sender_id": record.get("sender_id", ""),
                }
                delivered = self._deliver_to_cc(scope, message, record.get("attempt_id", ""), mbx["token"],
                                                 record["generation"], record.get("target"))
                if isinstance(delivered, dict):
                    record["status"] = "delivered"
                    self._save_claim(path, record)

    def _reconcile_deliveries(self):
        for path in sorted(self.events_dir.glob("*.json")):
            if path.is_symlink() or not path.is_file():
                continue
            try:
                record = read_control_json(path, "event")
            except (OSError, ValueError):
                continue
            if not isinstance(record, dict) or record.get("status") not in (
                    "dispatching", "unknown", "waiting_retry"):
                continue
            self._query_saved_event(path, record)

    # ------------------------------------------------------------------ loop

    def run_once(self):
        """Execute one sync cycle: reconcile, discover chats, claim/inject mail."""
        self.reconcile_events()
        for project, session_key, cwd in self.discover_active_sessions():
            try:
                scope = self.sync_chat(project, session_key, cwd)
                if scope:
                    self.process_inbox(scope)
            except Exception as exc:
                sys.stderr.write(f"[cc_adapter] error processing session {session_key}: {exc}\n")

    def run_loop(self):
        """Run the daemon loop until terminated."""
        running = True

        def stop_signal(sig, frame):
            nonlocal running
            running = False

        signal.signal(signal.SIGINT, stop_signal)
        signal.signal(signal.SIGTERM, stop_signal)

        sys.stderr.write("[cc_adapter] started daemon\n")
        try:
            while running:
                try:
                    self.run_once()
                except Exception as exc:
                    sys.stderr.write(f"[cc_adapter] loop error: {exc}\n")
                time.sleep(self.poll_interval)
        finally:
            self.close()
        sys.stderr.write("[cc_adapter] stopped daemon\n")


def main():
    parser = argparse.ArgumentParser(description="Atlas CC Connect out-of-process adapter")
    parser.add_argument("--config", required=True, help="Path to adapter private JSON config")
    parser.add_argument("--once", action="store_true", help="Run a single sync cycle and exit")
    args = parser.parse_args()

    adapter = CcAdapter(args.config)
    try:
        if args.once:
            adapter.run_once()
        else:
            adapter.run_loop()
    finally:
        adapter.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
