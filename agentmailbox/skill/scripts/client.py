"""Standard-library client for the agent-mailbox v1 local service.

The client speaks HTTP over a private Unix socket. It never retries on its own:
callers keep a stable request id and decide how to reconcile.
"""

import http.client
import json
import os
import secrets
import socket
from urllib.parse import quote

BINDING_ENV = "AGENT_MAILBOX_BINDING"


class MailboxError(Exception):
    """The service answered with a stable error code."""

    def __init__(self, code, status=None):
        super().__init__(code)
        self.code = code
        self.status = status

    def __str__(self):
        if self.status:
            return "%s (HTTP %s)" % (self.code, self.status)
        return self.code


class MailboxTransportError(Exception):
    """The request never produced a usable response."""


class MailboxTimeout(MailboxTransportError):
    """The request timed out; the outcome is unknown and must be reconciled."""


ERROR_HINTS = {
    "invalid_request": "请求缺少必需字段或字段不合法。",
    "invalid_cursor": "inbox 游标不是有效的非负整数。",
    "invalid_parent": "parent_id 不存在、与自己的 scope 不一致，或该 parent 不是协调者。",
    "unauthorized": "令牌缺失或错误，请检查 binding 与登记。",
    "peer_not_allowed": "目标不在同一 scope/协作组，也不是你的协调者或执行成员。",
    "reply_chain_invalid": "reply_to 不属于你和收件人之间的原消息链。",
    "not_found": "路径、邮箱或消息不存在，或你无权查看。",
    "registration_conflict": "同一登记键的请求内容与首次登记不同。",
    "idempotency_conflict": "同一 (sender, client_message_id) 已存在但内容不同。",
    "request_id_conflict": "该 request_id 已被另一个 generation 使用，请换新的 request_id。",
    "generation_mismatch": "generation 不是当前活跃代次，请先 activate 或改用当前代次。",
    "generation_ended": "该 generation 已结束，不能复活；请用新的 generation。",
    "capability_conflict": "同一活跃 generation 的 capabilities 不能悄悄变更。",
    "native_session_mismatch": "已登记的 native_session_id 与此值不同。",
    "attempt_mismatch": "receipt 的 attempt_id 不是该消息的原领取 attempt。",
    "payload_too_large": "请求体超过 128 KiB 或正文超过 60 KiB。",
    "internal": "服务内部错误；先查服务日志，不要凭此猜测重发。",
}


def hint_for(code):
    """Return a plain-language explanation for a stable error code."""
    return ERROR_HINTS.get(code, "未知错误码：%s" % code)


def new_id(prefix="id"):
    """Return a fresh stable id for client_message_id / request_id."""
    return "%s-%s" % (prefix, secrets.token_hex(8))


class _UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path, timeout):
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout)
            sock.connect(self._socket_path)
        except OSError:
            sock.close()
            raise
        self.sock = sock


class Client:
    """A thin, explicit client for one mailbox binding."""

    def __init__(self, socket, token, timeout=12):
        if not socket:
            raise ValueError("socket path is required")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be a positive number of seconds")
        self.socket = socket
        self.token = token
        self.timeout = timeout

    def request(self, method, path, payload=None):
        body = None
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        conn = _UnixHTTPConnection(self.socket, self.timeout)
        try:
            conn.request(method, path, body=body, headers=headers)
            response = conn.getresponse()
            raw = response.read()
        except socket.timeout as exc:
            raise MailboxTimeout("agent-mailbox timed out: %s %s" % (method, path)) from exc
        except http.client.HTTPException as exc:
            raise MailboxTransportError(
                "agent-mailbox request failed: %s: %s" % (type(exc).__name__, exc)
            ) from exc
        except OSError as exc:
            raise MailboxTransportError("agent-mailbox transport failed: %s" % exc) from exc
        finally:
            conn.close()

        data = {}
        if raw:
            try:
                data = json.loads(raw.decode("utf-8"))
            except ValueError as exc:
                raise MailboxTransportError(
                    "agent-mailbox returned non-JSON (HTTP %s)" % response.status
                ) from exc
        if response.status >= 400:
            code = data.get("error", "internal") if isinstance(data, dict) else "internal"
            raise MailboxError(code, response.status)
        if response.status < 200 or response.status >= 300:
            raise MailboxTransportError("agent-mailbox returned HTTP %s" % response.status)
        if not isinstance(data, dict):
            raise MailboxTransportError("agent-mailbox returned an unexpected response")
        return data

    # Convenience wrappers. Each returns exactly what PROTOCOL.md documents.
    def health(self):
        return self.request("GET", "/v1/health")

    def me(self):
        return self.request("GET", "/v1/me")["mailbox"]

    def peers(self):
        return self.request("GET", "/v1/peers")["peers"]

    def send(self, payload):
        return self.request("POST", "/v1/send", payload)["message"]

    def get(self, message_id):
        return self.request("GET", "/v1/messages/" + quote(str(message_id), safe=""))["message"]

    def inbox(self, after=None):
        path = "/v1/inbox"
        if after is not None:
            path += "?after=" + quote(str(after), safe="")
        return self.request("GET", path)

    def claim(self, payload):
        return self.request("POST", "/v1/claim", payload)

    def receipt(self, payload):
        return self.request("POST", "/v1/receipt", payload)["message"]

    def ack(self, message_id):
        return self.request("POST", "/v1/ack", {"message_id": message_id})["message"]

    def activate(self, payload):
        return self.request("POST", "/v1/activate", payload)["mailbox"]

    def heartbeat(self, payload):
        return self.request("POST", "/v1/heartbeat", payload)["mailbox"]

    def deactivate(self, payload):
        return self.request("POST", "/v1/deactivate", payload)["mailbox"]


def read_binding_file(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except OSError as exc:
        raise ValueError("cannot read binding %s: %s" % (path, exc)) from exc
    except ValueError as exc:
        raise ValueError("binding %s is not valid JSON" % path) from exc
    return validate_binding(data)


def validate_binding(data):
    if not isinstance(data, dict):
        raise ValueError("binding must be a JSON object")
    if data.get("schema") != 1:
        raise ValueError("binding schema must be 1")
    socket_path = data.get("socket")
    if not socket_path or not isinstance(socket_path, str) or not os.path.isabs(socket_path):
        raise ValueError("binding socket must be an absolute path")
    if not data.get("token") or not isinstance(data["token"], str):
        raise ValueError("binding token is required")
    return data


def load_binding(path=None):
    """Resolve a binding: explicit path > env var > host_runtime (lazy).

    host_runtime.binding_for_current() returns a binding file path, which is
    read and validated here. The host_runtime import only happens when no
    explicit binding is given, so an explicit-binding client never depends on
    the CC host runtime.
    """
    binding_path = path or os.environ.get(BINDING_ENV)
    if binding_path:
        return read_binding_file(binding_path)

    try:
        import host_runtime  # type: ignore
    except ImportError as exc:
        raise ValueError(
            "no binding: pass --binding, set %s, or provide host_runtime.binding_for_current()"
            % BINDING_ENV
        ) from exc
    except Exception as exc:  # a host module that fails to import must not traceback
        raise ValueError(
            "host_runtime import failed: %s: %s" % (type(exc).__name__, exc)
        ) from exc

    try:
        resolved = host_runtime.binding_for_current()
    except Exception as exc:  # report any host failure clearly, never as a traceback
        raise ValueError(
            "host_runtime.binding_for_current() failed: %s: %s" % (type(exc).__name__, exc)
        ) from exc
    if not resolved:
        raise ValueError("host_runtime.binding_for_current() returned no binding")
    if isinstance(resolved, str):
        # The host contract is a binding file path; read and validate it.
        return read_binding_file(resolved)
    if isinstance(resolved, dict):
        return validate_binding(resolved)
    raise ValueError(
        "host_runtime.binding_for_current() must return a binding file path, got %s"
        % type(resolved).__name__
    )


def client_for(binding, timeout=12):
    return Client(binding["socket"], binding["token"], timeout=timeout)
