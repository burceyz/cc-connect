"""MCP stdio server exposing the agent-mailbox client to a model.

One binding is fixed at startup. Tools cannot change socket, token, or sender.
Machine output goes to stdout as newline-delimited JSON-RPC; logs go to stderr.
"""

import argparse
import json
import sys
import time

from client import (
    MailboxError,
    MailboxTransportError,
    client_for,
    hint_for,
    load_binding,
)

SERVER_NAME = "agent-mailbox"
SERVER_VERSION = "1.0.0"
PROTOCOL_VERSION = "2024-11-05"
WAIT_MAX_SECONDS = 30.0
POLL_SECONDS = 0.5


class ToolArgumentError(Exception):
    """The model called a tool with missing or invalid arguments."""


TOOLS = [
    {
        "name": "list_peers",
        "description": "列出当前有通信权限的成员（同范围同协作组，或自己的协调者/执行成员）。",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
    {
        "name": "send_message",
        "description": "给指定成员发送一条消息。必须提供稳定 id 和 task_id。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "to": {"type": "string", "description": "精确的收件 mailbox_id"},
                "id": {"type": "string", "description": "本次发送的稳定 client_message_id"},
                "task_id": {"type": "string", "description": "任务关联标识"},
                "body": {"type": "string", "description": "UTF-8 正文原文"},
                "reply_to": {"type": "string", "description": "可选，原消息 message_id"},
            },
            "required": ["to", "id", "task_id", "body"],
            "additionalProperties": False,
        },
    },
    {
        "name": "read_message",
        "description": "按 message_id 读取原消息、当前传输状态、原领取信息与去重回执证据。只读，不领取。",
        "inputSchema": {
            "type": "object",
            "properties": {"message_id": {"type": "string"}},
            "required": ["message_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "ack_message",
        "description": "显式确认已读到某条发给自己的消息：只写独立的 received_at，不改变传输状态，也不是任务完成。",
        "inputSchema": {
            "type": "object",
            "properties": {"message_id": {"type": "string"}},
            "required": ["message_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "message_status",
        "description": "查看某条消息当前的传输状态、原领取信息与去重回执证据，用于公开对账。",
        "inputSchema": {
            "type": "object",
            "properties": {"message_id": {"type": "string"}},
            "required": ["message_id"],
            "additionalProperties": False,
        },
    },
    {
        "name": "wait_for_message",
        "description": "有界等待（至多 30 秒）自己的新消息；只读 inbox，不领取、不重复投递。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "after": {"type": "string", "description": "可选，上次返回的游标"},
                "timeout": {"type": "number", "description": "可选，秒，超过 30 按 30 计"},
            },
            "additionalProperties": False,
        },
    },
]


def _log(message):
    sys.stderr.write("agent-mailbox-mcp: %s\n" % message)
    sys.stderr.flush()


def _require_str(arguments, key):
    value = arguments.get(key)
    if not isinstance(value, str) or not value:
        raise ToolArgumentError("parameter %s is required and must be a non-empty string" % key)
    return value


class McpServer:
    def __init__(self, binding):
        self.binding = binding
        self.client = client_for(binding, timeout=WAIT_MAX_SECONDS + 5)

    def handle(self, request):
        if not isinstance(request, dict) or request.get("jsonrpc") != "2.0":
            return _error_response(None, -32600, "invalid request")
        method = request.get("method")
        request_id = request.get("id")

        if method in ("notifications/initialized", "initialized"):
            return None

        if method == "initialize":
            result = {
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": {"tools": {}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            }
        elif method == "ping":
            result = {}
        elif method == "tools/list":
            result = {"tools": TOOLS}
        elif method == "tools/call":
            params = request.get("params")
            if not isinstance(params, dict):
                if request_id is None:
                    return None
                return _error_response(request_id, -32602, "invalid params")
            result = self.call_tool(params.get("name"), params.get("arguments") or {})
        else:
            if request_id is None:
                return None
            return _error_response(request_id, -32601, "method not found")

        # JSON-RPC notifications carry no id and must not be answered.
        if request_id is None:
            return None
        return _result(request_id, result)

    def call_tool(self, name, arguments):
        try:
            if not isinstance(arguments, dict):
                raise ToolArgumentError("arguments must be an object")
            payload = self._dispatch(name, arguments)
        except ToolArgumentError as exc:
            return _error_result("invalid_request", str(exc))
        except MailboxError as exc:
            return _error_result(exc.code, hint_for(exc.code))
        except MailboxTransportError as exc:
            return _error_result("transport_error", str(exc))
        return _text_result(payload)

    def _dispatch(self, name, arguments):
        if name == "list_peers":
            return {"peers": self.client.peers()}
        if name == "send_message":
            message = self.client.send(
                {
                    "client_message_id": _require_str(arguments, "id"),
                    "to": _require_str(arguments, "to"),
                    "body": _require_str(arguments, "body"),
                    "task_id": _require_str(arguments, "task_id"),
                    "reply_to": arguments.get("reply_to") or "",
                }
            )
            return {"message": message}
        if name in ("read_message", "message_status"):
            return {"message": self.client.get(_require_str(arguments, "message_id"))}
        if name == "ack_message":
            return {"message": self.client.ack(_require_str(arguments, "message_id"))}
        if name == "wait_for_message":
            return self.wait_for_message(arguments.get("after"), arguments.get("timeout"))
        raise ToolArgumentError("unknown tool: %s" % name)

    def wait_for_message(self, after=None, timeout=None):
        seconds = WAIT_MAX_SECONDS
        if timeout is not None:
            try:
                seconds = float(timeout)
            except (TypeError, ValueError):
                raise ToolArgumentError("timeout must be a number") from None
        seconds = max(0.0, min(seconds, WAIT_MAX_SECONDS))
        deadline = time.monotonic() + seconds
        cursor = after
        while True:
            result = self.client.inbox(after=cursor)
            messages = result.get("messages") or []
            if messages:
                return {"messages": messages, "cursor": result.get("cursor")}
            cursor = result.get("cursor", cursor)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return {"messages": [], "cursor": cursor}
            time.sleep(min(POLL_SECONDS, remaining))


def _text_result(payload):
    return {"content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False)}]}


def _error_result(code, hint):
    text = json.dumps({"error": code, "hint": hint}, ensure_ascii=False)
    return {"content": [{"type": "text", "text": text}], "isError": True}


def _result(request_id, result):
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _error_response(request_id, code, message):
    return {"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}}


def _write(stdout, message):
    stdout.write(json.dumps(message, ensure_ascii=False) + "\n")
    stdout.flush()


def serve(binding, stdin=None, stdout=None):
    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    server = McpServer(binding)
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
        except ValueError:
            _write(stdout, _error_response(None, -32700, "parse error"))
            continue
        try:
            response = server.handle(request)
        except Exception as exc:  # keep the loop alive; do not leak raw internals
            _log("internal error: %s" % type(exc).__name__)
            request_id = request.get("id") if isinstance(request, dict) else None
            response = _error_response(request_id, -32603, "internal error")
        if response is not None:
            _write(stdout, response)


def main(argv=None):
    parser = argparse.ArgumentParser(description="agent-mailbox MCP stdio server")
    parser.add_argument("--binding", help="path to the private binding JSON")
    args = parser.parse_args(argv)
    try:
        binding = load_binding(args.binding)
    except ValueError as exc:
        _log(str(exc))
        return 2
    serve(binding)
    return 0


if __name__ == "__main__":
    sys.exit(main())
