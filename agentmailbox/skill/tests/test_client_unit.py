"""Unit tests for client.py using a small in-process Unix-socket HTTP stub.

These cover transport/error mapping without the Go service. The end-to-end
service tests live in test_mailbox_e2e.py.
"""

import http.server
import json
import os
import shutil
import socketserver
import sys
import tempfile
import threading
import types
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(os.path.dirname(HERE), "scripts")
sys.path.insert(0, SCRIPTS_DIR)

from client import (  # noqa: E402
    Client,
    MailboxError,
    MailboxTransportError,
    hint_for,
    load_binding,
    new_id,
    validate_binding,
)


class _FakeHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _respond(self, status, payload=None, raw=None):
        body = raw if raw is not None else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.server.requests.append((self.command, self.path, self.headers.get("Authorization")))
        if self.path == "/v1/health":
            self._respond(200, {"protocol": "agent-mailbox-v1", "version": "1", "instance_id": "inst_fake"})
        elif self.path == "/v1/me":
            self._respond(200, {"mailbox": {"mailbox_id": "mbx_fake"}})
        elif self.path == "/v1/conflict":
            self._respond(409, {"error": "generation_mismatch"})
        elif self.path == "/v1/notjson":
            self._respond(200, raw=b"<html>not json</html>")
        elif self.path == "/v1/redirect":
            self._respond(301, raw=b"")
        else:
            self._respond(404, {"error": "not_found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        self.rfile.read(length)
        self.server.requests.append((self.command, self.path, self.headers.get("Authorization")))
        self._respond(200, {"ok": True})


class ClientUnitTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="amb-fake-")
        self.socket_path = os.path.join(self.dir, "api.sock")
        self.server = socketserver.ThreadingUnixStreamServer(self.socket_path, _FakeHandler)
        self.server.daemon_threads = True
        self.server.requests = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        def teardown():
            self.server.shutdown()
            self.server.server_close()
            shutil.rmtree(self.dir, ignore_errors=True)

        self.addCleanup(teardown)
        self.client = Client(self.socket_path, "tok-1")

    def test_health_sends_bearer_and_returns_json(self):
        data = self.client.health()
        self.assertEqual(data["protocol"], "agent-mailbox-v1")
        self.assertEqual(self.server.requests[-1], ("GET", "/v1/health", "Bearer tok-1"))

    def test_error_maps_to_stable_code(self):
        with self.assertRaises(MailboxError) as caught:
            self.client.request("GET", "/v1/conflict")
        self.assertEqual(caught.exception.code, "generation_mismatch")
        self.assertEqual(caught.exception.status, 409)

    def test_non_json_response_is_transport_error(self):
        with self.assertRaises(MailboxTransportError):
            self.client.request("GET", "/v1/notjson")

    def test_missing_socket_is_transport_error(self):
        client = Client(os.path.join(self.dir, "missing.sock"), "tok")
        with self.assertRaises(MailboxTransportError):
            client.health()

    def test_new_id_is_prefixed_and_unique(self):
        first = new_id("c")
        second = new_id("c")
        self.assertTrue(first.startswith("c-"))
        self.assertNotEqual(first, second)

    def test_hint_for_known_and_unknown_codes(self):
        self.assertIn("代次", hint_for("generation_mismatch"))
        self.assertIn("unknown_code", hint_for("unknown_code"))

    def test_validate_binding_rejects_bad_shapes(self):
        valid = {"schema": 1, "socket": "/tmp/x.sock", "token": "t"}
        self.assertIs(validate_binding(valid), valid)
        for bad in [
            None,
            {"schema": 2, "socket": "/tmp/x.sock", "token": "t"},
            {"schema": 1, "socket": "relative.sock", "token": "t"},
            {"schema": 1, "socket": "/tmp/x.sock", "token": ""},
        ]:
            with self.assertRaises(ValueError):
                validate_binding(bad)

    def test_explicit_binding_does_not_use_host_runtime(self):
        module = types.ModuleType("host_runtime")

        def boom():
            raise AssertionError("host_runtime must not be used with an explicit binding")

        module.binding_for_current = boom
        sys.modules["host_runtime"] = module
        self.addCleanup(sys.modules.pop, "host_runtime", None)

        path = os.path.join(self.dir, "binding.json")
        binding = {"schema": 1, "socket": self.socket_path, "token": "tok-1", "mailbox_id": "mbx_1"}
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(binding, handle)
        self.assertEqual(load_binding(path)["socket"], self.socket_path)

        os.environ["AGENT_MAILBOX_BINDING"] = path
        try:
            self.assertEqual(load_binding()["socket"], self.socket_path)
        finally:
            del os.environ["AGENT_MAILBOX_BINDING"]

    def test_load_binding_reports_host_runtime_failure(self):
        module = types.ModuleType("host_runtime")

        def boom():
            raise RuntimeError("no runtime here")

        module.binding_for_current = boom
        sys.modules["host_runtime"] = module
        self.addCleanup(sys.modules.pop, "host_runtime", None)
        os.environ.pop("AGENT_MAILBOX_BINDING", None)
        with self.assertRaises(ValueError) as caught:
            load_binding()
        self.assertIn("host_runtime", str(caught.exception))

    def test_client_rejects_non_positive_timeout(self):
        for timeout in (0, -1):
            with self.assertRaises(ValueError):
                Client(self.socket_path, "tok-1", timeout=timeout)

    def test_get_encodes_message_id_and_rejects_bad_status(self):
        with self.assertRaises(MailboxError) as caught:
            self.client.get("a b?x=1/../me")
        self.assertEqual(caught.exception.code, "not_found")
        self.assertEqual(self.server.requests[-1][1], "/v1/messages/a%20b%3Fx%3D1%2F..%2Fme")

        with self.assertRaises(MailboxTransportError):
            self.client.request("GET", "/v1/redirect")


    def test_host_runtime_returns_binding_path(self):
        path = os.path.join(self.dir, "host-binding.json")
        binding = {"schema": 1, "socket": self.socket_path, "token": "tok-1", "mailbox_id": "mbx_1"}
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(binding, handle)
        module = types.ModuleType("host_runtime")
        module.binding_for_current = lambda: path
        sys.modules["host_runtime"] = module
        self.addCleanup(sys.modules.pop, "host_runtime", None)
        os.environ.pop("AGENT_MAILBOX_BINDING", None)
        self.assertEqual(load_binding()["socket"], self.socket_path)

    def test_host_runtime_import_failure_is_clear_error(self):
        with open(os.path.join(self.dir, "host_runtime.py"), "w", encoding="utf-8") as handle:
            handle.write("raise RuntimeError('host blew up at import')\n")
        sys.modules.pop("host_runtime", None)
        sys.path.insert(0, self.dir)
        os.environ.pop("AGENT_MAILBOX_BINDING", None)
        try:
            with self.assertRaises(ValueError) as caught:
                load_binding()
            self.assertIn("host_runtime", str(caught.exception))
        finally:
            sys.path.remove(self.dir)
            sys.modules.pop("host_runtime", None)

    def test_host_runtime_missing_path_is_clear_error(self):
        module = types.ModuleType("host_runtime")
        module.binding_for_current = lambda: os.path.join(self.dir, "nope.json")
        sys.modules["host_runtime"] = module
        self.addCleanup(sys.modules.pop, "host_runtime", None)
        os.environ.pop("AGENT_MAILBOX_BINDING", None)
        with self.assertRaises(ValueError) as caught:
            load_binding()
        self.assertIn("binding", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
