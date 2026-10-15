#!/usr/bin/env python3
"""Host runtime and mailbox binding resolution for agent-mailbox v1.

Provides:
- binding_for_current(): locates the registered binding for the current CC chat.
- compute_scope(): computes the normalized deterministic scope fingerprint.
- Unix HTTP helpers for launcher mailbox registration, binding, activation, and heartbeat.

Control-file rules (registration, binding, admin token, index, lock) are enforced
on the *final* path component only: a symlink or non-regular file is refused, while
macOS system/tmp path aliases (which only alias parent directories) stay allowed.
"""

import errno
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import sys

try:
    from client import Client, MailboxError, MailboxTransportError, MailboxTimeout
except ImportError:
    try:
        from skill.scripts.client import Client, MailboxError, MailboxTransportError, MailboxTimeout
    except ImportError:
        Client = None

        class MailboxError(Exception):
            def __init__(self, code, status=400):
                self.code = code
                self.status = status
                super().__init__(f"MailboxError({code}, status={status})")

        class MailboxTransportError(Exception):
            pass

        class MailboxTimeout(MailboxTransportError):
            pass

PROTOCOL_NAME = "agent-mailbox-v1"

# The helper is a sibling skill in both known layouts:
#   <workspace>/skills/cc-agent-ipc/scripts/host_runtime.py
#   <workspace>/skills/cc-dsh-collaboration/scripts/ipc.py
# and in the integration source tree:
#   agentmailbox/integration/workspace/skills/cc-dsh-collaboration/scripts/ipc.py
_IPC_RELATIVE_CANDIDATES = (
    "cc-dsh-collaboration/scripts",
    "integration/workspace/skills/cc-dsh-collaboration/scripts",
)


def compute_scope(project, session_key, cwd, socket_path):
    """Compute normalized scope fingerprint for a chat environment.

    Fingerprint input is a 4-element JSON array of:
    [project, session_key, canonical_cwd, canonical_socket_path]
    encoded as UTF-8, ensure_ascii=False, separators=(',', ':').
    """
    proj = str(project or "").strip()
    sk = str(session_key or "").strip()
    c = str(Path(cwd).resolve()) if cwd else ""
    s = str(Path(socket_path).resolve()) if socket_path else ""
    raw = json.dumps([proj, sk, c, s], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return f"cc:{hashlib.sha256(raw).hexdigest()}"


def default_runtime_dir():
    """Default runtime directory for agent-mailbox bindings and state."""
    env_dir = os.environ.get("AGENT_MAILBOX_RUNTIME")
    if env_dir:
        return Path(env_dir).resolve()
    cc_data = Path(os.environ.get("CC_DATA_DIR") or Path.home() / ".cc-connect")
    return (cc_data / "mailbox").resolve()


def _open_control(path, flags, label, mode=0o600):
    """Open a control file without following a final symlink.

    Raises ValueError('{label}_symlink') for a symlink and
    ValueError('{label}_not_regular_file') for anything but a regular file.
    """
    try:
        fd = os.open(path, flags | os.O_NOFOLLOW, mode)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.EMLINK):
            raise ValueError(f"{label}_symlink") from exc
        raise
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode):
        os.close(fd)
        raise ValueError(f"{label}_not_regular_file")
    try:
        lst = os.lstat(path)
    except OSError as exc:
        os.close(fd)
        raise ValueError(f"{label}_unreadable") from exc
    if not os.path.samestat(st, lst):
        os.close(fd)
        raise ValueError(f"{label}_changed")
    return fd


def _read_control_bytes(path, label, require_private=True):
    """Read a regular control file into bytes.

    ``require_private`` enforces owner-only (0600) permissions. The binding
    index is a secret-free locator and may have been written by older code
    with default permissions, so it is read with type checks only.
    """
    fd = _open_control(path, os.O_RDONLY, label)
    try:
        if require_private and os.fstat(fd).st_mode & 0o077:
            raise ValueError(f"{label}_permissions_insecure")
        chunks = []
        while True:
            block = os.read(fd, 65536)
            if not block:
                break
            chunks.append(block)
    finally:
        os.close(fd)
    return b"".join(chunks)


def read_control_json(path, label, require_private=True):
    try:
        data = _read_control_bytes(path, label, require_private)
        return json.loads(data.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        if isinstance(exc, ValueError) and str(exc).startswith(f"{label}_"):
            raise
        raise ValueError(f"{label}_corrupted") from exc


def read_admin_token(admin_token):
    """Return the admin token string.

    A PathLike argument must name a readable private regular file; a read
    failure is an explicit error instead of treating the path text as a token.
    A plain string is the token itself (the launcher may pass it directly).
    """
    if isinstance(admin_token, os.PathLike):
        try:
            text = _read_control_bytes(admin_token, "admin_token").decode("utf-8").strip()
        except ValueError:
            raise
        except OSError as exc:
            raise ValueError(f"admin_token_unreadable: {exc}") from exc
        if not text:
            raise ValueError("admin_token_empty")
        return text
    if isinstance(admin_token, str):
        text = admin_token.strip()
        if not text:
            raise ValueError("admin_token_empty")
        return text
    raise ValueError("admin_token_invalid")


def resolve_mailbox_service(runtime_dir=None):
    """Find agent-mailbox Unix socket and admin token path.

    Order of precedence:
    1. AGENT_MAILBOX_SOCKET and AGENT_MAILBOX_ADMIN_TOKEN (or ADMIN_TOKEN_PATH) env vars.
    2. Candidate runtime directories: runtime_dir, AGENT_MAILBOX_RUNTIME, CC_DATA_DIR/mailbox.

    The admin token's final component must be a private regular file; a symlink
    or unsafe permission is refused instead of silently resolved.
    """
    sock = os.environ.get("AGENT_MAILBOX_SOCKET")
    tok = os.environ.get("AGENT_MAILBOX_ADMIN_TOKEN_PATH") or os.environ.get("AGENT_MAILBOX_ADMIN_TOKEN")
    if sock and tok and Path(sock).exists() and Path(tok).exists():
        _check_admin_token_path(tok)
        return Path(sock).resolve(), Path(tok).resolve()

    candidates = []
    if runtime_dir:
        candidates.append(Path(runtime_dir).resolve() / "mailbox")
        candidates.append(Path(runtime_dir).resolve())
    env_rt = os.environ.get("AGENT_MAILBOX_RUNTIME")
    if env_rt:
        candidates.append(Path(env_rt).resolve())
    cc_data = Path(os.environ.get("CC_DATA_DIR") or Path.home() / ".cc-connect")
    candidates.append(cc_data / "mailbox")

    for c in candidates:
        s = c / "run" / "api.sock"
        if not s.exists():
            s = c / "api.sock"
        t = c / "admin.token"
        if not t.exists():
            t = c / "admin-token"
        if s.exists() and t.exists():
            _check_admin_token_path(t)
            return s.resolve(), t.resolve()
    return None, None


def _check_admin_token_path(path):
    try:
        st = os.lstat(path)
    except OSError as exc:
        raise ValueError(f"admin_token_unreadable: {exc}") from exc
    if stat.S_ISLNK(st.st_mode):
        raise ValueError("admin_token_symlink")
    if not stat.S_ISREG(st.st_mode):
        raise ValueError("admin_token_not_regular_file")
    if st.st_mode & 0o077:
        raise ValueError("admin_token_permissions_insecure")


def check_mailbox_service(socket_path, timeout=3):
    """Verify service health endpoint."""
    if not socket_path or not Path(socket_path).exists():
        return False
    try:
        res = request_unix(socket_path, "GET", "/v1/health", timeout=timeout)
        return res.get("protocol") == PROTOCOL_NAME
    except Exception:
        return False


def request_unix(socket_path, method, path, token=None, payload=None, timeout=12):
    """Make an HTTP/1.1 request over a private Unix domain socket without payload truncation."""
    if Client is not None:
        client = Client(str(socket_path), token, timeout=timeout)
        return client.request(method, path, payload)

    import http.client
    import socket as py_socket

    socket_path = Path(socket_path).resolve()
    sock = py_socket.socket(py_socket.AF_UNIX, py_socket.SOCK_STREAM)
    sock.settimeout(timeout)
    conn = http.client.HTTPConnection("localhost", timeout=timeout)
    try:
        sock.connect(str(socket_path))
        conn.sock = sock
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None
        conn.request(method, path, body, headers)
        res = conn.getresponse()
        raw = res.read()
        data = json.loads(raw.decode("utf-8")) if raw else {}
        if 200 <= res.status < 300:
            return data
        error_code = data.get("error", f"http_{res.status}") if isinstance(data, dict) else f"http_{res.status}"
        raise MailboxError(error_code, res.status)
    finally:
        conn.close()
        sock.close()


_HELPER_MODULE_CACHE = {}


def _load_helper_at(cand):
    """Load exactly ``<cand>/ipc.py`` so a same-named module cannot shadow it."""
    import importlib.util

    target = Path(cand) / "ipc.py"
    if str(cand) not in sys.path:
        sys.path.insert(0, str(cand))
    spec = importlib.util.spec_from_file_location("cc_dsh_ipc_helper", target)
    if spec is None or spec.loader is None:
        return None
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:
        return None
    return module


def _usable_helper(module):
    return (module is not None and hasattr(module, "capture")
            and hasattr(module, "receipt_allows_retry"))


def get_ipc_module(helper_dir=None):
    """Locate the CC IPC helper without scanning the machine.

    An explicit trusted ``helper_dir`` wins. Otherwise the two known skill
    layouts relative to this file and the legacy ``~/.cc-connect/skills``
    install are tried by exact file path; only then is an already importable
    ``ipc`` accepted. A same-named unrelated module is never trusted.
    """
    cache_key = str(helper_dir) if helper_dir else ""
    if cache_key in _HELPER_MODULE_CACHE:
        return _HELPER_MODULE_CACHE[cache_key]

    if helper_dir:
        candidates = [Path(helper_dir)]
    else:
        here = Path(__file__).resolve()
        candidates = [here.parents[2] / rel for rel in _IPC_RELATIVE_CANDIDATES]
        candidates.append(Path.home() / ".cc-connect/skills/cc-dsh-collaboration/scripts")

    resolved = None
    for cand in candidates:
        if (cand / "ipc.py").is_file():
            module = _load_helper_at(cand)
            if _usable_helper(module):
                resolved = module
                break

    if resolved is None and not helper_dir:
        try:
            import ipc
            if _usable_helper(ipc):
                resolved = ipc
        except ImportError:
            pass

    _HELPER_MODULE_CACHE[cache_key] = resolved
    return resolved


def _verify_binding_file(binding_file, expected_scope):
    """Read and verify a binding file, including its own service identity.

    Returns the parsed binding dict. Raises RuntimeError on any unsafe control
    file, bad schema, scope mismatch, or a service ``me`` that is not this
    binding's mailbox.
    """
    data = read_control_json(binding_file, "binding")
    if not isinstance(data, dict) or data.get("schema") != 1:
        raise RuntimeError("binding_schema_invalid")
    socket_path = data.get("socket")
    token = data.get("token")
    mailbox_id = data.get("mailbox_id")
    if (not isinstance(socket_path, str) or not socket_path or not os.path.isabs(socket_path)
            or not isinstance(token, str) or not token
            or not isinstance(mailbox_id, str) or not mailbox_id):
        raise RuntimeError("binding_schema_invalid")
    if expected_scope is not None and data.get("scope") != expected_scope:
        raise RuntimeError("binding_scope_mismatch")
    try:
        res = request_unix(socket_path, "GET", "/v1/me", token=token, timeout=5)
    except Exception as exc:
        raise RuntimeError(f"binding_service_unavailable: {type(exc).__name__}") from exc
    mailbox = res.get("mailbox") if isinstance(res, dict) else None
    if not isinstance(mailbox, dict) or mailbox.get("mailbox_id") != mailbox_id:
        raise RuntimeError("binding_mailbox_mismatch")
    if expected_scope is not None and mailbox.get("scope") != expected_scope:
        # The local scope field alone cannot prove this mailbox belongs to the
        # current chat; the service's own scope must agree too.
        raise RuntimeError("binding_scope_mismatch")
    return data


def _find_binding_file(bindings_dir, scope):
    direct = bindings_dir / f"{scope}.json"
    if direct.exists() and not direct.is_symlink():
        return direct

    index_file = bindings_dir / "index.json"
    if index_file.exists() or index_file.is_symlink():
        index = read_control_json(index_file, "binding_index", require_private=False)
        mapped = index.get(scope) if isinstance(index, dict) else None
        if isinstance(mapped, str) and mapped:
            candidate = Path(mapped)
            if candidate.exists() and not candidate.is_symlink():
                return candidate
    raise RuntimeError(f"no_binding_found_for_scope: {scope}")


def binding_for_current():
    """Locate the registered private binding for the current CC chat.

    Resolution order:
    1. AGENT_MAILBOX_BINDING environment variable (explicit private binding).
    2. The current CC chat identity, which must be verified through the trusted
       IPC helper's capture() (real CC call). A capture failure is reported;
       environment variables are never used to construct a chat target.
    Returns the absolute path string of the verified binding file.
    """
    env_binding = os.environ.get("AGENT_MAILBOX_BINDING")
    if env_binding:
        path = Path(env_binding)
        if path.is_symlink() or not path.is_file():
            raise RuntimeError("binding_file_unsafe")
        _verify_binding_file(path, None)
        return str(path.resolve())

    ipc_mod = get_ipc_module()
    if ipc_mod is None:
        raise RuntimeError("cc_ipc_helper_unavailable: cannot verify the current chat identity")

    try:
        captured = ipc_mod.capture()
    except Exception as exc:
        raise RuntimeError(f"cc_identity_capture_failed: {exc}") from exc
    if not isinstance(captured, dict):
        raise RuntimeError("cc_identity_capture_invalid")
    target = captured.get("target")
    cc_socket = captured.get("socket")
    if not isinstance(target, dict) or not isinstance(cc_socket, str) or not cc_socket:
        raise RuntimeError("cc_identity_capture_invalid")
    for key in ("project", "session_key", "cwd"):
        if not isinstance(target.get(key), str) or not target[key]:
            raise RuntimeError("cc_identity_capture_invalid")

    scope = compute_scope(target["project"], target["session_key"], target["cwd"], cc_socket)
    binding_file = _find_binding_file(default_runtime_dir() / "bindings", scope)
    _verify_binding_file(binding_file, scope)
    return str(binding_file.resolve())


def _fsync_dir(directory):
    """Best-effort directory fsync so a rename survives a crash."""
    try:
        fd = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_json(path, data, label):
    """Write a private control file atomically; refuses an unsafe final path."""
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        st = None
    if st is not None:
        if stat.S_ISLNK(st.st_mode):
            raise ValueError(f"{label}_symlink")
        if not stat.S_ISREG(st.st_mode):
            raise ValueError(f"{label}_not_regular_file")

    tmp_path = path.parent / f".tmp-{path.name}-{secrets.token_hex(8)}"
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(path.parent)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def register_task_mailbox(
    service_socket,
    admin_token,
    registration_key,
    scope,
    team="",
    label="",
    kind="hermes",
    coordinator=False,
    parent_id="",
    metadata=None,
    task_dir=None,
):
    """Register a mailbox with the service idempotently using private persistence.

    First persists the registration request and token to a private file, then POSTs
    register. A repeat with the same immutable request reuses the exact same token
    and request; any conflict or a corrupted/linked control file is refused.
    Raises ValueError('registration_conflict') on conflicting registration parameters.
    """
    for name, value in (("registration_key", registration_key), ("scope", scope),
                        ("kind", kind), ("team", team), ("label", label), ("parent_id", parent_id)):
        if not isinstance(value, str):
            raise ValueError(f"registration_invalid: {name}")
    if not registration_key.strip() or not scope:
        raise ValueError("registration_invalid: registration_key/scope required")
    if metadata is not None and not isinstance(metadata, dict):
        raise ValueError("registration_invalid: metadata")

    base_dir = Path(task_dir) if task_dir else default_runtime_dir() / "registrations"
    base_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    safe_name = hashlib.sha256(registration_key.encode("utf-8")).hexdigest()[:16]
    reg_file = base_dir / f"reg-{safe_name}.json"
    lock_file = base_dir / f"reg-{safe_name}.lock"

    try:
        lock_fd = os.open(lock_file, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as exc:
        if exc.errno in (errno.ELOOP, errno.EMLINK):
            raise ValueError("registration_lock_symlink") from exc
        if exc.errno == errno.EISDIR:
            raise ValueError("registration_lock_not_regular_file") from exc
        raise
    if not stat.S_ISREG(os.fstat(lock_fd).st_mode):
        os.close(lock_fd)
        raise ValueError("registration_lock_not_regular_file")
    os.fchmod(lock_fd, 0o600)

    with os.fdopen(lock_fd, "a+", encoding="utf-8") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            req_payload = None
            token = None

            if reg_file.exists() or reg_file.is_symlink():
                existing = read_control_json(reg_file, "registration")
                if not isinstance(existing, dict):
                    raise ValueError("registration_corrupted")
                if (
                    existing.get("registration_key") != registration_key
                    or existing.get("scope") != scope
                    or existing.get("team", "") != (team or "")
                    or existing.get("label", "") != (label or kind)
                    or existing.get("kind") != kind
                    or bool(existing.get("coordinator")) != bool(coordinator)
                    or existing.get("parent_id", "") != (parent_id or "")
                    or existing.get("metadata", {}) != (metadata or {})
                ):
                    raise ValueError("registration_conflict")
                token = existing.get("token")
                if not token or not isinstance(token, str):
                    raise ValueError("registration_corrupted")
                req_payload = existing

            if req_payload is None:
                token = secrets.token_hex(24)
                req_payload = {
                    "registration_key": registration_key,
                    "token": token,
                    "label": label or kind,
                    "kind": kind,
                    "coordinator": bool(coordinator),
                    "scope": scope,
                    "team": team or "",
                    "parent_id": parent_id or "",
                    "metadata": metadata or {},
                }
                atomic_write_json(reg_file, req_payload, "registration")

            admin_token_str = read_admin_token(admin_token)
            res = request_unix(
                service_socket,
                "POST",
                "/v1/register",
                token=admin_token_str,
                payload=req_payload,
            )
            return res.get("mailbox", {}), token
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def create_binding_file(binding_path, service_socket, token, mailbox_id, generation, scope=None):
    """Write a private binding file with mode 0600 atomically.

    ``scope`` is an optional keyword used by launchers that verify the binding
    against the computed chat scope. The final path component must not be a
    symlink or non-regular file.
    """
    if not isinstance(token, str) or not token:
        raise ValueError("binding_token_required")
    if not isinstance(mailbox_id, str) or not mailbox_id:
        raise ValueError("binding_mailbox_required")
    if not isinstance(generation, str) or not generation:
        raise ValueError("binding_generation_required")

    path = Path(binding_path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    data = {
        "schema": 1,
        "socket": str(Path(service_socket).resolve()),
        "token": token,
        "mailbox_id": mailbox_id,
        "generation": generation,
    }
    if scope:
        data["scope"] = scope
    atomic_write_json(path, data, "binding_path")
    return path


def activate_mailbox(service_socket, token, generation, native_session_id="", capabilities=None):
    """Activate generation on the mailbox."""
    if Client is not None:
        client = Client(str(service_socket), token)
        return client.activate({
            "generation": generation,
            "native_session_id": native_session_id or "",
            "capabilities": capabilities or [],
        })
    res = request_unix(
        service_socket,
        "POST",
        "/v1/activate",
        token=token,
        payload={
            "generation": generation,
            "native_session_id": native_session_id or "",
            "capabilities": capabilities or [],
        },
    )
    return res.get("mailbox", res)


def heartbeat_mailbox(service_socket, token, generation, native_session_id=""):
    """Heartbeat to update the observed native session ID."""
    if Client is not None:
        client = Client(str(service_socket), token)
        return client.heartbeat({
            "generation": generation,
            "native_session_id": native_session_id or "",
        })
    res = request_unix(
        service_socket,
        "POST",
        "/v1/heartbeat",
        token=token,
        payload={
            "generation": generation,
            "native_session_id": native_session_id or "",
        },
    )
    return res.get("mailbox", res)


def deactivate_mailbox(service_socket, token, generation):
    """Deactivate generation on the mailbox upon completion/exit."""
    if Client is not None:
        client = Client(str(service_socket), token)
        return client.deactivate({"generation": generation})
    res = request_unix(
        service_socket,
        "POST",
        "/v1/deactivate",
        token=token,
        payload={"generation": generation},
    )
    return res.get("mailbox", res)
