#!/usr/bin/env python3
"""Opt-in DSH completion delivery to a pinned CC Connect conversation over Unix IPC."""
import argparse
from contextlib import contextmanager
import fcntl
import http.client
import json
import os
from pathlib import Path
import re
import socket
import stat
import subprocess
import sys
import tempfile
import time

import bridge

PROTOCOL = 'cc-connect-session-ipc-v1'
DELIVERIES = ('notify', 'conversation')
TARGET_KEYS = {'project', 'session_key', 'session_id', 'agent_session_id', 'cwd'}
TERMINAL = {'delivered', 'unknown', 'blocked', 'launch_failed', 'expired'}
RECEIVER_STATES = {'waiting', 'dispatching', 'delivered', 'unknown', 'blocked'}


class ReceiverOffline(Exception):
    """Connection failed before any HTTP request bytes were sent."""


class DeliveryUnknown(Exception):
    """A request may have arrived. Query its receipt before considering a retry."""


class ReceiverRejected(Exception):
    """A read-only identity lookup was explicitly rejected by the receiver."""

    def __init__(self, code):
        self.code = code
        super().__init__(code)


def write_state(path, value):
    """Private, atomic, durable outbox; callers hold the per-job callback lock."""
    fd, name = tempfile.mkstemp(prefix='.callback-', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def socket_path():
    data = Path(os.environ.get('CC_DATA_DIR') or Path.home() / '.cc-connect')
    return (data / 'run' / 'api.sock').resolve()


def call(path, method, route, body=None, *, identity_lookup=False):
    """No TCP fallback, proxy, webhook token, shell, or latest-session lookup."""
    path = Path(path)
    try:
        info = path.stat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('ipc_socket_not_private')
    except FileNotFoundError:
        raise ReceiverOffline() from None
    transport = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    transport.settimeout(12)
    conn = http.client.HTTPConnection('localhost', timeout=12)
    try:
        try:
            transport.connect(str(path))
        except OSError:
            raise ReceiverOffline() from None
        conn.sock = transport
        try:
            encoded = None if body is None else json.dumps(body, ensure_ascii=False).encode('utf-8')
            conn.request(method, route, encoded, {'Content-Type': 'application/json'})
            response = conn.getresponse()
            data = response.read(65537)
            if len(data) > 65536:
                raise DeliveryUnknown()
            value = json.loads(data)
            if not isinstance(value, dict):
                raise DeliveryUnknown()
            if response.status != 200:
                # Mutating calls retain their existing conservative unknown
                # semantics. Only an origin lookup may expose safe error codes.
                code = value.get('code', '')
                if (identity_lookup and method == 'POST' and route == '/ipc/origin' and
                        response.status in {400, 404, 409} and
                        isinstance(code, str) and re.fullmatch(r'ipc_[a-z_]{1,80}', code)):
                    raise ReceiverRejected(code)
                raise DeliveryUnknown()
            return value
        except (OSError, http.client.HTTPException, UnicodeError, ValueError):
            raise DeliveryUnknown() from None
    finally:
        conn.close()
        transport.close()


def workspace_cwd(value):
    """A fixed IPC identity may name the workspace or an existing canonical child."""
    if not isinstance(value, str) or not value or any(ord(char) < 32 for char in value):
        raise ValueError('ipc_target_workspace_mismatch')
    path = Path(value)
    if (not path.is_absolute() or str(path) != value or path.resolve() != path or
            not path.is_dir() or not path.is_relative_to(bridge.WORKSPACE.resolve())):
        raise ValueError('ipc_target_workspace_mismatch')
    return str(path)


def source_cwd():
    # The invoking host owns this environment; it is never a model argument.
    return workspace_cwd(os.environ.get('CC_IPC_CWD', str(bridge.WORKSPACE.resolve())))


def validate_binding(binding):
    if not isinstance(binding, dict) or set(binding) != {'protocol', 'socket', 'target'}:
        raise ValueError('ipc_binding_invalid')
    if (binding['protocol'] != PROTOCOL or not isinstance(binding['socket'], str) or
            not Path(binding['socket']).is_absolute() or any(ord(c) < 32 for c in binding['socket'])):
        raise ValueError('ipc_binding_invalid')
    target = binding['target']
    if not isinstance(target, dict) or set(target) != TARGET_KEYS:
        raise ValueError('ipc_target_invalid')
    if any(not isinstance(v, str) or not v or len(v) > 2048 or any(ord(c) < 32 for c in v)
           for v in target.values()):
        raise ValueError('ipc_target_invalid')
    workspace_cwd(target['cwd'])
    return binding


def capture():
    origin = os.environ.get('CC_IPC_ORIGIN')
    if origin is not None:
        return capture_origin(origin)
    target = {'project': os.environ.get('CC_PROJECT', ''),
              'session_key': os.environ.get('CC_SESSION_KEY', ''), 'session_id': '',
              'agent_session_id': os.environ.get('CODEX_THREAD_ID', ''),
              'cwd': source_cwd()}
    if not all(target[k] for k in ['project', 'session_key', 'agent_session_id']):
        raise ValueError('ipc_origin_environment_missing')
    path = socket_path()
    try:
        value = call(path, 'POST', '/ipc/target', target)
    except (ReceiverOffline, DeliveryUnknown):
        raise ValueError('ipc_receiver_unavailable_or_unadapted') from None
    if value.get('protocol') != PROTOCOL:
        raise ValueError('ipc_protocol_mismatch')
    if value.get('supports_live_input') is not True:
        raise ValueError('ipc_live_input_backend_required')
    pinned = value.get('target', {})
    if any(pinned.get(k) != v for k, v in target.items() if k != 'session_id'):
        raise ValueError('ipc_origin_mismatch')
    return validate_binding({'protocol': PROTOCOL, 'socket': str(path), 'target': pinned})


def capture_origin(origin):
    """Resolve the current live instance; never fall back to another identity."""
    if not re.fullmatch(r'cc-origin-v1-[0-9a-f]{64}', origin):
        raise ValueError('ipc_origin_invalid')
    requested = {'origin_id': origin, 'project': os.environ.get('CC_PROJECT', ''),
                 'session_key': os.environ.get('CC_SESSION_KEY', ''),
                 'cwd': source_cwd()}
    if not requested['project'] or not requested['session_key']:
        raise ValueError('ipc_origin_environment_missing')
    path = socket_path()
    try:
        value = call(path, 'POST', '/ipc/origin', requested, identity_lookup=True)
    except ReceiverRejected as exc:
        raise ValueError(exc.code) from None
    except (ReceiverOffline, DeliveryUnknown):
        raise ValueError('ipc_receiver_unavailable_or_unadapted') from None
    if value.get('protocol') != PROTOCOL:
        raise ValueError('ipc_protocol_mismatch')
    if value.get('supports_live_input') is not True:
        raise ValueError('ipc_live_input_backend_required')
    pinned = value.get('target', {})
    if any(pinned.get(key) != requested[key] for key in ('project', 'session_key', 'cwd')):
        raise ValueError('ipc_origin_mismatch')
    return validate_binding({'protocol': PROTOCOL, 'socket': str(path), 'target': pinned})


def require_origin(binding):
    """Verify the caller is the pinned recipient before acknowledging input."""
    binding = validate_binding(binding)
    if 'CC_IPC_ORIGIN' not in os.environ and any(os.environ.get(env) != binding['target'][key] for env, key in (
            ('CC_PROJECT', 'project'), ('CC_SESSION_KEY', 'session_key'),
            ('CODEX_THREAD_ID', 'agent_session_id'))):
        raise ValueError('ipc_ack_not_origin_session')
    if capture() != binding:
        raise ValueError('ipc_ack_not_origin_session')


def configure(delivery=None, binding=None, prior=None):
    """Omission inherits an accepted request; new tasks retain notify-only default."""
    previous_mode = (prior or {}).get('delivery', 'notify')
    mode = previous_mode if delivery is None else delivery
    if mode not in DELIVERIES:
        raise ValueError('invalid_delivery')
    if prior is not None and mode != previous_mode:
        raise ValueError('delivery_conflict')
    if mode == 'notify':
        if binding is not None:
            raise ValueError('unexpected_callback_target')
        return {}
    if prior is not None:
        accepted = prior.get('callback_target')
        if binding is not None and binding != accepted:
            raise ValueError('callback_target_conflict')
        binding = accepted
    elif binding is None:
        binding = capture()
    return {'delivery': mode, 'callback_target': validate_binding(binding)}


def validate_record(record, request):
    if record.get('delivery', 'notify') != request.get('delivery', 'notify'):
        raise ValueError('delivery_mismatch')
    if request.get('delivery') == 'conversation':
        validate_binding(request.get('callback_target'))
        if record.get('callback') != PROTOCOL or record.get('callback_target') != request['callback_target']:
            raise ValueError('callback_target_mismatch')


def prepare(dest, request):
    if request.get('delivery') == 'conversation':
        write_state(dest / 'callback.json', {'status': 'waiting_task', 'updated_at': bridge.now()})


def read(dest):
    path = dest / 'callback.json'
    return json.loads(path.read_text(encoding='utf-8')) if path.is_file() else None


@contextmanager
def callback_locked(dest, lease_fd=None):
    path = dest / 'callback.lock'
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600) if lease_fd is None else lease_fd
    with os.fdopen(fd, 'a') as lock:
        info = os.fstat(lock.fileno())
        if not stat.S_ISREG(info.st_mode) or not os.path.samestat(info, path.stat(follow_symlinks=False)):
            raise ValueError('callback_lease_invalid')
        os.fchmod(lock.fileno(), 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('callback_watcher_busy') from None
        # Close without LOCK_UN: a spawned watcher inherits this open-file lease.
        yield lock


def launch(dest, runtime):
    with callback_locked(dest) as lease:
        request = json.loads((dest / 'request.json').read_text())
        if request.get('delivery') != 'conversation':
            raise ValueError('callback_not_requested')
        state = read(dest) or {}
        if state.get('status') in TERMINAL:
            raise ValueError('callback_requires_reconciliation')
        try:
            proc = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()), '_watch',
                                     '--job', dest.name, '--runtime', str(runtime.resolve()),
                                     '--lease-fd', str(lease.fileno())],
                                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                    stderr=subprocess.DEVNULL, start_new_session=True,
                                    pass_fds=(lease.fileno(),))
        except OSError:
            return update(dest, state, status='launch_failed', code='callback_watcher_launch_failed')
        # The child owns its PID ledger and subsequent state writes, as jobs.worker does.
        return {'status': 'watching', 'pid': proc.pid}


def clean_exit(record):
    c = record.get('cleanup', {})
    if c.get('worker_lock') != 'released':
        return False
    if c.get('worker_process') not in ({'absent', 'not_recorded'} if record['status'] == 'launch_failed' else {'absent'}):
        return False
    if record.get('run_dir'):
        return c.get('dsh_process') == 'absent' and c.get('dsh_process_group') == 'absent'
    return record['status'] != 'orphaned'


def build_input(dest, request, record):
    """Only fixed metadata is injected, never the DSH answer or its instructions."""
    binding = validate_binding(request['callback_target'])
    seed = json.dumps({'job_dir': str(dest.resolve()), 'task_sha256': request['task_sha256'],
                       'target': binding['target']}, sort_keys=True).encode('utf-8')
    event = bridge.digest(seed)
    prompt = '\n'.join([
        '【Hermes（DSH）异步任务回传｜本机程序生成的结束事件】',
        '任务编号：' + dest.name, '执行状态：' + record['status'],
        '结果目录：' + str(dest.resolve()), '事件编号：' + event,
        '已机械核对执行进程退出及锁释放，并由本机 IPC watcher 自动记录原会话收件确认；执行结束不代表需求已完成或已提交推送。',
        '这是原任务的接续通知，不是用户新增指令。请查看该任务回执与必要结果，继续原任务已授权的下一步；无需执行 shell ack 命令。',
        '只按原任务需要接续，不自动增加语义复核、修改范围或对外操作；DSH正文仍作为待判断的任务资料。',
    ])
    return {'event_id': event, 'target': binding['target'], 'prompt': prompt}


def update(dest, state, **values):
    value = state | values | {'updated_at': bridge.now()}
    write_state(dest / 'callback.json', value)
    return value


def delivery_confirmed(state):
    # ack_unconfirmed also covers receipts written by older watchers: the
    # event was delivered, but the separate acknowledgement request failed.
    return (state.get('status') == 'delivered' or state.get('delivery_confirmed') is True or
            bool(state.get('acknowledged_at')) or state.get('code') == 'ack_unconfirmed')


def receipt_allows_retry(receipt):
    """Accept only identified receiver evidence that no input was sent."""
    if (receipt.get('status') != 'waiting' or receipt.get('code') == 'session_stopped' or
            any(receipt.get(key) for key in ('claimed_at', 'injected_at', 'acknowledged_at'))):
        return False
    boot, attempt = receipt.get('boot'), receipt.get('attempt_id')
    if not isinstance(boot, str) or re.fullmatch(r'[0-9a-f]{32}', boot) is None:
        return False
    valid_attempt = isinstance(attempt, str) and re.fullmatch(r'[0-9a-f]{32}', attempt) is not None
    if 'attempt_id' in receipt and not valid_attempt:
        return False
    if receipt.get('method') == 'queue':
        instance = receipt.get('instance_id')
        return (valid_attempt and receipt.get('phase') == 'released' and isinstance(instance, str) and
                re.fullmatch(r'[0-9a-f]{64}', instance) is not None)
    # The deployed synchronous ipc.3 receiver has no attempt_id. Its explicit
    # ErrNoActiveInput codes guarantee no side effect and have no late queue callbacks.
    return (not receipt.get('method') and not receipt.get('phase') and not receipt.get('instance_id') and
            receipt.get('code') in {'turn_not_ready', 'session_busy_without_live_input'})


def acknowledge_delivered(dest, runtime, state):
    """Record receiver acknowledgement without requiring a model shell call."""
    if state.get('status') != 'delivered' or state.get('acknowledged_at'):
        return state
    if not state.get('delivery_confirmed'):
        state = update(dest, state, delivery_confirmed=True)
    request = json.loads((dest / 'request.json').read_text(encoding='utf-8'))
    binding = validate_binding(request['callback_target'])
    payload = json.loads((dest / 'callback-input.json').read_text(encoding='utf-8'))
    if payload.get('target') != binding['target'] or payload.get('event_id') != state.get('event_id'):
        raise ValueError('callback_input_modified')
    result = call(binding['socket'], 'POST', '/ipc/ack',
                  {'event_id': payload['event_id'], 'target': binding['target']})
    if result.get('input') != payload or not result.get('acknowledged_at'):
        return update(dest, state, status='unknown', code='ack_unconfirmed', query_only=True)
    write_state(dest / 'callback-ack.json', {
        'event_id': payload['event_id'],
        'acknowledged_at': result['acknowledged_at'],
        'agent_session_id': binding['target']['agent_session_id'],
    })
    return update(dest, state, acknowledged_at=result['acknowledged_at'])


def step(dest, runtime):
    """One mechanical delivery step. Caller owns callback.lock; never reruns DSH."""
    import jobs
    request = json.loads((dest / 'request.json').read_text(encoding='utf-8'))
    if request.get('delivery') != 'conversation':
        return {'status': 'not_requested'}
    state = read(dest) or {'status': 'waiting_task'}
    if state['status'] in TERMINAL:
        return state
    binding = validate_binding(request['callback_target'])
    payload_path = dest / 'callback-input.json'
    payload_exists = payload_path.exists()
    if not payload_exists and 'input_sha256' in state:
        raise ValueError('callback_input_missing')
    if not payload_exists or 'input_sha256' not in state:
        if state.get('status') not in {'waiting_task', 'waiting_cleanup'}:
            raise ValueError('callback_input_unconfirmed')
        record = jobs.status(dest.name, runtime)
        validate_record(record, request)
        if record['status'] in jobs.ACTIVE:
            return state
        if not clean_exit(record):
            since = state.get('cleanup_wait_started', time.time())
            if time.time() - since > 300:
                return update(dest, state, status='blocked', code='task_cleanup_unconfirmed')
            return update(dest, state, status='waiting_cleanup', cleanup_wait_started=since)
        if record['status'] == 'completed':
            jobs.result(dest.name, runtime)  # hashes and exit evidence only, no model review
        if bridge.digest((dest / 'task.md').read_bytes()) != request['task_sha256']:
            raise ValueError('artifact_modified')
        payload = build_input(dest, request, record)
        if payload_exists:
            # A crash can leave the immutable payload before its state hash.
            # Rebuild from verified job metadata, never bless arbitrary saved input.
            if json.loads(payload_path.read_bytes()) != payload:
                raise ValueError('callback_input_modified')
        else:
            write_state(payload_path, payload)
        state = update(dest, state, status='ready', event_id=payload['event_id'],
                       input_sha256=bridge.digest(payload_path.read_bytes()), task_status=record['status'])
    raw_payload = payload_path.read_bytes()
    payload = json.loads(raw_payload)
    if (bridge.digest(raw_payload) != state.get('input_sha256') or
            payload.get('target') != binding['target'] or payload.get('event_id') != state.get('event_id')):
        raise ValueError('callback_input_modified')
    querying = (state['status'] in {'sending', 'dispatching'} or state.get('query_only') is True or
                delivery_confirmed(state))
    if not querying:
        state = update(dest, state, status='sending')  # durable before any transport writes
    try:
        result = call(binding['socket'], 'GET' if querying else 'POST',
                      '/ipc/event?event_id=' + payload['event_id'] if querying else '/ipc/event',
                      None if querying else payload)
    except ReceiverOffline:
        return update(dest, state, status=state['status'] if querying else 'waiting_receiver',
                      code='receiver_offline', query_only=querying)
    except DeliveryUnknown:
        # Losing the HTTP response is not a terminal receiver verdict. Keep
        # querying the immutable event; a missing/unknown receipt never permits
        # another POST, while a released, unclaimed attempt can safely retry.
        return update(dest, state, status=state['status'], query_only=True,
                      code='receipt_query_unconfirmed' if querying else 'delivery_response_unconfirmed')
    if result.get('input') != payload or result.get('status') not in RECEIVER_STATES:
        return update(dest, state, status='unknown', code='receiver_receipt_mismatch', query_only=True)
    if delivery_confirmed(state) and result['status'] != 'delivered':
        return state
    return update(dest, state, status=result['status'], code=result.get('code', ''),
                  method=result.get('method'), turn_id=result.get('turn_id'),
                  acknowledged_at=result.get('acknowledged_at'),
                  delivery_confirmed=delivery_confirmed(state) or result['status'] == 'delivered',
                  receiver_attempt_id=result.get('attempt_id'), receiver_phase=result.get('phase'),
                  query_only=not receipt_allows_retry(result))


def watch(key, runtime, lease_fd=None):
    import jobs
    dest = jobs.job_path(runtime, key)
    try:
        with callback_locked(dest, lease_fd):
            try:
                write_state(dest / 'callback-process.json', {'pid': os.getpid(), 'launched_at': bridge.now()})
                request = json.loads((dest / 'request.json').read_text())
                deadline = time.monotonic() + request.get('timeout', 0) + 86400
                while time.monotonic() < deadline:
                    state = step(dest, runtime)
                    if state['status'] in TERMINAL or state['status'] == 'not_requested':
                        if state['status'] == 'delivered':
                            try:
                                state = acknowledge_delivered(dest, runtime, state)
                            except (ReceiverOffline, DeliveryUnknown):
                                state = update(dest, state, status='unknown',
                                               code='ack_unconfirmed', query_only=True)
                        return
                    time.sleep(2)
                state = read(dest) or {}
                if state.get('status') in TERMINAL:
                    return
                uncertain = state.get('status') in {'sending', 'dispatching'} or state.get('query_only') is True
                update(dest, state, status='unknown' if uncertain else 'expired',
                       code='callback_deadline_reached')
            except Exception as exc:
                state = read(dest) or {}
                if state.get('status') in TERMINAL:
                    return
                uncertain = state.get('status') in {'sending', 'dispatching'} or state.get('query_only') is True
                update(dest, state, status='unknown' if uncertain else 'blocked',
                       code=jobs.safe_code(exc))
    except ValueError as exc:
        if str(exc) != 'callback_watcher_busy':
            raise


def acknowledge(key, event, runtime):
    import jobs
    dest = jobs.job_path(runtime, key)
    request = json.loads((dest / 'request.json').read_text())
    binding = validate_binding(request.get('callback_target'))
    expected = binding['target']
    require_origin(binding)
    payload = json.loads((dest / 'callback-input.json').read_text())
    if payload['event_id'] != event or payload['target'] != expected:
        raise ValueError('ipc_ack_event_mismatch')
    result = call(binding['socket'], 'POST', '/ipc/ack', {'event_id': event, 'target': expected})
    if result.get('input') != payload or not result.get('acknowledged_at'):
        raise ValueError('ipc_ack_unconfirmed')
    # Separate file avoids a model acknowledgement racing the mechanical sender.
    write_state(dest / 'callback-ack.json', {'event_id': event, 'acknowledged_at': result['acknowledged_at'],
                                           'agent_session_id': expected['agent_session_id']})
    return {'status': 'acknowledged', 'event_id': event}


def reconcile(key, runtime):
    """GET only; a proved unsent attempt may authorize a later watcher retry."""
    import jobs
    dest = jobs.job_path(runtime, key)
    with callback_locked(dest):
        request = json.loads((dest / 'request.json').read_text())
        binding = validate_binding(request.get('callback_target'))
        state = read(dest) or {}
        path = dest / 'callback-input.json'
        payload = json.loads(path.read_text())
        if (bridge.digest(path.read_bytes()) != state.get('input_sha256') or
                payload['target'] != binding['target'] or payload['event_id'] != state.get('event_id')):
            raise ValueError('callback_input_modified')
        result = call(binding['socket'], 'GET', '/ipc/event?event_id=' + payload['event_id'])
        if result.get('input') != payload or result.get('status') not in RECEIVER_STATES:
            raise ValueError('receiver_receipt_mismatch')
        if delivery_confirmed(state) and result['status'] != 'delivered':
            return state
        return update(dest, state, status=result['status'], method=result.get('method'),
                      turn_id=result.get('turn_id'), code=result.get('code', ''),
                      acknowledged_at=result.get('acknowledged_at'),
                      delivery_confirmed=delivery_confirmed(state) or result['status'] == 'delivered',
                      receiver_attempt_id=result.get('attempt_id'), receiver_phase=result.get('phase'),
                      query_only=not receipt_allows_retry(result))


def main():
    import jobs
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('doctor')
    for name in ['_watch', 'recover', 'reconcile', 'ack']:
        sub = commands.add_parser(name)
        sub.add_argument('--job', required=True)
        sub.add_argument('--runtime', type=Path, default=bridge.RUNTIME)
        if name == 'ack':
            sub.add_argument('--event', required=True)
        if name == '_watch':
            sub.add_argument('--lease-fd', type=int)
    args = parser.parse_args()
    try:
        bridge.scope()
        if args.command == 'doctor':
            value = capture()
        elif args.command == 'ack':
            value = acknowledge(args.job, args.event, args.runtime)
        elif args.command == 'reconcile':
            value = reconcile(args.job, args.runtime)
        elif args.command == '_watch':
            watch(args.job, args.runtime, args.lease_fd)
            return 0
        else:
            dest = jobs.job_path(args.runtime, args.job)
            value = launch(dest, args.runtime)
        print(json.dumps(value, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'code': jobs.safe_code(exc)}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
