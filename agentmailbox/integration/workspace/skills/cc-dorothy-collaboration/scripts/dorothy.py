#!/usr/bin/env python3
"""Workspace-local, six-slot Antigravity worker controlled by Atlas."""
import argparse
from contextlib import contextmanager, ExitStack
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import selectors
import shutil
import signal
import secrets
import shlex
import stat
import subprocess
import sys
import tempfile
import time
import uuid

import native

WORKSPACE = Path(__file__).resolve().parents[3]
SKILL = Path(__file__).resolve().parents[1]
HERMES_SKILL = WORKSPACE / 'skills/cc-dsh-collaboration'
RUNTIME = WORKSPACE / '.local/dorothy-collaboration'
HERMES_RUNTIME = WORKSPACE / '.local/dsh-collaboration'
sys.path.insert(0, str(WORKSPACE / 'skills/cc-dsh-collaboration/scripts'))
import instances
import ipc

def resolve_shared_script(name):
    candidates = [
        WORKSPACE / 'skills/cc-agent-ipc/scripts' / name,
        Path(__file__).resolve().parents[2] / 'cc-agent-ipc/scripts' / name,
        WORKSPACE.parents[1] / 'skill/scripts' / name,
        Path(__file__).resolve().parents[5] / 'skill/scripts' / name,
        WORKSPACE.parent / 'skill/scripts' / name,
        Path(__file__).resolve().parents[4] / 'skill/scripts' / name,
    ]
    for c in candidates:
        if c.is_file() and not c.is_symlink():
            return c.resolve()
    return None


def resolve_agy_hook_path():
    return resolve_shared_script('agy_hook.py')


def resolve_mcp_server_path():
    return resolve_shared_script('mcp_server.py')


_hr_script = resolve_shared_script('host_runtime.py')
if _hr_script and str(_hr_script.parent) not in sys.path:
    sys.path.insert(0, str(_hr_script.parent))

try:
    import host_runtime
except ImportError:
    host_runtime = None

ACTIVE = {'queued', 'starting', 'running', 'saving'}
MAX_TASK = 60 * 1024
SLOTS = 6
BROAD_CWD = ('workdir', '.worktrees', '.task-clones', '.local/dorothy-work')
ERROR_MESSAGES = {
    'cwd_too_broad': '执行目录为公共容器目录（如 workdir/.worktrees/.task-clones/.local/dorothy-work），不能作为独占执行目录。',
}


def broad_cwd(target, workspace=WORKSPACE):
    root = Path(workspace).resolve()
    blocked = {root / name for name in BROAD_CWD}
    return Path(target).resolve() in {path.resolve() for path in blocked}


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def error_code(exc):
    """Keep known stable codes; never leak exception text into a record."""
    code = getattr(exc, 'code', None)
    if isinstance(code, str) and native.ID_PATTERN.fullmatch(code):
        return code
    text = str(exc)
    if isinstance(exc, ValueError) and native.ID_PATTERN.fullmatch(text):
        return text
    return type(exc).__name__


def write_json(path, value):
    ipc.write_state(Path(path), value)


def read_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file():
        raise ValueError('record_missing_or_invalid')
    value = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('record_invalid')
    return value


def runtime_path(value):
    path = Path(value)
    if path.is_symlink():
        raise ValueError('runtime_symlink')
    path = path.resolve()
    if not path.is_relative_to(WORKSPACE) or path == WORKSPACE:
        raise ValueError('runtime_outside_workspace')
    return path


def job_path(runtime, key):
    if not isinstance(key, str) or not native.ID_PATTERN.fullmatch(key):
        raise ValueError('invalid_job_id')
    parent = runtime / 'jobs'
    dest = parent / key
    if parent.is_symlink() or dest.is_symlink():
        raise ValueError('job_path_invalid')
    return dest


def configuration(runtime):
    config = read_json(runtime / 'config.json')
    if set(config) != {'executable', 'auth_file'}:
        raise ValueError('configuration_invalid')
    for name in ('executable', 'auth_file'):
        path = Path(config[name])
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise ValueError(name + '_unavailable')
    if not os.access(config['executable'], os.X_OK):
        raise ValueError('executable_unavailable')
    return config


def file_digest(path):
    with open(path, 'rb') as handle:
        return hashlib.file_digest(handle, 'sha256').hexdigest()


def freeze_components():
    """Resolve and hash every shared component once; followups reuse this record.

    Returns None when any component is missing, which makes an explicitly
    connected task fail before the model starts instead of silently dropping
    the mailbox.
    """
    resolved = {
        'agy_hook': resolve_agy_hook_path(),
        'mcp_server': resolve_mcp_server_path(),
        'host_runtime': resolve_shared_script('host_runtime.py'),
        'client': resolve_shared_script('client.py'),
    }
    if not all(resolved.values()):
        return None
    return {name: {'path': str(path), 'sha256': file_digest(path)}
            for name, path in resolved.items()}


def validate_temp_boundary(runtime):
    for temp in native.system_temp_roots():
        for control in (runtime, SKILL, HERMES_SKILL, HERMES_RUNTIME):
            if temp.is_relative_to(control) or control.is_relative_to(temp):
                raise ValueError('temp_overlaps_host_control')


def tree_digest(root):
    result = hashlib.sha256()
    for path in sorted(Path(root).rglob('*')):
        relative = str(path.relative_to(root))
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            kind, value = 'directory', ''
        elif stat.S_ISLNK(mode):
            kind, value = 'link', os.readlink(path)
        elif stat.S_ISREG(mode):
            kind, value = 'executable' if mode & stat.S_IXUSR else 'file', file_digest(path)
        else:
            raise ValueError('native_store_nonregular_entry')
        result.update(json.dumps([relative, kind, value], ensure_ascii=False).encode('utf-8'))
    return result.hexdigest()


def _open_lease(path):
    """Open a read-only lease file; a worker must inherit a read-only descriptor."""
    fd = os.open(path, os.O_RDONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    return os.fdopen(fd, 'r')


@contextmanager
def locked(path, *, blocking=False):
    """Read-only leases can be inherited without exposing writable host records."""
    handle = _open_lease(path)
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        except BlockingIOError:
            raise ValueError('dorothy_busy') from None
        yield handle
    finally:
        # No LOCK_UN: a native process may still own this open-file lease.
        handle.close()


def slot_lock_path(runtime, index):
    """Slot 0 stays run.lock so an old worker's lease still counts as one slot."""
    if index == 0:
        return runtime / 'run.lock'
    return runtime / ('run-slot-' + str(index) + '.lock')


def request_slot(request):
    """Only a record written before slots existed lacks the field; it is slot 0."""
    if 'slot' not in request:
        return 0
    slot = request['slot']
    if type(slot) is not int or not 0 <= slot < SLOTS:
        raise ValueError('slot_invalid')
    return slot


def acquire_slot(runtime):
    """Hold the first free run slot, or report dorothy_busy when all six are leased."""
    for index in range(SLOTS):
        handle = _open_lease(slot_lock_path(runtime, index))
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.close()
            continue
        except OSError:
            handle.close()
            raise
        return index, handle
    raise ValueError('dorothy_busy')


def process_info(pid):
    if type(pid) is not int or pid <= 0:
        return None
    try:
        result = subprocess.run(['/bin/ps', '-p', str(pid), '-o', 'pid=,pgid=,lstart='],
                                capture_output=True, text=True, timeout=5)
    except PermissionError:
        try:
            pgid = os.getpgid(pid)
            return {'pid': pid, 'pgid': pgid, 'started': 'sandboxed'}
        except (ProcessLookupError, PermissionError, OSError):
            return None
    if result.returncode not in (0, 1):
        raise ValueError('process_lookup_failed')
    parts = result.stdout.strip().split(None, 2)
    if not parts:
        return None
    if len(parts) != 3:
        raise ValueError('process_lookup_invalid')
    return {'pid': int(parts[0]), 'pgid': int(parts[1]), 'started': parts[2]}


def process_matches(identity):
    return bool(identity and process_info(identity.get('pid')) == identity)


def group_members(pgid):
    if type(pgid) is not int or pgid <= 0:
        return []
    try:
        result = subprocess.run(['/bin/ps', '-axo', 'pid=,pgid='], capture_output=True,
                                text=True, timeout=5, check=True)
        return [int(parts[0]) for line in result.stdout.splitlines()
                if len(parts := line.split()) == 2 and int(parts[1]) == pgid]
    except PermissionError:
        try:
            res = subprocess.run(['/usr/bin/pgrep', '-g', str(pgid)], capture_output=True, text=True)
            pids = [int(p) for p in res.stdout.strip().split() if p.isdigit()]
            if not pids:
                try:
                    os.kill(pgid, 0)
                    return [pgid]
                except (ProcessLookupError, PermissionError, OSError):
                    return []
            return pids
        except Exception:
            try:
                os.kill(pgid, 0)
                return [pgid]
            except (ProcessLookupError, PermissionError, OSError):
                return []


def cleanup_process(proc):
    """Only signal this invocation's live Popen group, never a stored old PID."""
    proc.poll()
    for sig, seconds in ((signal.SIGTERM, 3), (signal.SIGKILL, 3)):
        if not group_members(proc.pid):
            break
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            break
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            proc.poll()
            if not group_members(proc.pid):
                break
            time.sleep(0.05)
    try:
        proc.wait(timeout=1)
    except subprocess.TimeoutExpired:
        return False
    return not group_members(proc.pid)


def state_update(dest, state, **values):
    state.update(values, updated_at=now())
    write_json(dest / 'state.json', state)


def task_cwd(runtime, key, requested, parent_request=None):
    validate_temp_boundary(runtime)
    if parent_request:
        inherited = Path(parent_request['cwd'])
        if requested is not None and Path(requested).resolve() != inherited:
            raise ValueError('cwd_mismatch')
        path = inherited
    elif requested is None:
        path = WORKSPACE / '.local/dorothy-work' / key
        if path.is_symlink():
            raise ValueError('cwd_symlink')
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
    else:
        path = Path(requested)
    path = path.resolve(strict=True)
    if not path.is_dir() or not path.is_relative_to(WORKSPACE) or path == WORKSPACE:
        raise ValueError('cwd_requires_workspace_subdirectory')
    if broad_cwd(path, WORKSPACE):
        raise ValueError('cwd_too_broad')
    for control in (runtime, SKILL, HERMES_SKILL, HERMES_RUNTIME):
        if path.is_relative_to(control) or control.is_relative_to(path):
            raise ValueError('cwd_overlaps_host_control')
    return path


def checked_request(dest):
    request = read_json(dest / 'request.json')
    if file_digest(dest / 'task.md') != request.get('task_sha256'):
        raise ValueError('task_changed')
    return request


def lease_held(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        return False
    finally:
        os.close(fd)


def status(key, runtime=RUNTIME):
    runtime = runtime_path(runtime)
    dest = job_path(runtime, key)
    request = checked_request(dest)
    state = read_json(dest / 'state.json')
    launch = read_json(dest / 'launch.json') if (dest / 'launch.json').exists() else {}
    worker_identity = state.get('worker') or launch.get('worker')
    worker_alive = process_matches(worker_identity)
    pgid = state.get('native_pid')
    members = group_members(pgid)
    child_unknown = bool(state.get('native_launch_started') and pgid is None)
    effective = state['status']
    startup_unknown = (effective == 'queued' and not worker_identity and not launch and
                       lease_held(slot_lock_path(runtime, request_slot(request))))
    if effective in ACTIVE and not worker_alive and not startup_unknown:
        effective = 'orphaned' if members or child_unknown else 'interrupted'
    callback_launch = read_json(dest / 'callback-launch.json') if (dest / 'callback-launch.json').exists() else {}
    return {'job_id': key, 'status': effective, 'recorded_status': state['status'],
            'cwd': request['cwd'], 'parent_job': request.get('parent_job'),
            'root_job': request['root_job'], 'conversation_id': state.get('conversation_id'),
            'returncode': state.get('returncode'), 'error': state.get('error'),
            'result_dir': str(dest), 'created_at': request['created_at'],
            'cleanup': {'worker': 'running' if worker_alive else 'unknown' if startup_unknown else 'absent',
                        'native_group': 'running' if members else 'unknown' if child_unknown else 'absent',
                        'native_archive': bool(state.get('native_sha256')),
                        'callback_worker': 'running' if process_matches(callback_launch.get('worker')) else 'absent'},
            'callback': read_json(dest / 'callback.json') if (dest / 'callback.json').exists() else None,
            **{field: state[field] for field in ('native_status', 'native_outcome', 'protocol',
                                                 'partial_bytes', 'partial_sha256') if field in state}}


def result(key, runtime=RUNTIME):
    runtime = runtime_path(runtime)
    value = status(key, runtime)
    if value['status'] != 'completed' or value['cleanup']['worker'] != 'absent' or value['cleanup']['native_group'] != 'absent':
        raise ValueError('result_not_complete')
    dest = job_path(runtime, key)
    state = read_json(dest / 'state.json')
    raw = (dest / 'report.md').read_bytes()
    if len(raw) != state['report_bytes'] or digest(raw) != state['report_sha256']:
        raise ValueError('report_changed')
    return value | {'report_path': str(dest / 'report.md'), 'report_bytes': len(raw),
                    'report_sha256': digest(raw), 'report': raw.decode('utf-8')}


def export_visible(key, runtime=RUNTIME):
    """Export a verified visible reply without promoting a failed invocation."""
    runtime = runtime_path(runtime)
    value = status(key, runtime)
    if (value['status'] in ACTIVE or value['cleanup']['worker'] != 'absent' or
            value['cleanup']['native_group'] != 'absent'):
        raise ValueError('native_execution_not_closed')
    dest = job_path(runtime, key)
    request = checked_request(dest)
    state = read_json(dest / 'state.json')
    archive = dest / 'native'
    if not state.get('native_sha256') or tree_digest(archive) != state['native_sha256']:
        raise ValueError('native_archive_changed')
    sid = state.get('conversation_id')
    if not isinstance(sid, str) or not native.ID_PATTERN.fullmatch(sid):
        raise ValueError('invalid_conversation_id')
    path = archive / 'antigravity-cli/brain' / sid / '.system_generated/logs/transcript_full.jsonl'
    if not path.resolve().is_relative_to(archive.resolve()):
        raise ValueError('native_transcript_outside_archive')
    protocol = state.get('protocol', {})
    if (protocol.get('input_accepted') is not True or protocol.get('reply_done') is not True or
            protocol.get('last_step_type') != 'agent_response' or
            type(protocol.get('reply_index')) is not int or
            protocol.get('reply_index') != protocol.get('last_step_index')):
        raise ValueError('visible_reply_unverified')
    messages, errors = native.visible_turn(path, (dest / 'task.md').read_text(encoding='utf-8'),
                                           protocol.get('reply_index'))
    # CLI DONE events add one newline per visible response. Check the captured
    # stream against the independently archived, hash-verified native source.
    if (dest / 'partial.md').is_symlink():
        raise ValueError('visible_text_mismatch')
    raw = (dest / 'partial.md').read_bytes()
    expected = ''.join(message['text'] + '\n' for message in messages if message['role'] == 'assistant').encode('utf-8')
    if (raw != expected or (state.get('partial_sha256') and digest(raw) != state['partial_sha256']) or
            ('partial_bytes' in state and state['partial_bytes'] != len(raw))):
        raise ValueError('visible_text_mismatch')
    reply = (messages[-1]['text'] + '\n').encode('utf-8')
    return value | {'artifact_kind': 'visible_reply', 'verification': 'native_archive',
                    'native_success': state.get('native_status') == 'SUCCESS',
                    'native_archive_sha256': state['native_sha256'],
                    'visible_messages': messages, 'native_error_steps': errors,
                    'visible_reply': reply.decode('utf-8'), 'visible_reply_bytes': len(reply),
                    'visible_reply_sha256': digest(reply), 'task_sha256': request['task_sha256']}


def verify_parent_binding(binding_file, service_socket):
    """Trust the current chat binding only if the service confirms it.

    The binding must point at our target service and the service's ``me`` must
    still report the same mailbox, a coordinator, and a scope. A path field
    alone is not enough to derive scope/parent for a new task.
    """
    data = json.loads(Path(binding_file).read_text(encoding='utf-8'))
    socket, token, mailbox_id = data.get('socket'), data.get('token'), data.get('mailbox_id')
    if not socket or not os.path.isabs(socket) or not token or not mailbox_id:
        raise ValueError('agent_mailbox_binding_invalid')
    if Path(socket).resolve() != Path(service_socket).resolve():
        raise ValueError('agent_mailbox_binding_service_mismatch')
    me = host_runtime.Client(socket, token).me()
    if (me.get('mailbox_id') != mailbox_id or me.get('coordinator') is not True
            or not me.get('scope') or (data.get('scope') and data['scope'] != me['scope'])):
        raise ValueError('agent_mailbox_binding_invalid')
    return me['scope'], mailbox_id


def reconcile_leftover_generation(parent_dir, parent_request):
    """End only this task's own leftover generation before a new turn starts.

    The parent already exited and released its leases. We ask the service which
    generation is active: if it is the one we recorded, we end it; if another
    executor is active we refuse instead of preempting it; if it already ended
    we just record that fact.
    """
    record_path = Path(parent_dir) / 'generation.json'
    if not record_path.is_file():
        return
    record = read_json(record_path)
    if record.get('phase') in (None, 'ended'):
        return
    socket = parent_request.get('service_socket')
    token = parent_request.get('mailbox_token')
    generation = record.get('generation')
    if host_runtime is None or not socket or not token or not generation:
        return
    me = (host_runtime.request_unix(socket, 'GET', '/v1/me', token=token) or {}).get('mailbox') or {}
    if me.get('mailbox_id') != record.get('mailbox_id'):
        raise ValueError('generation_recovery_identity_mismatch')
    if me.get('active') and me.get('generation') != generation:
        raise ValueError('generation_foreign_active')
    if me.get('active'):
        host_runtime.deactivate_mailbox(socket, token, generation)
    write_json(record_path, record | {'phase': 'ended', 'updated_at': now()})


def submit(key, task_file, *, cwd=None, parent=None, timeout=None, delivery=None, runtime=RUNTIME, team=None):
    runtime = runtime_path(runtime)
    runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
    (runtime / 'jobs').mkdir(exist_ok=True, mode=0o700)
    dest = job_path(runtime, key)
    task = Path(task_file).read_bytes()
    if not task.strip() or len(task) > MAX_TASK:
        raise ValueError('task_size_invalid')
    task.decode('utf-8')
    with locked(runtime / 'submit.lock', blocking=True):
        if dest.exists():
            request = checked_request(dest)
            if (digest(task) != request['task_sha256'] or parent != request.get('parent_job') or
                    (cwd is not None and str(Path(cwd).resolve()) != request['cwd']) or
                    (timeout is not None and timeout != request['timeout']) or
                    (delivery is not None and delivery != request['delivery']) or
                    (team is not None and team != request.get('team'))):
                raise ValueError('job_request_conflict')
            return status(key, runtime) | {'reused': True}
        parent_request = parent_state = None
        if parent:
            previous = status(parent, runtime)
            if previous['status'] in ACTIVE | {'orphaned'} or previous['cleanup']['worker'] != 'absent' or previous['cleanup']['native_group'] != 'absent':
                raise ValueError('parent_still_running')
            parent_dir = job_path(runtime, parent)
            parent_request = checked_request(parent_dir)
            parent_state = read_json(parent_dir / 'state.json')
            if not parent_state.get('native_sha256') or not parent_state.get('conversation_id'):
                raise ValueError('parent_not_resumable')
            if tree_digest(parent_dir / 'native') != parent_state['native_sha256']:
                raise ValueError('parent_archive_changed')
            root_dir = job_path(runtime, parent_request['root_job'])
            if read_json(root_dir / 'session.json')['latest_job'] != parent:
                raise ValueError('parent_is_not_latest_turn')

        mailbox_info = None
        components = None

        if parent:
            if 'mailbox_id' in parent_request:
                if team is not None and team != parent_request.get('team', ''):
                    raise ValueError('job_request_conflict')
                team = parent_request.get('team', '')
                components = parent_request.get('components')
                if not host_runtime or not components:
                    raise ValueError('agent_mailbox_components_missing')
                reconcile_leftover_generation(parent_dir, parent_request)
                mailbox_info = {
                    'team': team,
                    'mailbox_id': parent_request['mailbox_id'],
                    'root_mailbox_id': parent_request.get('root_mailbox_id') or parent_request['mailbox_id'],
                    'scope': parent_request.get('scope'),
                    'parent_mailbox_id': parent_request.get('parent_mailbox_id'),
                    'service_socket': parent_request.get('service_socket'),
                    'admin_token_path': parent_request.get('admin_token_path'),
                    'mailbox_token': parent_request.get('mailbox_token'),
                    'registration_file': parent_request.get('registration_file'),
                    'components': components,
                }
            else:
                if team is not None:
                    raise ValueError('job_request_conflict')
        else:
            components = freeze_components()
            has_components = bool(host_runtime and components)
            service_sock = None
            admin_tok = None
            service_ok = False
            atlas_binding_file = None
            if has_components:
                service_sock, admin_tok = host_runtime.resolve_mailbox_service()
                service_ok = bool(service_sock and admin_tok and host_runtime.check_mailbox_service(service_sock))
                try:
                    atlas_binding_file = host_runtime.binding_for_current()
                except Exception:
                    atlas_binding_file = None

            if team is not None:
                if not has_components:
                    raise ValueError('agent_mailbox_components_missing')
                if not (service_ok and atlas_binding_file):
                    raise ValueError('agent_mailbox_service_unavailable')
            elif has_components and service_ok and atlas_binding_file:
                team = ''

            if team is not None and service_ok and atlas_binding_file:
                scope, parent_id = verify_parent_binding(atlas_binding_file, service_sock)

                reg_key = f"dorothy:{key}"
                reg_dir = runtime / 'registrations'
                mailbox_obj, mtoken = host_runtime.register_task_mailbox(
                    service_sock,
                    admin_tok,
                    reg_key,
                    scope,
                    team=team,
                    label=f"dorothy:{key}",
                    kind='dorothy',
                    coordinator=False,
                    parent_id=parent_id or '',
                    task_dir=reg_dir,
                )
                safe_name = hashlib.sha256(reg_key.encode('utf-8')).hexdigest()[:16]
                reg_file = reg_dir / f"reg-{safe_name}.json"
                mailbox_info = {
                    'team': team,
                    'mailbox_id': mailbox_obj['mailbox_id'],
                    'root_mailbox_id': mailbox_obj['mailbox_id'],
                    'scope': scope,
                    'parent_mailbox_id': parent_id,
                    'service_socket': str(service_sock),
                    'admin_token_path': str(admin_tok),
                    'mailbox_token': mtoken,
                    'registration_file': str(reg_file),
                    'components': components,
                }

        timeout = timeout if timeout is not None else 1800
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or not 1 <= timeout <= 86400:
            raise ValueError('timeout_invalid')
        delivery = delivery or (parent_request or {}).get('delivery', 'manual')
        if delivery not in ('manual', 'conversation'):
            raise ValueError('delivery_invalid')
        directory = task_cwd(runtime, key, cwd, parent_request)
        config = configuration(runtime)
        with ExitStack() as leases:
            slot, single = acquire_slot(runtime)
            leases.callback(single.close)
            directory_lease = leases.enter_context(instances.workspace_lease(HERMES_RUNTIME, directory))
            for record in (runtime / 'jobs').iterdir():
                if not record.name.startswith('.') and (record / 'request.json').is_file() and status(record.name, runtime)['status'] == 'orphaned':
                    raise ValueError('prior_orphan_requires_reconciliation')
            binding = None
            if delivery == 'conversation':
                binding = ((parent_request or {}).get('callback_target') or ipc.capture())
                ipc.validate_binding(binding)
            request = {'version': 1, 'job_id': key, 'slot': slot,
                       'root_job': (parent_request or {}).get('root_job', key),
                       'parent_job': parent, 'cwd': str(directory), 'task_sha256': digest(task),
                       'timeout': timeout, 'delivery': delivery, 'callback_target': binding,
                       'model': native.MODEL, 'effort': native.EFFORT, 'permission_mode': 'workspace-write',
                       'config': config, 'executable_sha256': file_digest(config['executable']),
                       'expected_conversation_id': (parent_state or {}).get('conversation_id'),
                       'native_dir': (parent_request or {}).get('native_dir') or
                           str(Path(tempfile.gettempdir()).resolve() / ('dorothy-' + uuid.uuid4().hex)),
                       'created_at': now()}
            if mailbox_info:
                request.update(mailbox_info)
            stage = Path(tempfile.mkdtemp(prefix='.partial-', dir=runtime / 'jobs'))
            try:
                native.private_file(stage / 'task.md', task)
                write_json(stage / 'request.json', request)
                write_json(stage / 'state.json', {'status': 'queued', 'updated_at': now()})
                if parent is None:
                    write_json(stage / 'session.json', {'latest_job': key})
                stage.rename(dest)
            finally:
                if stage.exists():
                    shutil.rmtree(stage)
            argv = [sys.executable, '-B', str(Path(__file__).resolve()), '_worker', '--job', key,
                    '--runtime', str(runtime), '--lease-fd', str(single.fileno()),
                    '--workspace-fd', str(directory_lease.fileno())]
            launch_recorded = False
            try:
                proc = subprocess.Popen(argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL, start_new_session=True,
                                        pass_fds=(single.fileno(), directory_lease.fileno()))
            except OSError:
                write_json(dest / 'state.json', {'status': 'launch_failed', 'error': 'worker_launch_failed', 'updated_at': now()})
            else:
                # The worker owns state.json once Popen succeeds. A separate
                # launch-ledger failure must never erase a live worker's state.
                try:
                    write_json(dest / 'launch.json', {'worker': process_info(proc.pid), 'pid': proc.pid})
                    launch_recorded = True
                except OSError:
                    pass
                if delivery == 'conversation':
                    launch_watcher(dest, runtime)
            return status(key, runtime) | {'reused': False, 'launch_recorded': launch_recorded}


def cancel(key, runtime=RUNTIME):
    runtime = runtime_path(runtime)
    value = status(key, runtime)
    if value['status'] not in ACTIVE | {'orphaned'}:
        return value | {'cancel_requested': False}
    dest = job_path(runtime, key)
    request = checked_request(dest)
    write_json(dest / 'cancel.json', {'job_id': key, 'task_sha256': request['task_sha256'], 'requested_at': now()})
    return value | {'cancel_requested': True}


def cancellation_requested(dest, request):
    path = dest / 'cancel.json'
    if not path.exists():
        return False
    value = read_json(path)
    if value.get('job_id') != request['job_id'] or value.get('task_sha256') != request['task_sha256']:
        raise ValueError('cancel_request_invalid')
    return True


def child_environment():
    allowed = {'HOME', 'PATH', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'LC_CTYPE', 'TZ', 'TMPDIR',
               'SSL_CERT_FILE', 'SSL_CERT_DIR', 'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY',
               'http_proxy', 'https_proxy', 'no_proxy'}
    # Never transfer Atlas origin handles or tool credentials as model environment.
    return {key: value for key, value in os.environ.items() if key in allowed}


def inherited_lease(fd, path):
    actual, expected = os.fstat(fd), Path(path).stat()
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        raise ValueError('worker_lease_mismatch')
    if fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY:
        raise ValueError('worker_lease_not_readonly')


def execute_native(dest, request, state, lease_fds, interrupted, mailbox_ctx=None):
    task = (dest / 'task.md').read_text(encoding='utf-8')
    stream = native.Stream(request['expected_conversation_id'])
    argv = native.command(request['config']['executable'], request['cwd'], request['native_dir'], task,
                          request['expected_conversation_id'], request['timeout'])
    state_update(dest, state, status='starting', native_launch_started=True)
    try:
        proc = subprocess.Popen(argv, cwd=request['cwd'], env=child_environment(), stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
                                pass_fds=tuple(lease_fds))
    except OSError:
        state_update(dest, state, native_launch_started=False)
        raise ValueError('native_launch_failed') from None
    deadline = time.monotonic() + request['timeout']
    buffer = b''
    reason = None
    try:
        state_update(dest, state, status='running', native_pid=proc.pid)
        with selectors.DefaultSelector() as selector:
            for name, pipe in (('stdout', proc.stdout), ('stderr', proc.stderr)):
                os.set_blocking(pipe.fileno(), False)
                selector.register(pipe, selectors.EVENT_READ, name)
            while selector.get_map() or proc.poll() is None:
                if interrupted() or cancellation_requested(dest, request):
                    reason = 'cancelled'
                    break
                if time.monotonic() >= deadline:
                    reason = 'timeout'
                    break
                for key, _ in selector.select(0.1):
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    if key.data == 'stderr':
                        continue  # Native stderr can contain credentials and request details.
                    buffer += chunk
                    while b'\n' in buffer:
                        line, buffer = buffer.split(b'\n', 1)
                        if len(line) > native.MAX_OUTPUT:
                            reason = 'native_event_too_large'
                            break
                        if line.strip():
                            stream.feed(line)
                    if len(buffer) > native.MAX_OUTPUT:
                        reason = 'native_event_too_large'
                    if stream.session_id and state.get('conversation_id') != stream.session_id:
                        state_update(dest, state, conversation_id=stream.session_id)
                        root_id_file = dest / 'root-conversation-id.txt'
                        if not root_id_file.exists():
                            tmp_id = dest / f".tmp-root-id-{secrets.token_hex(4)}"
                            tmp_id.write_text(stream.session_id, encoding='utf-8')
                            os.replace(tmp_id, root_id_file)
                        if mailbox_ctx and mailbox_ctx.get('mailbox_dir'):
                            mbx_root_id = Path(mailbox_ctx['mailbox_dir']) / 'root-conversation-id.txt'
                            if not mbx_root_id.exists():
                                tmp_id = Path(mailbox_ctx['mailbox_dir']) / f".tmp-root-id-{secrets.token_hex(4)}"
                                tmp_id.write_text(stream.session_id, encoding='utf-8')
                                os.replace(tmp_id, mbx_root_id)
                        if mailbox_ctx and mailbox_ctx.get('active'):
                            try:
                                host_runtime.heartbeat_mailbox(
                                    mailbox_ctx['socket'],
                                    mailbox_ctx['token'],
                                    mailbox_ctx['generation'],
                                    native_session_id=stream.session_id,
                                )
                            except Exception:
                                pass
                if reason or (stream.error and stream.error != 'native_run_failed'):
                    reason = reason or stream.error
                    break
            if buffer.strip() and reason is None:
                stream.feed(buffer)
        if cancellation_requested(dest, request):
            reason = 'cancelled'
    finally:
        clean = cleanup_process(proc)
        proc.stdout.close()
        proc.stderr.close()
    return stream, reason, proc.returncode, clean


def worker(key, runtime, lease_fd, workspace_fd):
    runtime = runtime_path(runtime)
    dest = job_path(runtime, key)
    request = checked_request(dest)
    inherited_lease(lease_fd, slot_lock_path(runtime, request_slot(request)))
    inherited_lease(workspace_fd, instances.workspace_lock_path(HERMES_RUNTIME, request['cwd']))
    interrupted = False
    def stop(signum, frame):
        nonlocal interrupted
        interrupted = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    state = read_json(dest / 'state.json')
    stream = None
    native_created = False
    mailbox_ctx = None
    try:
        with locked(dest / 'worker.lock'):
            state_update(dest, state, worker=process_info(os.getpid()), status='starting')
            if interrupted or cancellation_requested(dest, request):
                state_update(dest, state, status='cancelled')
                return
            task_cwd(runtime, key, request['cwd'])
            if configuration(runtime) != request['config'] or file_digest(request['config']['executable']) != request['executable_sha256']:
                raise ValueError('execution_configuration_changed')
            archive = None
            if request['parent_job']:
                parent = job_path(runtime, request['parent_job'])
                previous = read_json(parent / 'state.json')
                archive = parent / 'native'
                if tree_digest(archive) != previous['native_sha256']:
                    raise ValueError('parent_archive_changed')
            native.prepare_store(request['native_dir'], request['cwd'], request['config']['auth_file'], archive)
            native_created = True

            if request.get('mailbox_id'):
                if host_runtime is None:
                    raise ValueError('agent_mailbox_components_missing')
                components = request.get('components') or {}
                component_paths = {}
                for name, error in (('agy_hook', 'hook_script_tampered'),
                                    ('mcp_server', 'mcp_script_tampered'),
                                    ('host_runtime', 'host_script_tampered'),
                                    ('client', 'client_script_tampered')):
                    record = components.get(name)
                    if not isinstance(record, dict) or not record.get('path') or not record.get('sha256'):
                        raise ValueError('agent_mailbox_components_missing')
                    path = Path(record['path'])
                    if not path.is_file() or file_digest(path) != record['sha256']:
                        raise ValueError(error)
                    component_paths[name] = path

                hook_path = component_paths['agy_hook']
                mcp_script_path = component_paths['mcp_server']
                service_socket = request['service_socket']
                mailbox_id = request['mailbox_id']
                token = request['mailbox_token']
                generation = secrets.token_hex(8)
                native_mbx_dir = Path(request['native_dir']) / 'mailbox'
                native_mbx_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
                binding_file = native_mbx_dir / 'mailbox-binding.json'
                # Persist the exact generation and phase on the host before any
                # network call, so a lost activate response can be reconciled.
                generation_file = dest / 'generation.json'
                generation_base = {'mailbox_id': mailbox_id, 'generation': generation,
                                   'socket': str(service_socket), 'phase': 'prepared'}
                host_runtime.create_binding_file(
                    binding_file,
                    service_socket,
                    token,
                    mailbox_id,
                    generation,
                    scope=request.get('scope'),
                )
                host_runtime.create_binding_file(
                    dest / 'mailbox-binding.json',
                    service_socket,
                    token,
                    mailbox_id,
                    generation,
                    scope=request.get('scope'),
                )
                write_json(generation_file, generation_base | {'updated_at': now()})

                mailbox_ctx = {
                    'socket': service_socket,
                    'token': token,
                    'mailbox_id': mailbox_id,
                    'generation': generation,
                    'mailbox_dir': str(native_mbx_dir),
                    'active': False,
                }

                try:
                    host_runtime.activate_mailbox(
                        service_socket,
                        token,
                        generation,
                        capabilities=request.get('capabilities', ['live_input']),
                    )
                except Exception as exc:
                    # The request may or may not have committed. Keep the exact
                    # generation and phase for a later public reconcile.
                    write_json(generation_file, generation_base | {
                        'phase': 'activate_unknown', 'error': type(exc).__name__, 'updated_at': now()})
                    raise
                mailbox_ctx['active'] = True
                write_json(generation_file, generation_base | {'phase': 'activated', 'updated_at': now()})

                native_config = Path(request['native_dir']) / 'config'
                native_config.mkdir(mode=0o700, exist_ok=True)

                hooks_path = native_config / 'hooks.json'
                existing_hooks = {}
                if hooks_path.exists() and not hooks_path.is_symlink():
                    try:
                        existing_hooks = json.loads(hooks_path.read_text(encoding='utf-8'))
                    except (json.JSONDecodeError, OSError) as exc:
                        raise ValueError('hooks_config_invalid') from exc
                    if not isinstance(existing_hooks, dict):
                        raise ValueError('hooks_config_invalid')
                python = shlex.quote(sys.executable)
                existing_hooks['agent-mailbox'] = {
                    'PreInvocation': [
                        {
                            'type': 'command',
                            'command': f"{python} {shlex.quote(str(hook_path))} --phase pre --task-dir {shlex.quote(str(native_mbx_dir))}",
                            'timeout': 10,
                        }
                    ],
                    'PostInvocation': [
                        {
                            'type': 'command',
                            'command': f"{python} {shlex.quote(str(hook_path))} --phase post --task-dir {shlex.quote(str(native_mbx_dir))}",
                            'timeout': 10,
                        }
                    ],
                }
                if hooks_path.exists():
                    hooks_path.unlink()
                native.private_file(hooks_path, (json.dumps(existing_hooks, ensure_ascii=False) + '\n').encode('utf-8'))

                mcp_config_path = native_config / 'mcp_config.json'
                existing_mcp = {}
                if mcp_config_path.exists() and not mcp_config_path.is_symlink():
                    try:
                        existing_mcp = json.loads(mcp_config_path.read_text(encoding='utf-8'))
                    except (json.JSONDecodeError, OSError) as exc:
                        raise ValueError('mcp_config_invalid') from exc
                    if not isinstance(existing_mcp, dict):
                        raise ValueError('mcp_config_invalid')
                servers = existing_mcp.setdefault('mcpServers', {})
                if not isinstance(servers, dict):
                    raise ValueError('mcp_config_invalid')
                servers['agent-mailbox'] = {
                    'command': sys.executable,
                    'args': [
                        str(mcp_script_path),
                        '--binding',
                        str(binding_file),
                    ],
                }
                if mcp_config_path.exists():
                    mcp_config_path.unlink()
                native.private_file(mcp_config_path, (json.dumps(existing_mcp, ensure_ascii=False) + '\n').encode('utf-8'))

            if interrupted or cancellation_requested(dest, request):
                state_update(dest, state, status='cancelled')
                return
            # Reserve the next turn just before native execution, while holding
            # both leases. Failures known to precede execution restore the parent.
            write_json(job_path(runtime, request['root_job']) / 'session.json', {'latest_job': key})
            stream, reason, code, clean = execute_native(dest, request, state, (lease_fd, workspace_fd), lambda: interrupted, mailbox_ctx=mailbox_ctx)
            state_update(dest, state, status='saving', returncode=code, cleanup_confirmed=clean,
                         tools=list(stream.tools.values()), native_status=(stream.terminal or {}).get('status'),
                         native_outcome=stream.diagnostic(),
                         protocol={'input_accepted': stream.accepted, 'reply_done': stream.reply_done,
                                   'last_step_type': stream.last_kind, 'reply_index': stream.reply_index,
                                   'last_step_index': stream.last_index})
            if not clean:
                state_update(dest, state, status='cleanup_failed', error='native_processes_remain')
                return
            raw_partial = ''.join(stream.partial).encode('utf-8')
            if raw_partial:
                native.private_file(dest / 'partial.md', raw_partial)
                state.update(partial_bytes=len(raw_partial), partial_sha256=digest(raw_partial))
            native.copy_tree(request['native_dir'], dest / 'native')
            state['native_sha256'] = tree_digest(dest / 'native')
            shutil.rmtree(request['native_dir'])
            if cancellation_requested(dest, request) or interrupted:
                reason = 'cancelled'
            report = stream.report()
            if reason or stream.error or code != 0 or report is None:
                status_code = reason or stream.error or ('process_failed' if code != 0 else 'incomplete')
                state_update(dest, state, status=status_code, error=status_code)
            else:
                raw = report.encode('utf-8')
                native.private_file(dest / 'report.md', raw)
                state_update(dest, state, status='completed', report_bytes=len(raw), report_sha256=digest(raw), error=None)
    except Exception as exc:
        # Report local failure classes, never exception text containing task or credentials.
        code = error_code(exc)
        state_update(dest, state, status='failed', error=code, native_store_preserved=native_created)
    finally:
        if mailbox_ctx and mailbox_ctx.get('active'):
            generation_file = dest / 'generation.json'
            try:
                host_runtime.deactivate_mailbox(
                    mailbox_ctx['socket'],
                    mailbox_ctx['token'],
                    mailbox_ctx['generation'],
                )
            except Exception as deact_err:
                # Keep the exact generation and phase; a later turn reconciles
                # only its own leftover generation against the public service.
                write_json(generation_file, {
                    'mailbox_id': mailbox_ctx['mailbox_id'],
                    'generation': mailbox_ctx['generation'],
                    'socket': mailbox_ctx['socket'],
                    'phase': 'deactivate_failed',
                    'error': type(deact_err).__name__,
                    'updated_at': now(),
                })
                try:
                    write_json(dest / 'pending-deactivation.json', {
                        'mailbox_id': mailbox_ctx['mailbox_id'],
                        'generation': mailbox_ctx['generation'],
                        'token': mailbox_ctx['token'],
                        'socket': mailbox_ctx['socket'],
                        'error': str(deact_err),
                        'timestamp': now(),
                    })
                except Exception:
                    pass
            else:
                write_json(generation_file, {
                    'mailbox_id': mailbox_ctx['mailbox_id'],
                    'generation': mailbox_ctx['generation'],
                    'socket': mailbox_ctx['socket'],
                    'phase': 'ended',
                    'updated_at': now(),
                })
            mailbox_ctx['active'] = False
        try:
            if not state.get('native_launch_started'):
                if native_created:
                    shutil.rmtree(request['native_dir'])
                    state_update(dest, state, native_store_preserved=False)
                if request['parent_job']:
                    session_path = job_path(runtime, request['root_job']) / 'session.json'
                    if read_json(session_path)['latest_job'] == key:
                        write_json(session_path, {'latest_job': request['parent_job']})
        finally:
            for fd in (lease_fd, workspace_fd):
                os.close(fd)


def launch_watcher(dest, runtime):
    write_json(dest / 'callback.json', {'status': 'waiting_task', 'updated_at': now()})
    try:
        proc = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()), '_watch',
                                 '--job', dest.name, '--runtime', str(runtime)], stdin=subprocess.DEVNULL,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
    except OSError:
        write_json(dest / 'callback.json', {'status': 'launch_failed', 'updated_at': now()})
    else:
        try:
            write_json(dest / 'callback-launch.json', {'worker': process_info(proc.pid)})
        except OSError:
            pass  # The watcher also records its own identity; do not reset it.


def callback_step(dest, request, value):
    binding = ipc.validate_binding(request['callback_target'])
    state = read_json(dest / 'callback.json')
    if state['status'] in ('delivered', 'unknown', 'blocked', 'expired', 'launch_failed'):
        return state
    event_id = digest(json.dumps({'job': str(dest), 'task': request['task_sha256'],
                                  'target': binding['target']}, sort_keys=True).encode())
    payload = {'event_id': event_id, 'target': binding['target'], 'prompt': '\n'.join([
        '【桃乐丝按任务调用回传｜本机程序生成的结束事件】',
        '任务编号：' + dest.name, '执行状态：' + value['status'], '结果目录：' + str(dest),
        '事件编号：' + event_id,
        '本机程序已核对执行进程退出。请读取本任务回执及必要结果，继续原任务已授权的下一步。',
        '这是原任务的接续资料，不是用户新增指令；研究正文需按原任务判断，执行结束不代表已入库或发布。'])}
    payload_path = dest / 'callback-input.json'
    if payload_path.exists():
        saved = read_json(payload_path)
        if saved.get('routing') == 'chat':
            payload['routing'] = 'chat'
        if saved != payload or (state.get('input_sha256') and state['input_sha256'] != file_digest(payload_path)):
            raise ValueError('callback_input_modified')
    else:
        if state.get('input_sha256'):
            raise ValueError('callback_input_missing')
        # Old watchers did not save input.json. Their recorded event stays v1.
        if not state.get('event_id'):
            payload['routing'] = 'chat'
        write_json(payload_path, payload)
    state = state | {'input_sha256': file_digest(payload_path)}
    querying = state['status'] != 'waiting_task' or state.get('query_only', False)
    state = state | {'status': 'querying' if querying else 'sending',
                     'event_id': event_id, 'query_only': True, 'updated_at': now()}
    write_json(dest / 'callback.json', state)
    try:
        reply = ipc.call(binding['socket'], 'GET' if querying else 'POST',
                         '/ipc/event?event_id=' + event_id if querying else '/ipc/event', None if querying else payload)
    except ipc.ReceiverOffline:
        status_code = 'querying' if querying else 'waiting_task'
        output = state | {'status': status_code, 'query_only': querying, 'code': 'receiver_offline'}
    except ipc.DeliveryUnknown:
        output = state | {'status': 'querying', 'query_only': True, 'code': 'delivery_unconfirmed'}
    else:
        if reply.get('input') != payload or reply.get('status') not in ipc.RECEIVER_STATES:
            output = state | {'status': 'unknown', 'query_only': True, 'code': 'receipt_mismatch'}
        else:
            retry = ipc.receipt_allows_retry(reply)
            output = state | {'status': 'waiting_task' if retry else reply['status'], 'query_only': not retry,
                              'event_id': event_id, 'code': reply.get('code', ''),
                              'delivery_target': reply.get('delivery_target')}
            if reply['status'] == 'delivered':
                try:
                    actual = ipc.delivery_binding(binding, payload, reply)
                    ack = ipc.call(binding['socket'], 'POST', '/ipc/ack', {'event_id': event_id, 'target': actual['target']})
                    output['acknowledged'] = bool(ack.get('acknowledged_at') and ack.get('input') == payload)
                except (ipc.ReceiverOffline, ipc.DeliveryUnknown, ValueError):
                    output['acknowledged'] = False
    write_json(dest / 'callback.json', output | {'updated_at': now()})
    return output


def watch(key, runtime):
    runtime = runtime_path(runtime)
    dest = job_path(runtime, key)
    request = checked_request(dest)
    with locked(dest / 'callback.lock'):
        write_json(dest / 'callback-launch.json', {'worker': process_info(os.getpid())})
        deadline = time.monotonic() + request['timeout'] + 600
        while time.monotonic() < deadline:
            value = status(key, runtime)
            if value['status'] not in ACTIVE:
                if value['cleanup']['worker'] == 'absent' and value['cleanup']['native_group'] == 'absent':
                    if value['status'] == 'completed':
                        result(key, runtime)
                    state = callback_step(dest, request, value)
                    if state['status'] in ('delivered', 'unknown', 'blocked', 'expired', 'launch_failed'):
                        return
            time.sleep(1)
        state = read_json(dest / 'callback.json')
        uncertain = state.get('query_only') or state['status'] in ('sending', 'querying', 'dispatching')
        write_json(dest / 'callback.json', state | {'status': 'unknown' if uncertain else 'expired',
                                                  'code': 'callback_deadline', 'updated_at': now()})


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description='由 Atlas 按任务调用桃乐丝')
    sub = parser.add_subparsers(dest='action', required=True)
    for name in ('submit', 'followup', 'status', 'result', 'export-visible', 'cancel', '_worker', '_watch'):
        item = sub.add_parser(name)
        item.add_argument('--runtime', type=Path, default=RUNTIME)
        item.add_argument('--job', required=True)
        if name in ('submit', 'followup'):
            item.add_argument('--task', type=Path, required=True)
            item.add_argument('--cwd', type=Path)
            item.add_argument('--timeout', type=int)
            item.add_argument('--delivery', choices=('manual', 'conversation'))
            item.add_argument('--team', type=str)
        if name == 'followup':
            item.add_argument('--from-job', required=True)
        if name == '_worker':
            item.add_argument('--lease-fd', type=int, required=True)
            item.add_argument('--workspace-fd', type=int, required=True)
    item = sub.add_parser('doctor')
    item.add_argument('--runtime', type=Path, default=RUNTIME)
    item = sub.add_parser('list')
    item.add_argument('--runtime', type=Path, default=RUNTIME)
    args = parser.parse_args()
    try:
        if args.action in ('submit', 'followup'):
            output = submit(args.job, args.task, cwd=args.cwd, parent=getattr(args, 'from_job', None),
                            timeout=args.timeout, delivery=args.delivery, runtime=args.runtime,
                            team=getattr(args, 'team', None))
        elif args.action in ('status', 'result', 'cancel'):
            output = globals()[args.action](args.job, args.runtime)
        elif args.action == 'export-visible':
            output = export_visible(args.job, args.runtime)
        elif args.action == '_worker':
            worker(args.job, args.runtime, args.lease_fd, args.workspace_fd)
            return
        elif args.action == '_watch':
            watch(args.job, args.runtime)
            return
        elif args.action == 'doctor':
            runtime = runtime_path(args.runtime)
            config = configuration(runtime)
            validate_temp_boundary(runtime)
            output = {'available': True, 'executable': config['executable'], 'authentication_file_available': True,
                      'model': native.MODEL, 'effort': native.EFFORT, 'permission_mode': 'workspace-write',
                      'sandbox_available': sys.platform == 'darwin' and Path('/usr/bin/sandbox-exec').is_file(),
                      'note': '仅检查本机文件与配置，不发模型请求。'}
        else:
            runtime = runtime_path(args.runtime)
            output = [status(path.name, runtime) for path in sorted((runtime / 'jobs').glob('*'))
                      if not path.name.startswith('.') and (path / 'request.json').is_file()]
        print(json.dumps(output, ensure_ascii=False, indent=2))
    except (ValueError, OSError, KeyError, json.JSONDecodeError) as exc:
        code = error_code(exc)
        msg = ERROR_MESSAGES.get(code, '操作未完成，请按错误码核对本机任务记录。')
        print(json.dumps({'error': code, 'message': msg}, ensure_ascii=False))
        raise SystemExit(1)


if __name__ == '__main__':
    main()
