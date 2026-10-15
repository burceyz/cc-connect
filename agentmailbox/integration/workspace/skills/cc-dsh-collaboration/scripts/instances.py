"""Independent DSH homes and inherited leases for concurrent main agents."""
from contextlib import contextmanager, ExitStack
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import uuid

AUTO = 'auto'
CAPACITY = 6
PROXY_KEYS = {'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY'}


def root(runtime, instance_id):
    """Resolve an existing instance without creating or migrating anything."""
    runtime = Path(runtime).resolve()
    if instance_id is None:
        return runtime
    if not isinstance(instance_id, str) or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}', instance_id) or instance_id == AUTO:
        raise ValueError('invalid_instance_id')
    parent = runtime / 'instances'
    target = parent / instance_id
    if parent.is_symlink() or target.is_symlink() or not target.resolve().is_relative_to(parent):
        raise ValueError('instance_outside_runtime')
    if not target.is_dir():
        raise ValueError('instance_unavailable')
    for name in ('home', 'work'):
        child = target / name
        if child.is_symlink() or not child.resolve().is_relative_to(target):
            raise ValueError('instance_outside_runtime')
        if (name == 'home' or child.exists()) and not child.is_dir():
            raise ValueError('instance_unavailable')
    return target


def _proxy_environment(runtime):
    source = runtime / 'home' / '.env'
    if (runtime / 'home').is_symlink() or source.is_symlink():
        raise ValueError('proxy_environment_symlink')
    if not source.exists():
        return ''
    selected = {}
    for line in source.read_text(encoding='utf-8').splitlines():
        match = re.fullmatch(r'\s*(?:export\s+)?([A-Za-z_][A-Za-z_0-9]*)\s*=\s*(.*)', line)
        if not match or match[1] not in PROXY_KEYS:
            continue
        try:
            values = shlex.split(match[2], comments=True, posix=True)
        except ValueError:
            raise ValueError('invalid_proxy_environment') from None
        if len(values) > 1 or any(c in ''.join(values) for c in '\r\n\x00'):
            raise ValueError('invalid_proxy_environment')
        selected[match[1]] = values[0] if values else ''
    return ''.join(key + '=' + json.dumps(value, ensure_ascii=False) + '\n'
                   for key, value in selected.items())


def allocate(runtime):
    """Create one private home/work pair; copy only selected proxy values."""
    runtime = Path(runtime).resolve()
    runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
    parent = runtime / 'instances'
    if parent.is_symlink():
        raise ValueError('instance_outside_runtime')
    parent.mkdir(exist_ok=True, mode=0o700)
    proxy = _proxy_environment(runtime)
    instance_id = uuid.uuid4().hex
    target = parent / instance_id
    target.mkdir(mode=0o700)
    try:
        (target / 'home').mkdir(mode=0o700)
        (target / 'work').mkdir(mode=0o700)
        if proxy:
            fd = os.open(target / 'home/.env', os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                handle.write(proxy)
    except BaseException:
        shutil.rmtree(target)
        raise
    return instance_id


def prepare(runtime, instance_id=AUTO):
    if instance_id == AUTO:
        return allocate(runtime)
    if instance_id is None:
        target = root(runtime, None)
        target.mkdir(parents=True, exist_ok=True, mode=0o700)
        for name in ('home', 'work'):
            child = target / name
            if child.is_symlink():
                raise ValueError('instance_outside_runtime')
            child.mkdir(exist_ok=True, mode=0o700)
    else:
        root(runtime, instance_id)
    return instance_id


def _open_lock(path):
    """Do not unlink leases: inherited descriptors must keep the same inode."""
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError:
        raise ValueError('lock_unavailable') from None
    return os.fdopen(fd, 'r+', encoding='utf-8')


@contextmanager
def locked(runtime, instance_id=None):
    target = root(runtime, instance_id)
    if instance_id is None:
        target.mkdir(parents=True, exist_ok=True, mode=0o700)
    with _open_lock(target / 'bridge.lock') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('bridge_busy') from None
        yield handle


def _coordination(runtime):
    target = Path(runtime).resolve() / 'coordination'
    if target.is_symlink() or (target / 'workspaces').is_symlink():
        raise ValueError('lock_outside_runtime')
    target.mkdir(exist_ok=True, mode=0o700)
    (target / 'workspaces').mkdir(exist_ok=True, mode=0o700)
    return target


def workspace_lock_path(runtime, cwd):
    value = str(Path(cwd).resolve())
    key = hashlib.sha256(value.encode()).hexdigest()
    return Path(runtime).resolve() / 'coordination/workspaces' / (key + '.lock')


def _workspace_lease(runtime, cwd, *, readonly=False):
    control = _coordination(runtime)
    # This short lock only publishes/checks leases. No model work occurs under it.
    with _open_lock(control / 'registry.lock') as registry:
        fcntl.flock(registry, fcntl.LOCK_EX)
        for path in (control / 'workspaces').glob('*.lock'):
            with _open_lock(path) as probe:
                try:
                    fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    try:
                        value = json.load(probe)['cwd']
                        if not isinstance(value, str) or not Path(value).is_absolute():
                            raise ValueError()
                    except (ValueError, TypeError, KeyError):
                        raise ValueError('workspace_lease_invalid') from None
                    other = Path(value)
                    if cwd.is_relative_to(other) or other.is_relative_to(cwd):
                        raise ValueError('workspace_busy')
        handle = _open_lock(workspace_lock_path(runtime, cwd))
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            handle.seek(0)
            handle.truncate()
            json.dump({'cwd': str(cwd)}, handle)
            handle.flush()
            if readonly:
                # Transfer under registry.lock so another executor cannot enter
                # the gap. A native child needs the lease, not a writable ledger.
                handle.close()
                fd = os.open(workspace_lock_path(runtime, cwd), os.O_RDONLY | os.O_NOFOLLOW)
                handle = os.fdopen(fd, 'r', encoding='utf-8')
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException:
            handle.close()
            raise
        return handle


@contextmanager
def workspace_lease(runtime, cwd):
    """Share directory exclusion with another executor, without a DSH capacity slot."""
    if not isinstance(cwd, (str, Path)) or not str(cwd):
        raise ValueError('invalid_cwd')
    cwd = Path(cwd).resolve()
    if not cwd.is_dir():
        raise ValueError('cwd_not_directory')
    with _workspace_lease(runtime, cwd, readonly=True) as handle:
        yield handle


@contextmanager
def execution(runtime, instance_id, cwd):
    """Hold instance, capacity and overlapping-workspace leases for one run."""
    if not isinstance(cwd, (str, Path)) or not str(cwd):
        raise ValueError('invalid_cwd')
    cwd = Path(cwd).resolve()
    if not cwd.is_dir():
        raise ValueError('cwd_not_directory')
    with ExitStack() as stack:
        instance = stack.enter_context(locked(runtime, instance_id))
        control = _coordination(runtime)
        capacity = None
        for index in range(CAPACITY):
            candidate = _open_lock(control / ('capacity-' + str(index) + '.lock'))
            try:
                fcntl.flock(candidate, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                candidate.close()
            else:
                capacity = stack.enter_context(candidate)
                break
        if capacity is None:
            raise ValueError('capacity_busy')
        workspace = stack.enter_context(_workspace_lease(runtime, cwd))
        # Close-only on context exit: explicit LOCK_UN would unlock a child copy.
        yield instance.fileno(), capacity.fileno(), workspace.fileno()
