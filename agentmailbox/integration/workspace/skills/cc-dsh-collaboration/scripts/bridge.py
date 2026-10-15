#!/usr/bin/env python3
"""Workspace-local DSH bridge. Never persist raw model streams or stderr."""
import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import uuid
import secrets

import instances
import read_sources as source_policy
import routing


def host_runtime_candidates(base):
    """Search production and development layouts for the shared host module.

    Production installs the bridge at ``WORKSPACE/skills/cc-dsh-collaboration/scripts``
    and the shared host at ``WORKSPACE/skills/cc-agent-ipc/scripts``; the development
    worktree keeps it beside the plugin under ``skill/scripts``. Only this file's
    location matters, never the hardcoded workspace root.
    """
    base = Path(base).resolve()
    preferred = []
    fallback = []
    for ancestor in list(base.parents)[:6]:
        preferred.append(ancestor / 'cc-agent-ipc/scripts/host_runtime.py')
        preferred.append(ancestor / 'skill/scripts/host_runtime.py')
        fallback.append(ancestor / 'host_runtime.py')
    return preferred + fallback


try:
    import host_runtime
except ImportError:
    for candidate in host_runtime_candidates(__file__):
        if candidate.is_file():
            if str(candidate.parent) not in sys.path:
                sys.path.insert(0, str(candidate.parent))
            break
    try:
        import host_runtime
    except ImportError:
        host_runtime = None


def resolve_dsh_plugin_path(base=None):
    """Locate the trusted mailbox plugin in production or development layout."""
    base = Path(base or __file__).resolve()
    for ancestor in list(base.parents)[:6]:
        for candidate in (
            ancestor / 'cc-agent-ipc/scripts/dsh-plugin.mjs',
            ancestor / 'skill/scripts/dsh-plugin.mjs',
            ancestor / 'dsh-plugin.mjs',
        ):
            if candidate.is_file():
                return candidate.resolve()
    return None

WORKSPACE = Path(__file__).resolve().parents[3]
RUNTIME = WORKSPACE / '.local/dsh-collaboration'
BROAD_CWD = ('', '.local', '.worktrees', '.task-clones', 'workdir', '.local/dorothy-work')
PATCH_NAME = 'model.patch.yml'
# Per-invocation subscription profiles map to fixed patch files. Callers can
# never pass a path and there is no automatic fallback between subscriptions.
DEFAULT_PROFILE = 'opencode-go'
PROFILE_PATCHES = {DEFAULT_PROFILE: PATCH_NAME, 'happy2': 'happy2-high.patch.yml',
                   'happy1': 'happy1-high.patch.yml'}
DEFAULT_REASONING_EFFORT = 'high'
REASONING_EFFORTS = ('high', 'max')
MAX_PROFILE_PATCHES = {DEFAULT_PROFILE: 'opencode-go-max.patch.yml',
                       'happy2': 'happy2-max.patch.yml', 'happy1': 'happy1-max.patch.yml'}
DEFAULT_MODEL = 'deepseek-v4.1-flash'
MODEL_PATCHES = {
    DEFAULT_MODEL: {'high': PROFILE_PATCHES, 'max': MAX_PROFILE_PATCHES},
    'glm-5.3': {
        'high': {'happy2': 'happy2-glm-5.3-high.patch.yml', 'happy1': 'happy1-glm-5.3-high.patch.yml'},
        'max': {'happy2': 'happy2-glm-5.3-max.patch.yml', 'happy1': 'happy1-glm-5.3-max.patch.yml'},
    },
}
# Friendly workspace profile names need not match the native provider route.
PROFILE_PROVIDERS = {DEFAULT_PROFILE: DEFAULT_PROFILE, 'happy2': 'happy2', 'happy1': 'happy'}
MAX_LINE = 16 * 1024 * 1024
TOOL_POLICIES = ('default', 'none', 'research')
POLICY_VERSION = 1
POLICY_PLUGIN = Path(__file__).resolve().with_name('consultation-policy.mjs')
POLICY_OVERLAY = Path(__file__).resolve().with_name('consultation-policy.patch.yml')
RESEARCH_PLUGIN = Path(__file__).resolve().with_name('research-policy.mjs')
RESEARCH_OVERLAY = Path(__file__).resolve().with_name('research-policy.patch.yml')
RESEARCH_REQUIRED_TOOLS = ('mcp__duckduckgo__search', 'web_fetch')
RESEARCH_ALLOWED_TOOLS = RESEARCH_REQUIRED_TOOLS + tuple(
    'mcp__research_status__' + name for name in
    ('status_read', 'status_report', 'status_claim_probe', 'status_complete_probe'))
POLICY_HASH_FIELDS = ('model_config_sha256', 'tool_policy_plugin_sha256',
                      'tool_policy_overlay_sha256')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def resolve_tool_policy(tool_policy=None):
    if tool_policy is None:
        return 'default'
    if not isinstance(tool_policy, str) or tool_policy not in TOOL_POLICIES:
        raise ValueError('unknown_tool_policy')
    return tool_policy


def tool_policy_record(record):
    """Validate a receipt's policy binding without reading today's deployment."""
    policy = resolve_tool_policy(record.get('tool_policy'))
    result = {'tool_policy': policy}
    if policy != 'default':
        if type(record.get('tool_policy_version')) is not int or record['tool_policy_version'] != POLICY_VERSION:
            raise ValueError('tool_policy_invalid')
        result['tool_policy_version'] = POLICY_VERSION
        for field in POLICY_HASH_FIELDS:
            value = record.get(field)
            if not isinstance(value, str) or not re.fullmatch('[a-f0-9]{64}', value):
                raise ValueError('tool_policy_invalid')
            result[field] = value
        expected = digest(json.dumps(result, sort_keys=True, separators=(',', ':')).encode())
        if record.get('config_sha256') != expected:
            raise ValueError('config_changed')
    return result


def validate_tool_policy(record, expected):
    """Bind request, receipt and run; missing legacy fields mean default only."""
    if tool_policy_record(record) != tool_policy_record(expected):
        raise ValueError('tool_policy_mismatch')


def attestation_fields(record, tools=None):
    """The fixed host policies define which native tool surfaces may be proved."""
    policy = resolve_tool_policy(record.get('tool_policy'))
    if policy not in ('none', 'research'):
        raise ValueError('tool_policy_unverified')
    expected = {'policy': policy, 'version': POLICY_VERSION, 'schema_count': 0,
                'presentation': 'native', 'guard': 'deny_all',
                'plugin_sha256': record['tool_policy_plugin_sha256'],
                'overlay_sha256': record['tool_policy_overlay_sha256'],
                'config_sha256': record['config_sha256']}
    if policy == 'research':
        if (not isinstance(tools, list) or any(not isinstance(name, str) for name in tools) or
                tools != sorted(set(tools)) or not set(RESEARCH_REQUIRED_TOOLS).issubset(tools) or
                set(tools) - set(RESEARCH_ALLOWED_TOOLS)):
            raise ValueError('tool_policy_unverified')
        expected.update(schema_count=len(tools), guard='allow_research_only', tools=tools)
    return expected


def validate_tool_policy_attestation(record):
    """A successful consultation must have the native plugin's request proof."""
    if tool_policy_record(record)['tool_policy'] == 'default':
        return
    proof = record.get('tool_policy_attestation')
    if not isinstance(proof, dict):
        raise ValueError('tool_policy_unverified')
    expected = attestation_fields(record, proof.get('tools')) | {'session_id': record.get('session_id')}
    if (not isinstance(proof, dict) or proof != expected or
            type(proof.get('version')) is not int or type(proof.get('schema_count')) is not int):
        raise ValueError('tool_policy_unverified')


def execution_config(patch, tool_policy=None):
    """Read fixed host artifacts once; callers later execute these exact bytes."""
    policy = resolve_tool_policy(tool_policy)
    artifacts = {'model': patch.read_bytes()}
    record = {'tool_policy': policy, 'config_sha256': digest(artifacts['model'])}
    if policy != 'default':
        plugin, overlay = ((POLICY_PLUGIN, POLICY_OVERLAY) if policy == 'none' else
                           (RESEARCH_PLUGIN, RESEARCH_OVERLAY))
        for label, path in [('plugin', plugin), ('overlay', overlay)]:
            if path.is_symlink() or not path.is_file():
                raise ValueError('tool_policy_unavailable')
            artifacts[label] = path.read_bytes()
        record.update(tool_policy_version=POLICY_VERSION,
                      model_config_sha256=record['config_sha256'],
                      tool_policy_plugin_sha256=digest(artifacts['plugin']),
                      tool_policy_overlay_sha256=digest(artifacts['overlay']))
        bound = {key: value for key, value in record.items() if key != 'config_sha256'}
        record['config_sha256'] = digest(json.dumps(bound, sort_keys=True, separators=(',', ':')).encode())
    return record, artifacts


def validate_consultation_model(tool_policy, profile, reasoning_effort, model=None):
    """Validate model, profile and reasoning effort for restricted tool policies."""
    if tool_policy == 'none':
        if (profile not in PROFILE_PROVIDERS or
                reasoning_effort != DEFAULT_REASONING_EFFORT or resolve_model(model) != DEFAULT_MODEL):
            raise ValueError('consultation_model_mismatch')
    elif tool_policy == 'research':
        if (profile != DEFAULT_PROFILE or
                reasoning_effort != DEFAULT_REASONING_EFFORT or resolve_model(model) != DEFAULT_MODEL):
            raise ValueError('consultation_model_mismatch')


def snapshot_tool_policy(dest, artifacts, record):
    """Materialize the accepted plugin and overlay; never take a caller path."""
    plugin = dest / 'consultation-policy.mjs'
    overlay = dest / 'consultation-policy.patch.yml'
    model = dest / 'model.patch.yml'
    substitutions = {'__CONSULTATION_PLUGIN__': str(plugin),
                     '__CONSULTATION_PLUGIN_SHA256__': record['tool_policy_plugin_sha256'],
                     '__CONSULTATION_OVERLAY_SHA256__': record['tool_policy_overlay_sha256'],
                     '__CONSULTATION_CONFIG_SHA256__': record['config_sha256']}
    rendered = artifacts['overlay'].decode('utf-8')
    for placeholder, value in substitutions.items():
        token = json.dumps(placeholder)
        if rendered.count(token) != 1:
            raise ValueError('tool_policy_invalid')
        rendered = rendered.replace(token, json.dumps(value))
    plugin.write_bytes(artifacts['plugin'])
    overlay.write_text(rendered, encoding='utf-8')
    model.write_bytes(artifacts['model'])
    record['tool_policy_runtime_overlay_sha256'] = digest(overlay.read_bytes())
    return model, overlay


def resolve_reasoning_effort(reasoning_effort=None):
    if reasoning_effort is None:
        return DEFAULT_REASONING_EFFORT
    if reasoning_effort not in REASONING_EFFORTS:
        raise ValueError('unknown_reasoning_effort')
    return reasoning_effort


def resolve_model(model=None):
    if model is None:
        return DEFAULT_MODEL
    if not isinstance(model, str) or model not in MODEL_PATCHES:
        raise ValueError('unknown_model')
    return model


def resolve_profile(profile=None, runtime=None, reasoning_effort=None, model=None):
    """Return (profile, patch path) for a fixed subscription mapping.

    When ``profile`` is omitted (None), normal tasks follow runtime routing,
    falling back to opencode-go when routing configuration is missing.
    Restricted tool policies enforce fixed models: none allows explicit Go or
    Happy (DeepSeek/High) while implicit none defaults to Go; research strictly
    requires Go/High. An explicit unknown profile or missing patch is
    rejected here before any model process is launched.
    """
    runtime = RUNTIME if runtime is None else runtime
    reasoning_effort = resolve_reasoning_effort(reasoning_effort)
    model = resolve_model(model)
    if profile is None:
        routing_cfg = routing.load_routing(runtime)
        profile = routing_cfg['default_profile']
        if profile == DEFAULT_PROFILE and reasoning_effort == DEFAULT_REASONING_EFFORT and model == DEFAULT_MODEL:
            return DEFAULT_PROFILE, runtime / PATCH_NAME
    if not isinstance(profile, str) or profile not in PROFILE_PATCHES:
        raise ValueError('unknown_profile')
    patches = MODEL_PATCHES[model][reasoning_effort]
    if profile not in patches:
        raise ValueError('model_profile_unavailable')
    patch = runtime / patches[profile]
    if not patch.is_file():
        raise ValueError('profile_patch_unavailable')
    return profile, patch


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    tmp.replace(path)


def scope():
    if not Path.cwd().resolve().is_relative_to(WORKSPACE):
        raise ValueError('outside_workspace')


def binary():
    configured = os.environ.get('DSH_BIN')
    found = shutil.which(configured or 'dsh')
    if found:
        return found
    if configured:
        raise ValueError('dsh_unavailable')

    # launchd commonly starts cc-connect with a minimal PATH, while the
    # installed npm binary lives under the user's nvm tree. Prefer the newest
    # explicitly installed nvm version without guessing a system-wide binary.
    nvm_root = Path.home() / '.nvm' / 'versions' / 'node'
    def version(path):
        match = re.fullmatch(r'v(\d+)\.(\d+)\.(\d+)', path.parent.parent.name)
        return tuple(map(int, match.groups())) if match else (0, 0, 0)
    candidates = sorted(nvm_root.glob('*/bin/dsh'), key=version, reverse=True)
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise ValueError('dsh_unavailable')


def execution_env(executable):
    """Keep the selected installation's interpreter available to its launcher and children."""
    env = os.environ.copy()
    # npm launchers use /usr/bin/env node. Finding an absolute dsh path is not
    # sufficient when launchd omitted its nvm bin directory from PATH. Async
    # receipts canonicalize the launcher to lib/node_modules/.../bin.js, so
    # recover that installation's own bin directory as well.
    selected = shutil.which(str(executable)) or str(executable)
    launcher = Path(selected).absolute()
    bin_dirs = [str(launcher.parent)]
    for ancestor in launcher.resolve().parents:
        if ancestor.name != 'node_modules' or ancestor.parent.name != 'lib':
            continue
        paired_bin = ancestor.parent.parent / 'bin'
        node = paired_bin / 'node'
        if node.is_file() and os.access(node, os.X_OK):
            if str(paired_bin) not in bin_dirs:
                bin_dirs.insert(0, str(paired_bin))
            break
    paths = env.get('PATH', os.defpath).split(os.pathsep)
    env['PATH'] = os.pathsep.join(bin_dirs + [path for path in paths if path not in bin_dirs])
    return env


@contextmanager
def locked(runtime):
    with instances.locked(runtime) as lock:
        yield lock


class Projection:
    """Whitelist state. The CLI's text events may be truncated; ignore them."""
    def __init__(self, expected_session=None, expected_policy=None):
        self.session_id = None
        self.expected_session = expected_session
        self.final = None
        self.error = None
        self.expected_policy = expected_policy
        self.tool_policy_attestation = None

    def feed(self, line):
        if self.error in ('invalid_event', 'tool_policy_violation'):
            return
        try:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError()
            kind = event.get('type')
            if kind == 'session':
                sid = event.get('sessionId')
                if not isinstance(sid, str) or not re.fullmatch(r'session-[a-zA-Z0-9-]+', sid):
                    raise ValueError()
                if self.session_id is not None or (self.expected_session and sid != self.expected_session):
                    raise ValueError()
                self.session_id = sid
            elif kind == 'final':
                text = event.get('text')
                if self.final is not None or not isinstance(text, str):
                    raise ValueError()
                self.final = text
            elif kind == 'error':
                self.error = 'dsh_error'
            elif kind == 'tool_policy_violation':
                self.error = 'tool_policy_violation'
            elif kind == 'tool_policy':
                expected = self.expected_policy or {}
                required = attestation_fields(expected, event.get('tools'))
                if (type(event.get('version')) is not int or type(event.get('schema_count')) is not int or
                        any(event.get(key) != value for key, value in required.items()) or
                        not isinstance(event.get('sessionId'), str)):
                    raise ValueError()
                self.tool_policy_attestation = required | {'session_id': event['sessionId']}
            # thinking, text, tool inputs/results and other metadata never persist.
        except (ValueError, TypeError, UnicodeError):
            self.error = 'invalid_event'

    def status(self, returncode):
        if self.error:
            return self.error
        if self.final is not None and not self.final.strip():
            return 'empty_response'
        if returncode != 0:
            return 'process_failed'
        if self.session_id is None or self.final is None:
            return 'incomplete'
        if (self.expected_policy or {}).get('tool_policy') in ('none', 'research'):
            if not self.tool_policy_attestation or self.tool_policy_attestation['session_id'] != self.session_id:
                return 'tool_policy_unverified'
        return 'completed'


def terminate(proc):
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=3)
        except ProcessLookupError:
            pass


def run_process(argv, task, cwd, env, timeout, projection, on_session, lock_fd, on_start):
    deadline = time.monotonic() + timeout
    proc = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                            start_new_session=True,
                            pass_fds=(lock_fd,) if isinstance(lock_fd, int) else tuple(lock_fd))
    # Tasks are capped by run(); concurrent I/O prevents a full stdin pipe from
    # blocking the timeout while the child is still booting.
    pending = memoryview(task)
    buffer = b''
    reason = None
    reported = False
    try:
        on_start(proc.pid)
        with selectors.DefaultSelector() as sel:
            os.set_blocking(proc.stdin.fileno(), False)
            os.set_blocking(proc.stdout.fileno(), False)
            sel.register(proc.stdin, selectors.EVENT_WRITE, 'input')
            sel.register(proc.stdout, selectors.EVENT_READ, 'output')
            while sel.get_map():
                if time.monotonic() >= deadline:
                    reason = 'timeout'
                    break
                for key, _ in sel.select(min(0.2, max(0, deadline - time.monotonic()))):
                    if key.data == 'input':
                        try:
                            sent = os.write(key.fd, pending[:65536])
                            pending = pending[sent:]
                        except BrokenPipeError:
                            pending = pending[:0]
                        if not pending:
                            sel.unregister(key.fileobj)
                            proc.stdin.close()
                    else:
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            sel.unregister(key.fileobj)
                            if buffer:
                                projection.feed(buffer)
                            buffer = b''
                        else:
                            buffer += chunk
                            while b'\n' in buffer:
                                line, buffer = buffer.split(b'\n', 1)
                                if len(line) > MAX_LINE:
                                    reason = 'event_too_large'
                                    break
                                if line:
                                    projection.feed(line)
                                    if projection.error == 'tool_policy_violation':
                                        reason = 'tool_policy_violation'
                                        break
                            if len(buffer) > MAX_LINE:
                                reason = 'event_too_large'
                                break
                    if projection.session_id and not reported:
                        on_session(projection.session_id)
                        reported = True
                if reason:
                    break
            if not reason:
                try:
                    proc.wait(timeout=max(0.01, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    reason = 'timeout'
    except KeyboardInterrupt:
        reason = 'interrupted'
    finally:
        terminate(proc)
        proc.stdin.close()
        proc.stdout.close()
    return reason or projection.status(proc.returncode), proc.returncode


def broad_cwd(target):
    root = WORKSPACE.resolve()
    runtime = RUNTIME.resolve()
    blocked = {root / name for name in BROAD_CWD}
    blocked.update([runtime, runtime / 'instances'])
    blocked.update(parent for parent in runtime.parents if parent.is_relative_to(root))
    return Path(target).resolve() in {path.resolve() for path in blocked}


def resolve_cwd(cwd, runtime, require_exists=True):
    """Resolve an invocation target without changing the process or global default."""
    default = (runtime / 'work').resolve()
    if cwd is None:
        target = default
    else:
        if not isinstance(cwd, (str, Path)) or not str(cwd):
            raise ValueError('invalid_cwd')
        target = Path(cwd).expanduser().resolve()
        if target != default and not target.is_relative_to(WORKSPACE.resolve()):
            raise ValueError('cwd_outside_workspace')
        if require_exists and broad_cwd(target):
            raise ValueError('cwd_too_broad')
    if require_exists and not target.is_dir():
        raise ValueError('cwd_not_directory')
    return target


def load_run(path, runtime):
    path = Path(path).resolve()
    if not path.is_relative_to((runtime / 'runs').resolve()):
        raise ValueError('run_outside_runtime')
    record = json.loads((path / 'result.json').read_text(encoding='utf-8'))
    source_policy.record(record)
    instance_root = instances.root(runtime, record.get('instance_id'))
    recorded = record.get('cwd')
    if not isinstance(recorded, str) or not Path(recorded).is_absolute():
        raise ValueError('invalid_cwd')
    if recorded != str(resolve_cwd(recorded, instance_root, require_exists=False)):
        raise ValueError('cwd_mismatch')
    return path, record


def resolve_dsh_modules(executable):
    """Resolve the installed dsh-tools/dsh-llm entries the trusted plugin imports.

    A plugin loaded from outside the DSH profile tree cannot resolve
    ``@deepseek-ai/*`` bare specifiers, so the host passes absolute entries.
    """
    real = Path(shutil.which(str(executable)) or str(executable)).resolve()
    if real.name != 'bin.js' or real.parent.name != 'lib':
        raise ValueError('dsh_plugin_modules_unavailable')
    package_dir = real.parent.parent
    if not (package_dir / 'package.json').is_file():
        raise ValueError('dsh_plugin_modules_unavailable')
    env = execution_env(executable)
    node = shutil.which('node', path=env.get('PATH'))
    if not node:
        raise ValueError('dsh_plugin_modules_unavailable')
    entries = []
    for name in ('@deepseek-ai/dsh-tools', '@deepseek-ai/dsh-llm'):
        try:
            out = subprocess.run(
                [node, '-e', 'process.stdout.write(require.resolve(process.argv[1]))', name],
                cwd=str(package_dir), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                check=True, timeout=30,
            ).stdout.decode('utf-8').strip()
        except (OSError, subprocess.SubprocessError, UnicodeError):
            raise ValueError('dsh_plugin_modules_unavailable') from None
        entry = Path(out)
        if not entry.is_file():
            raise ValueError('dsh_plugin_modules_unavailable')
        entries.append(str(entry))
    return tuple(entries)


def mailbox_disabled():
    """Host-owned kill switch. It can only disable communication, never enable it."""
    return os.environ.get('AGENT_MAILBOX_DISABLE', '').strip().lower() not in ('', '0', 'false')


def read_binding_file(path):
    """Read and validate one trusted private binding file."""
    candidate = Path(path)
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError('agent_mailbox_binding_invalid')
    try:
        data = json.loads(candidate.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        raise ValueError('agent_mailbox_binding_invalid') from None
    if (data.get('schema') != 1 or not isinstance(data.get('socket'), str) or
            not isinstance(data.get('token'), str) or not data.get('token') or
            not isinstance(data.get('mailbox_id'), str) or not data.get('mailbox_id')):
        raise ValueError('agent_mailbox_binding_invalid')
    return data


def resolve_host_binding_path():
    """Return the current trusted binding path, without reading the environment twice."""
    path = os.environ.get('AGENT_MAILBOX_BINDING')
    if not path and host_runtime:
        try:
            path = host_runtime.binding_for_current()
        except Exception:
            path = None
    return path or None


def mailbox_identity(runtime):
    """Return the accepted {scope, parent_id, binding_path}, confirmed through /v1/me."""
    scope = (os.environ.get('AGENT_MAILBOX_SCOPE') or '').strip()
    parent_id = (os.environ.get('AGENT_MAILBOX_PARENT_ID') or '').strip()
    binding_path = resolve_host_binding_path()
    identity = {'scope': scope, 'parent_id': parent_id, 'binding_path': ''}
    if binding_path:
        binding = read_binding_file(binding_path)
        try:
            result = host_runtime.request_unix(binding['socket'], 'GET', '/v1/me', token=binding['token'])
        except Exception:
            raise ValueError('agent_mailbox_binding_unavailable') from None
        me = result.get('mailbox') if isinstance(result, dict) else None
        if not isinstance(me, dict) or me.get('mailbox_id') != binding['mailbox_id']:
            raise ValueError('agent_mailbox_binding_mismatch')
        if not me.get('coordinator'):
            raise ValueError('agent_mailbox_parent_invalid')
        parent_scope = me.get('scope') if isinstance(me.get('scope'), str) else ''
        if scope and parent_scope and scope != parent_scope:
            raise ValueError('agent_mailbox_scope_mismatch')
        if parent_id and parent_id != me['mailbox_id']:
            raise ValueError('agent_mailbox_parent_mismatch')
        identity['scope'] = scope or parent_scope
        identity['parent_id'] = parent_id or me['mailbox_id']
        identity['binding_path'] = str(Path(binding_path).resolve())
    if not identity['scope']:
        return None
    return identity


def resolve_mailbox_plan(runtime, team, tool_policy, executable, registration_key=None):
    """Fix one task's mailbox ownership at acceptance.

    Auto-registration happens only when the service and the trusted host identity
    are both confirmed; an explicit team turns any gap into a pre-launch failure.
    The plan records the exact service and parent-binding locations so a later
    worker never has to reconstruct identity from a possibly different environment.
    """
    team = (team or '').strip()
    if tool_policy != 'default':
        if team:
            raise ValueError('team_tool_policy_conflict')
        return {'status': 'policy_incompatible'}
    if mailbox_disabled():
        if team:
            raise ValueError('agent_mailbox_service_unavailable')
        return {'status': 'disabled'}
    if not host_runtime:
        if team:
            raise ValueError('agent_mailbox_service_unavailable')
        return {'status': 'not_connected'}
    service_sock, admin_tok = host_runtime.resolve_mailbox_service(runtime)
    if not service_sock or not admin_tok or not host_runtime.check_mailbox_service(service_sock):
        if team:
            raise ValueError('agent_mailbox_service_unavailable')
        return {'status': 'not_connected'}
    plugin = resolve_dsh_plugin_path()
    if plugin is None:
        if team:
            raise ValueError('agent_mailbox_plugin_unavailable')
        return {'status': 'not_connected'}
    try:
        identity = mailbox_identity(runtime)
    except ValueError:
        if team:
            raise
        return {'status': 'not_connected'}
    if identity is None:
        if team:
            raise ValueError('agent_mailbox_identity_unavailable')
        return {'status': 'not_connected'}
    try:
        tools_module, llm_module = resolve_dsh_modules(executable)
    except ValueError:
        if team:
            raise ValueError('agent_mailbox_plugin_unavailable')
        return {'status': 'not_connected'}
    plan = {
        'status': 'planned',
        'team': team,
        'scope': identity['scope'],
        'parent_id': identity['parent_id'],
        'service_socket': str(Path(service_sock).resolve()),
        'admin_token_path': str(Path(admin_tok).resolve()),
        'parent_binding_path': identity['binding_path'],
        'plugin_sha256': digest(plugin.read_bytes()),
        'tools_module': tools_module,
        'llm_module': llm_module,
    }
    if registration_key:
        plan['registration_key'] = registration_key
    return plan


def plan_from_record(record):
    """Reconstruct the plan fixed at acceptance from a run record, without the environment."""
    if not (record or {}).get('mailbox_id'):
        return None
    return {
        'status': 'planned',
        'team': record.get('team', ''),
        'scope': record.get('mailbox_scope', ''),
        'parent_id': record.get('parent_id', ''),
        'service_socket': record.get('mailbox_service_socket', ''),
        'admin_token_path': record.get('mailbox_admin_token_path', ''),
        'parent_binding_path': record.get('mailbox_parent_binding_path', ''),
        'plugin_sha256': record.get('mailbox_plugin_sha256', ''),
        'tools_module': record.get('mailbox_tools_module', ''),
        'llm_module': record.get('mailbox_llm_module', ''),
        'registration_key': record.get('registration_key', ''),
    }


def mailbox_record_complete(record):
    """Whether a run record carries every fixed field needed to resume its mailbox.

    A mailbox recorded before these fields existed cannot be safely resumed from
    the current environment, so it fails closed instead of rebuilding identity.
    """
    for name in ('mailbox_id', 'mailbox_scope', 'mailbox_service_socket',
                 'mailbox_plugin_sha256', 'mailbox_task_dir', 'mailbox_generation',
                 'mailbox_registration_path', 'mailbox_registration_sha256'):
        if not isinstance((record or {}).get(name), str) or not (record or {}).get(name):
            return False
    return True


def reconcile_prior_generation(service_socket, token, prior_generation):
    """End this task's own previous generation before a resume activates a new one.

    The service keeps at most one active generation per mailbox, so a run that
    died without deactivating would otherwise block the next generation. Only the
    exact generation recorded for this task is ended; any other active generation
    belongs to someone else and fails closed instead of being taken over.
    """
    def current_generation():
        try:
            result = host_runtime.request_unix(service_socket, 'GET', '/v1/me', token=token)
        except Exception as exc:
            raise ValueError('agent_mailbox_generation_unconfirmed') from exc
        mailbox = result.get('mailbox') if isinstance(result, dict) else None
        if not isinstance(mailbox, dict):
            raise ValueError('agent_mailbox_generation_unconfirmed')
        return mailbox.get('generation', '') if mailbox.get('active') else ''

    active = current_generation()
    if not active:
        return
    if active != prior_generation:
        raise ValueError('agent_mailbox_generation_foreign')
    try:
        host_runtime.deactivate_mailbox(service_socket, token, prior_generation)
    except Exception:
        pass  # The re-read below decides whether it really ended.
    active = current_generation()
    if not active:
        return
    if active == prior_generation:
        raise ValueError('agent_mailbox_generation_unrecovered')
    raise ValueError('agent_mailbox_generation_foreign')


def validate_mailbox_plan(plan, executable):
    """Validate a fixed plan against the live service, fixed parent, and plugin bytes.

    This never consults the current CC environment: a resume keeps the original
    service, parent relationship, and scope even after the chat changes.
    """
    if not isinstance(plan, dict) or plan.get('status') != 'planned':
        return plan
    socket = plan.get('service_socket')
    admin = plan.get('admin_token_path')
    if (not isinstance(socket, str) or not socket or
            not isinstance(admin, str) or not admin or
            not host_runtime or not host_runtime.check_mailbox_service(socket)):
        raise ValueError('agent_mailbox_service_unavailable')
    plugin = resolve_dsh_plugin_path()
    if plugin is None or digest(plugin.read_bytes()) != plan.get('plugin_sha256'):
        raise ValueError('plugin_changed')
    tools_module, llm_module = resolve_dsh_modules(executable)
    if (tools_module, llm_module) != (plan.get('tools_module'), plan.get('llm_module')):
        raise ValueError('mailbox_plan_changed')
    binding_path = plan.get('parent_binding_path') or ''
    if binding_path:
        binding = read_binding_file(binding_path)
        try:
            result = host_runtime.request_unix(socket, 'GET', '/v1/me', token=binding['token'])
        except Exception:
            raise ValueError('agent_mailbox_binding_unavailable') from None
        me = result.get('mailbox') if isinstance(result, dict) else None
        if (not isinstance(me, dict) or me.get('mailbox_id') != plan.get('parent_id') or
                me.get('scope') != plan.get('scope') or not me.get('coordinator')):
            raise ValueError('agent_mailbox_binding_mismatch')
    return plan


def validate_mailbox(record, plan):
    """A resumed run must keep the accepted service, scope, team, parent, and plugin bytes.

    A run whose plan legitimately degraded before the model started records a
    non-registered status and has no mailbox evidence to check.
    """
    if not isinstance(plan, dict) or plan.get('status') != 'planned':
        return
    status = (record or {}).get('mailbox_status')
    if status is not None and status != 'registered':
        return
    for actual, expected in (
        ('mailbox_scope', plan.get('scope')),
        ('parent_id', plan.get('parent_id', '')),
        ('team', plan.get('team', '')),
        ('mailbox_plugin_sha256', plan.get('plugin_sha256')),
        ('mailbox_service_socket', plan.get('service_socket')),
        ('mailbox_parent_binding_path', plan.get('parent_binding_path')),
    ):
        if (record or {}).get(actual) != expected:
            raise ValueError('mailbox_binding_mismatch')


def run(task_path, resume, timeout, runtime=None, executable=None, allow_incomplete=False,
        on_created=None, cwd=None, profile=None, expected_config_hash=None,
        instance_id=instances.AUTO, reasoning_effort=None, tool_policy=None,
        before_start=None, on_started=None, model=None, sources=None, expected_source_binding=None,
        team=None, mailbox_plan=None):
    runtime = RUNTIME if runtime is None else runtime
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('invalid_timeout')
    task = Path(task_path).read_bytes()
    task.decode('utf-8')
    if not task.strip() or len(task) > 2 * 1024 * 1024:
        raise ValueError('invalid_task_size')
    executable = executable or binary()
    prior = None
    if resume:
        _, prior = load_run(resume, runtime)
        recorded_instance = prior.get('instance_id')
        if instance_id != instances.AUTO and instance_id != recorded_instance:
            raise ValueError('instance_mismatch')
        instance_id = recorded_instance
    if prior and sources is None:
        sources = source_policy.record(prior).get('read_sources')
    source_paths = source_policy.resolve(sources, WORKSPACE)
    source_fields, source_patch = source_policy.binding(source_paths)
    if prior:
        source_policy.validate(source_fields, prior)
    if source_fields and not prior and cwd is not None:
        raise ValueError('source_cwd_conflict')
    if source_fields and instance_id is None:
        raise ValueError('source_requires_private_instance')
    if expected_source_binding is not None:
        source_policy.validate(source_fields, expected_source_binding)
    input_task = source_policy.prompt(task, source_paths)
    # Reject invalid profiles before allocating a new home. Re-check at launch.
    if prior and profile is None:
        initial_profile = prior.get('profile', DEFAULT_PROFILE)
    elif profile is None:
        initial_profile = DEFAULT_PROFILE if tool_policy in ('none', 'research') else None
    else:
        initial_profile = profile
    prof_to_check = routing.resolve_default_profile(runtime, tool_policy) if initial_profile is None else initial_profile
    routing.check_profile_enabled(prof_to_check, runtime)
    initial_effort = (prior.get('reasoning_effort', DEFAULT_REASONING_EFFORT)
                      if prior and reasoning_effort is None else reasoning_effort)
    initial_model = prior.get('model', DEFAULT_MODEL) if prior and model is None else model
    initial_policy = resolve_tool_policy(prior.get('tool_policy') if prior and tool_policy is None else tool_policy)
    if source_fields and initial_policy != 'default':
        raise ValueError('source_tool_policy_conflict')
    if prior and initial_policy != resolve_tool_policy(prior.get('tool_policy')):
        raise ValueError('tool_policy_mismatch')
    if prior:
        prior_team = prior.get('team')
        if team is None:
            team = prior_team
        elif team != prior_team:
            raise ValueError('team_mismatch')
    if mailbox_plan is None:
        if resume and (prior or {}).get('mailbox_id'):
            # Resume recovers the fixed plan from the record; the current chat
            # environment must not move or re-identify the task.
            mailbox_plan = plan_from_record(prior)
        elif resume and not team:
            # Legacy runs keep their old behavior; resume never adds communication.
            mailbox_plan = {'status': 'disabled'}
        else:
            mailbox_plan = resolve_mailbox_plan(runtime, team, initial_policy, executable)
    if mailbox_disabled():
        # The host switch turns communication off for this run too; it never enables.
        mailbox_plan = {'status': 'disabled'}
    if resume and (prior or {}).get('mailbox_id') and mailbox_plan.get('status') != 'disabled':
        if not mailbox_record_complete(prior):
            raise ValueError('mailbox_record_incompatible')
        if mailbox_plan.get('status') != 'planned':
            raise ValueError('agent_mailbox_service_unavailable')
        validate_mailbox(prior, mailbox_plan)
        team = mailbox_plan.get('team', '')
    initial_profile, initial_patch = resolve_profile(initial_profile, runtime, initial_effort, initial_model)
    validate_consultation_model(initial_policy, initial_profile, resolve_reasoning_effort(initial_effort), initial_model)
    initial_config, _ = execution_config(initial_patch, initial_policy)
    if expected_config_hash is not None and expected_config_hash != initial_config['config_sha256']:
        raise ValueError('config_changed')
    instance_id = instances.prepare(runtime, instance_id)
    instance_root = instances.root(runtime, instance_id)
    target = resolve_cwd(prior['cwd'] if prior else cwd, instance_root)
    if source_fields and target != (instance_root / 'work').resolve():
        raise ValueError('source_requires_private_workspace')
    with instances.execution(runtime, instance_id, target) as lock_fds:
        sid = None
        before_seq = -1
        requested_cwd = cwd
        if resume:
            _, prior = load_run(resume, runtime)
            if prior.get('instance_id') != instance_id or prior['cwd'] != str(target):
                raise ValueError('instance_mismatch')
            # Legacy records carry no profile and implicitly used opencode-go.
            prior_profile = prior.get('profile', DEFAULT_PROFILE)
            if profile is None:
                profile = prior_profile
            elif profile != prior_profile:
                # Even identical patch bytes must not cross subscriptions.
                raise ValueError('profile_mismatch')
            prior_effort = prior.get('reasoning_effort', DEFAULT_REASONING_EFFORT)
            if reasoning_effort is None:
                reasoning_effort = prior_effort
            elif reasoning_effort != prior_effort:
                raise ValueError('reasoning_effort_mismatch')
            reasoning_effort = resolve_reasoning_effort(reasoning_effort)
            prior_model = prior.get('model', DEFAULT_MODEL)
            if model is None:
                model = prior_model
            elif model != prior_model:
                raise ValueError('model_mismatch')
            model = resolve_model(model)
            profile, patch = resolve_profile(profile, runtime, reasoning_effort, model)
            prior_policy = resolve_tool_policy(prior.get('tool_policy'))
            tool_policy = prior_policy if tool_policy is None else resolve_tool_policy(tool_policy)
            if tool_policy != prior_policy:
                raise ValueError('tool_policy_mismatch')
            config, artifacts = execution_config(patch, tool_policy)
            validate_tool_policy(prior, config)
            cwd = resolve_cwd(prior['cwd'], instance_root, require_exists=False)
            if requested_cwd is not None and resolve_cwd(requested_cwd, instance_root) != cwd:
                raise ValueError('cwd_mismatch')
            if prior.get('status') != 'completed' and not allow_incomplete:
                raise ValueError('incomplete_resume_requires_review')
            if prior.get('config_sha256') != config['config_sha256']:
                raise ValueError('config_changed')
            cwd = resolve_cwd(cwd, instance_root)
            sid = prior.get('session_id')
            if not sid:
                raise ValueError('session_unavailable')
            _, prior_rows = session_rows(sid, runtime, instance_id)
            if (not prior_rows or prior_rows[0].get('id') != sid or
                    prior_rows[0].get('cwd') != str(cwd)):
                raise ValueError('source_identity_mismatch')
            if source_fields:
                source_policy.validate(prior, source_fields)
                source_policy.exported_prompt(Path(resume), (Path(resume) / 'task.md').read_bytes(), prior)
                source_policy.native_read_only(prior_rows)
            before_seq = max(r.get('seq', -1) for r in prior_rows)
        else:
            reasoning_effort = resolve_reasoning_effort(reasoning_effort)
            model = resolve_model(model)
            if profile is None and tool_policy in ('none', 'research'):
                profile = DEFAULT_PROFILE
            profile, patch = resolve_profile(profile, runtime, reasoning_effort, model)
            tool_policy = resolve_tool_policy(tool_policy)
            config, artifacts = execution_config(patch, tool_policy)
            cwd = resolve_cwd(cwd, instance_root)
        routing.check_profile_enabled(profile, runtime)
        # An async worker fixes the accepted profile hash; re-check it under the
        # same lock that serializes launch so a change between the worker check
        # and the actual exec cannot slip past.
        validate_consultation_model(tool_policy, profile, reasoning_effort, model)
        if (config['config_sha256'] != initial_config['config_sha256'] or
                expected_config_hash is not None and expected_config_hash != config['config_sha256']):
            raise ValueError('config_changed')
        if source_fields:
            current_sources, _ = source_policy.binding(source_policy.resolve(source_paths, WORKSPACE))
            source_policy.validate(current_sources, source_fields)
        home = instance_root / 'home'
        home.mkdir(exist_ok=True, mode=0o700)
        # A scheduler may reserve a start only after all execution leases and
        # identity checks pass. A refusal here has not created or launched a run.
        if before_start:
            before_start()
        dest = runtime / 'runs' / (datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:8])
        dest.mkdir(parents=True, mode=0o700)
        (dest / 'task.md').write_bytes(task)
        record = {'status': 'running', 'started': now(), 'session_id': sid,
                  'cwd': str(cwd.resolve()), 'profile': profile, 'patch': patch.name,
                  'reasoning_effort': reasoning_effort, 'model': model,
                  **config,
                  'task_sha256': digest(task), 'source_before_seq': before_seq,
                  'resume_from': str(resume) if resume else None}
        record.update(source_fields)
        if source_fields:
            (dest / 'prompt.md').write_bytes(input_task)
            (dest / 'read-sources.patch.yml').write_bytes(source_patch)
            record['prompt_sha256'] = digest(input_task)
        # One logical task, including children and continuation turns, shares a
        # probe budget. Keep the original task bytes/hash and visible transcript.
        record['research_task_id'] = (prior.get('research_task_id', Path(resume).name)
                                      if prior else dest.name)
        if instance_id is not None:
            record['instance_id'] = instance_id
        launch_patch, policy_overlay = patch, None
        if tool_policy != 'default':
            launch_patch, policy_overlay = snapshot_tool_policy(dest, artifacts, record)
        mailbox_active = False
        mailbox_token = None
        mailbox_gen = None
        service_sock = None
        record['mailbox_status'] = mailbox_plan.get('status', 'not_connected')
        root_identity_file = dest / 'mailbox-root.json'
        if mailbox_plan.get('status') == 'planned':
            # Validate the fixed plan against the saved service/parent/plugin
            # evidence; never reconstruct identity from the current environment.
            try:
                mailbox_plan = validate_mailbox_plan(mailbox_plan, executable)
            except ValueError:
                # A previously connected task must not silently lose its mailbox.
                if team or (prior and prior.get('mailbox_id')):
                    raise
                mailbox_plan = {'status': 'not_connected'}
                record['mailbox_status'] = 'not_connected'
        if mailbox_plan.get('status') == 'planned':
            service_sock = Path(mailbox_plan['service_socket'])
            admin_tok = Path(mailbox_plan['admin_token_path'])
            dsh_plugin = resolve_dsh_plugin_path()
            if dsh_plugin is None:
                raise ValueError('agent_mailbox_plugin_unavailable')
            tools_module = mailbox_plan['tools_module']
            llm_module = mailbox_plan['llm_module']
            reg_key = ((prior.get('registration_key') if prior else None) or
                       mailbox_plan.get('registration_key') or f'dsh-run:{dest.name}')
            # The registration root is fixed at the first run and reused by every
            # later resume, so the original request and token survive. The host
            # stores it as reg-<sha256(registration_key)[:16]>.json.
            task_dir = Path((prior or {}).get('mailbox_task_dir') or dest)
            label = ((prior or {}).get('mailbox_label')) or f'hermes-{dest.name}'
            safe_name = hashlib.sha256(reg_key.encode('utf-8')).hexdigest()[:16]
            registration_file = task_dir / f'reg-{safe_name}.json'
            if prior and prior.get('mailbox_id'):
                if Path(prior.get('mailbox_registration_path', '')).resolve() != registration_file.resolve():
                    raise ValueError('mailbox_registration_path_mismatch')
                if registration_file.is_symlink() or not registration_file.is_file():
                    raise ValueError('mailbox_registration_missing')
                if digest(registration_file.read_bytes()) != prior.get('mailbox_registration_sha256'):
                    raise ValueError('mailbox_registration_changed')
            mbx_info, mailbox_token = host_runtime.register_task_mailbox(
                service_sock,
                admin_tok,
                reg_key,
                scope=mailbox_plan['scope'],
                team=mailbox_plan.get('team', ''),
                label=label,
                kind='hermes',
                parent_id=mailbox_plan.get('parent_id', ''),
                task_dir=task_dir,
            )
            if prior and prior.get('mailbox_id') and mbx_info.get('mailbox_id') != prior['mailbox_id']:
                raise ValueError('mailbox_binding_mismatch')
            if prior and prior.get('mailbox_id'):
                # The previous generation is ended from the durable record and the
                # task's own token, not from a binding file that may be gone.
                reconcile_prior_generation(service_sock, mailbox_token, prior['mailbox_generation'])
            mailbox_gen = secrets.token_hex(16)
            binding_file = dest / 'mailbox-binding.json'
            host_runtime.create_binding_file(
                binding_file, service_sock, mailbox_token, mbx_info['mailbox_id'], mailbox_gen)
            snapshot = dest / 'dsh-plugin.mjs'
            snapshot.write_bytes(dsh_plugin.read_bytes())
            if digest(snapshot.read_bytes()) != mailbox_plan['plugin_sha256']:
                raise ValueError('plugin_changed')
            mailbox_patch = (
                '- insert:\n'
                '    - id: dsh-mailbox-plugin\n'
                f'      name: {json.dumps(str(snapshot))}\n'
                '      config:\n'
                f'        bindingFile: {json.dumps(str(binding_file))}\n'
                f'        rootIdentityFile: {json.dumps(str(root_identity_file))}\n'
                f'        stateDir: {json.dumps(str(task_dir))}\n'
                f'        toolsModule: {json.dumps(tools_module)}\n'
                f'        llmModule: {json.dumps(llm_module)}\n'
            )
            (dest / 'mailbox.patch.yml').write_text(mailbox_patch, encoding='utf-8')
            mailbox_active = True
            record['mailbox_status'] = 'registered'
            record['team'] = mailbox_plan.get('team', '')
            record['mailbox_id'] = mbx_info['mailbox_id']
            record['mailbox_scope'] = mailbox_plan['scope']
            record['parent_id'] = mailbox_plan.get('parent_id', '')
            record['registration_key'] = reg_key
            record['mailbox_label'] = label
            record['mailbox_generation'] = mailbox_gen
            record['mailbox_service_socket'] = str(service_sock)
            record['mailbox_admin_token_path'] = str(admin_tok)
            record['mailbox_parent_binding_path'] = mailbox_plan.get('parent_binding_path', '')
            record['mailbox_tools_module'] = tools_module
            record['mailbox_llm_module'] = llm_module
            record['mailbox_plugin_sha256'] = mailbox_plan['plugin_sha256']
            record['mailbox_binding_sha256'] = digest(binding_file.read_bytes())
            record['mailbox_patch_sha256'] = digest((dest / 'mailbox.patch.yml').read_bytes())
            record['mailbox_task_dir'] = str(task_dir)
            record['mailbox_registration_path'] = str(registration_file)
            record['mailbox_registration_sha256'] = (digest(registration_file.read_bytes())
                                                     if registration_file.is_file() else '')
        if mailbox_active and sid:
            # A resumed run already knows the exact native session id.
            write_json(root_identity_file, {'schema': 1, 'sessionId': sid, 'cwd': str(cwd.resolve())})
        write_json(dest / 'result.json', record)
        print(json.dumps({'run_dir': str(dest), 'status': 'running'}), flush=True)
        if on_created:
            on_created(dest)
        projection = Projection(sid, config)
        def on_session(value):
            record['session_id'] = value
            write_json(dest / 'result.json', record)
            if mailbox_active:
                # Publish the exact native root id; the plugin binds precisely to it.
                write_json(root_identity_file, {'schema': 1, 'sessionId': value, 'cwd': str(cwd.resolve())})
        def on_start(pid):
            record['child_pid'] = pid
            write_json(dest / 'result.json', record)
            if on_started:
                on_started(pid)
        argv = [executable, 'headless', '--patch', str(launch_patch)]
        if policy_overlay is not None:
            argv.extend(['--patch', str(policy_overlay)])
        if source_fields:
            argv.extend(['--patch', str(dest / 'read-sources.patch.yml')])
        if mailbox_active:
            argv.extend(['--patch', str(dest / 'mailbox.patch.yml')])
        argv.append('--json')
        if sid:
            argv.extend(['--session-id', sid])
        env = execution_env(executable)
        # The model only ever sees its own binding, never the parent Atlas one.
        for name in ('AGENT_MAILBOX_BINDING', 'AGENT_MAILBOX_SCOPE', 'AGENT_MAILBOX_PARENT_ID'):
            env.pop(name, None)
        env['DSH_HOME'] = str(home)
        if source_fields:
            env['DSH_PERMISSION_MODE'] = 'read-only'
        env['DSH_RESEARCH_RUN_ID'] = record['research_task_id']
        env['DSH_RESEARCH_PROFILE'] = profile
        if tool_policy != 'default':
            env['DSH_TOOLS_MODE'] = 'native'
            env['DSH_TELEMETRY_DISABLED'] = '1'
        if mailbox_active:
            env['AGENT_MAILBOX_BINDING'] = str(dest / 'mailbox-binding.json')
        try:
            status, code = run_process(argv, input_task, cwd, env, timeout, projection, on_session,
                                       lock_fds, on_start)
        except OSError:
            status, code = 'launch_failed', None
        finally:
            if mailbox_active and mailbox_token and mailbox_gen and host_runtime and service_sock:
                try:
                    host_runtime.deactivate_mailbox(service_sock, mailbox_token, mailbox_gen)
                except Exception:
                    pass
        if status != 'completed' and record.get('session_id'):
            record['stream_status'] = status
            try:
                _, native_rows = session_rows(record['session_id'], runtime, instance_id)
                details = native_run_diagnostic(record, native_rows, input_task)
                record['native_outcome'] = details
                if (status in ('empty_response', 'process_failed', 'dsh_error') and
                        details['status'] != 'completed'):
                    status = details['status']
            except (OSError, ValueError, TypeError, KeyError, subprocess.SubprocessError):
                record['native_outcome'] = {'status': 'unavailable'}
        if status == 'completed' and source_fields:
            try:
                _, native_rows = session_rows(record['session_id'], runtime, instance_id)
                source_policy.native_read_only(native_rows)
                source_policy.exported_prompt(dest, task, record)
            except (OSError, ValueError, TypeError, KeyError):
                status = 'failed'
                record['code'] = 'source_verification_failed'
        record.update(status=status, returncode=code, ended=now())
        if projection.tool_policy_attestation is not None:
            record['tool_policy_attestation'] = projection.tool_policy_attestation
        if status == 'completed':
            (dest / 'response.md').write_text(projection.final, encoding='utf-8')
            record['response_sha256'] = digest(projection.final.encode())
        write_json(dest / 'result.json', record)
        print(json.dumps({'run_dir': str(dest), 'status': status, 'session_id': record['session_id']}))
        return dest, status


def visible_messages(rows):
    messages = []
    for row in rows:
        kind = row.get('type')
        if kind not in ('user/message', 'assistant/message'):
            continue
        data = row['data'] if kind == 'user/message' else row['data']['message']
        if kind == 'user/message' and data.get('source', {}).get('kind') != 'user':
            continue
        for index, block in enumerate(data['content']):
            if block.get('type') == 'text' and block.get('text'):
                messages.append({'seq': row['seq'], 'block': index,
                                 'speaker': 'Codex' if kind == 'user/message' else 'DSH',
                                 'text': block['text']})
    return messages


def render_transcript(messages):
    result = '# Codex 与 DSH 完整可见会话\n\n仅含双方text正文；不含内部推理、系统上下文和工具日志。消息头与边界标记为导出器添加。\n'
    for m in messages:
        key = f'{m["seq"]}:{m["block"]}'
        result += f'\n## {m["speaker"]} · {key}\n\n<!-- BEGIN {key} -->\n' + m['text'] + f'\n<!-- END {key} -->\n'
    return result


def session_rows(sid, runtime, instance_id=None):
    if not isinstance(sid, str) or not re.fullmatch(r'session-[a-zA-Z0-9-]+', sid):
        raise ValueError('invalid_session')
    home = instances.root(runtime, instance_id) / 'home'
    matches = list((home / 'sessions').glob('*/' + sid + '/session.v4.jsonl.zstd'))
    if len(matches) != 1 or not shutil.which('zstd'):
        raise ValueError('source_unavailable')
    source = matches[0]
    if not source.resolve().is_relative_to(home.resolve()):
        raise ValueError('source_outside_runtime')
    raw = subprocess.run(['zstd', '-dc', str(source)], stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, check=True, timeout=30).stdout
    return source, [json.loads(line) for line in raw.splitlines() if line.strip()]


def native_run_diagnostic(record, rows, task):
    """Read one exact turn; never persist prompts, reasoning or raw error text."""
    if (not rows or not isinstance(rows[0], dict) or rows[0].get('type') != 'session' or
            rows[0].get('id') != record.get('session_id') or
            not isinstance(rows[0].get('cwd'), str) or not isinstance(record.get('cwd'), str) or
            Path(rows[0]['cwd']).resolve() != Path(record['cwd']).resolve()):
        raise ValueError('source_identity_mismatch')
    before = record.get('source_before_seq')
    if type(before) is not int or before < -1:
        raise ValueError('source_cursor_invalid')
    start = None
    accepted = False
    previous_seq = -1
    cursor_found = before == -1
    content = [{'type': 'text', 'text': task.decode('utf-8')}]
    for index, row in enumerate(rows[1:], 1):
        if not isinstance(row, dict) or not isinstance(row.get('data'), dict):
            raise ValueError('native_event_invalid')
        seq = row.get('seq')
        if type(seq) is not int or seq != previous_seq + 1:
            raise ValueError('native_sequence_invalid')
        previous_seq = seq
        if seq == before:
            cursor_found = True
        if seq <= before:
            continue
        if not cursor_found:
            raise ValueError('source_cursor_invalid')
        kind, data = row.get('type'), row.get('data', {})
        if kind == 'turn/start':
            if start is not None or type(data.get('turn')) is not int:
                raise ValueError('native_turn_incomplete')
            start = row
        if start is None:
            continue
        if kind == 'user/message':
            source = data.get('source')
            if not isinstance(source, dict):
                raise ValueError('native_event_invalid')
            if source.get('kind') == 'user':
                if accepted or data.get('content') != content:
                    raise ValueError('task_not_in_source')
                accepted = True
        if kind == 'turn/end':
            if not accepted or data.get('turn') != start['data']['turn']:
                raise ValueError('native_turn_mismatch')
            break
    else:
        raise ValueError('native_turn_incomplete')

    reason = data.get('reason', {})
    if not isinstance(reason, dict):
        raise ValueError('native_reason_unknown')
    kind = reason.get('kind')
    statuses = {'completed': 'completed', 'max-tokens': 'max_tokens',
                'error': 'native_error', 'aborted': 'native_cancelled',
                'cancelled': 'native_cancelled'}
    if not isinstance(kind, str) or kind not in statuses:
        raise ValueError('native_reason_unknown')
    details = {'status': statuses[kind], 'reason': kind,
               'turn': start['data']['turn'], 'start_seq': start['seq'], 'end_seq': seq,
               'source_prefix_sha256': digest(json.dumps(rows[:index + 1], sort_keys=True,
                    ensure_ascii=False, separators=(',', ':')).encode('utf-8'))}
    if kind == 'error':
        error = reason.get('error', {})
        if not isinstance(error, dict):
            raise ValueError('native_event_invalid')
        code = error.get('code')
        if not isinstance(code, str):
            raise ValueError('native_event_invalid')
        allowed = {'CONTEXT_WINDOW_EXCEEDED': 'context_window_exceeded',
                   'QUOTA': 'quota_exceeded', 'QUOTA_EXCEEDED': 'quota_exceeded',
                   'AUTH': 'authentication_failed', 'RATE_LIMIT': 'rate_limited',
                   'SERVER': 'provider_error', 'TIMEOUT': 'provider_timeout',
                   'TRANSPORT': 'transport_error', 'EMPTY_RESPONSE': 'empty_response',
                   'INVALID_REQUEST': 'invalid_request', 'PI_AI_ERROR': 'native_error'}
        details['status'] = allowed.get(code, 'native_error')
        details['error_code'] = code if code in allowed else 'OTHER'
        message = error.get('message', '')
        match = re.search(r'\b([45][0-9]{2}) status code\b', message) if isinstance(message, str) else None
        if match:
            details['http_status'] = int(match.group(1))
    for row in rows[1:index + 1]:
        data = row.get('data', {})
        if row.get('type') == 'request/context':
            capacity = data.get('contextWindow')
            if type(capacity) is int and capacity > 0:
                details['context_window'] = capacity
        elif row.get('type') == 'assistant/message' and row['seq'] >= start['seq']:
            message = data.get('message', {})
            usage = data.get('usage', {})
            if not isinstance(message, dict) or not isinstance(usage, dict):
                raise ValueError('native_event_invalid')
            blocks = message.get('content', [])
            if (not isinstance(blocks, list) or any(not isinstance(block, dict) for block in blocks) or
                    any(not isinstance(block.get('text'), str) for block in blocks if block.get('type') == 'text')):
                raise ValueError('native_event_invalid')
            details['usage'] = {key: usage[key] for key in
                ('inputTokens', 'outputTokens', 'totalTokens', 'cacheReadTokens', 'cacheWriteTokens')
                if type(usage.get(key)) is int and usage[key] >= 0}
            details['visible_text_bytes'] = sum(len(block['text'].encode('utf-8'))
                for block in blocks if block.get('type') == 'text')
    return details


def diagnose_run(path, runtime=None):
    """Inspect historical evidence without resuming or rewriting a run."""
    runtime = RUNTIME if runtime is None else runtime
    _, initial = load_run(path, runtime)
    with locked(instances.root(runtime, initial.get('instance_id'))):
        dest, record = load_run(path, runtime)
        task = (dest / 'task.md').read_bytes()
        if digest(task) != record.get('task_sha256'):
            raise ValueError('artifact_modified')
        task = source_policy.exported_prompt(dest, task, record)
        _, rows = session_rows(record['session_id'], runtime, record.get('instance_id'))
        return {'recorded_status': record['status'], 'returncode': record.get('returncode'),
                'session_id': record['session_id'],
                'native_outcome': native_run_diagnostic(record, rows, task)}


def export_run(path, runtime=None):
    runtime = RUNTIME if runtime is None else runtime
    _, initial = load_run(path, runtime)
    instance_id = initial.get('instance_id')
    with locked(instances.root(runtime, instance_id)):
        dest, record = load_run(path, runtime)
        if record.get('instance_id') != instance_id:
            raise ValueError('instance_mismatch')
        if record.get('status') != 'completed':
            raise ValueError('run_not_completed')
        sid = record['session_id']
        source, rows = session_rows(sid, runtime, instance_id)
        header = rows[0]
        if header.get('id') != sid or Path(header.get('cwd', '')).resolve() != Path(record['cwd']):
            raise ValueError('source_identity_mismatch')
        messages = visible_messages(rows)
        task = (dest / 'task.md').read_bytes()
        final = (dest / 'response.md').read_bytes()
        if digest(task) != record['task_sha256'] or digest(final) != record['response_sha256']:
            raise ValueError('artifact_modified')
        # The first task after the pre-run cursor disambiguates identical tasks
        # repeated in later turns. Never include later turns in an old export.
        input_task = source_policy.exported_prompt(dest, task, record)
        starts = [i for i, m in enumerate(messages) if m['seq'] > record['source_before_seq'] and m['speaker'] == 'Codex' and m['text'].strip() == input_task.decode().strip()]
        if not starts:
            raise ValueError('task_not_in_source')
        start = starts[0]
        end = len(messages)
        for i in range(start + 1, len(messages)):
            if messages[i]['speaker'] == 'Codex':
                end = i
                break
        turn_messages = messages[start + 1:end]
        if not turn_messages:
            raise ValueError('final_not_in_source')
        last_seq = turn_messages[-1]['seq']
        last_text = ''.join(m['text'] for m in turn_messages if m['seq'] == last_seq)
        if last_text != final.decode():
            raise ValueError('final_not_in_source')
        messages = messages[:end]
        transcript = render_transcript(messages)
        manifest = {'session_id': sid, 'status': 'verified', 'exported': now(),
                    'source_sha256': digest(source.read_bytes()),
                    'transcript_sha256': digest(transcript.encode()),
                    'messages': [{k: v for k, v in m.items() if k != 'text'} | {'sha256': digest(m['text'].encode())} for m in messages]}
        if instance_id is not None:
            manifest['instance_id'] = instance_id
        exports = dest / 'exports'
        exports.mkdir(exist_ok=True)
        target = exports / uuid.uuid4().hex
        manifest['export_dir'] = str(target)
        # Publish the pair atomically. A crash can leave a .partial directory,
        # never a completed export; retry creates another version without a call.
        with tempfile.TemporaryDirectory(prefix='.partial-', dir=exports) as staging:
            stage = Path(staging)
            (stage / 'transcript.md').write_text(transcript, encoding='utf-8')
            write_json(stage / 'transcript-manifest.json', manifest)
            stage.rename(target)
        print(json.dumps({'status': 'verified', 'messages': len(messages), 'export_dir': str(target)}))
        return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    doc = sub.add_parser('doctor')
    doc.add_argument('--profile', help='Check a fixed subscription: opencode-go (default), happy2 or happy1')
    doc.add_argument('--reasoning-effort', choices=REASONING_EFFORTS)
    doc.add_argument('--model', choices=MODEL_PATCHES)
    call = sub.add_parser('run')
    call.add_argument('--task', required=True)
    call.add_argument('--resume')
    call.add_argument('--cwd', help='Existing workspace directory; default: dedicated work directory, or recorded directory on resume')
    call.add_argument('--source', action='append', help='Read-only material path; repeatable, uses a private execution directory')
    call.add_argument('--timeout', type=float, default=300)
    call.add_argument('--resume-incomplete', action='store_true', help='Only after reviewing a failed/interrupted run; never auto-retry')
    call.add_argument('--profile', help='Fixed subscription: opencode-go (default), happy2 or happy1')
    call.add_argument('--reasoning-effort', choices=REASONING_EFFORTS,
                      help='Task reasoning effort: high by default; inherit on resume')
    call.add_argument('--model', choices=MODEL_PATCHES,
                      help='Task model: DeepSeek by default; GLM requires explicit Happy selection; inherit on resume')
    call.add_argument('--tool-policy', choices=TOOL_POLICIES,
                      help='none: explicit Go/Happy (DeepSeek/High), implicit none uses Go; research: Go/High public search and fetch; inherit on resume')
    call.add_argument('--team', help='Collaboration group identifier for shared tasks')
    export = sub.add_parser('export')
    export.add_argument('--run', required=True)
    diagnostic = sub.add_parser('diagnose', help='只读核对指定运行的原生结束原因，不重试任务')
    diagnostic.add_argument('--run', required=True)
    args = parser.parse_args()
    try:
        scope()
        if args.command == 'doctor':
            routing_config = routing.load_routing(RUNTIME)
            requested = args.profile
            doctor_profile = requested or routing_config['default_profile']
            try:
                _, patch = resolve_profile(requested, RUNTIME, args.reasoning_effort, args.model)
            except ValueError:
                patch_ready = False
            else:
                patch_ready = patch.is_file()
            try:
                executable = binary()
                launch_env = execution_env(executable)
                dsh_ready = True
            except ValueError:
                launch_env, dsh_ready = os.environ, False
            profile_enabled = doctor_profile not in routing_config['disabled_profiles']
            checks = {'dsh': dsh_ready,
                      'node': bool(shutil.which('node', path=launch_env.get('PATH'))),
                      'zstd': bool(shutil.which('zstd')),
                      'patch': patch_ready,
                      'profile_enabled': profile_enabled}
            print(json.dumps({'checks': checks, 'profile': doctor_profile,
                              'default_profile': routing_config['default_profile'],
                              'disabled_profiles': sorted(routing_config['disabled_profiles']),
                              'reasoning_effort': resolve_reasoning_effort(args.reasoning_effort),
                              'model': resolve_model(args.model),
                              'network_tested': False, 'auth_tested': False}))
            return 0 if all(checks.values()) else 1
        if args.command == 'run':
            if args.resume_incomplete and not args.resume:
                raise ValueError('resume_required')
            _, status = run(args.task, args.resume, args.timeout,
                            allow_incomplete=args.resume_incomplete, cwd=args.cwd,
                            profile=args.profile, reasoning_effort=args.reasoning_effort,
                            tool_policy=args.tool_policy, model=args.model, sources=args.source,
                            team=args.team)
            return 0 if status == 'completed' else 1
        if args.command == 'diagnose':
            print(json.dumps(diagnose_run(args.run), ensure_ascii=False))
        else:
            export_run(args.run)
        return 0
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        # Do not echo paths or exception messages that could include task data.
        safe = str(exc) if isinstance(exc, ValueError) and str(exc).replace('_', '').isalpha() else type(exc).__name__
        print(json.dumps({'status': 'failed', 'code': safe}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
