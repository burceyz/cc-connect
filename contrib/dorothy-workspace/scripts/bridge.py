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

import instances

WORKSPACE = Path('/Users/yz/WorkSpace/cc-connect-demo')
RUNTIME = WORKSPACE / '.local/dsh-collaboration'
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
# Friendly workspace profile names need not match the native provider route.
PROFILE_PROVIDERS = {DEFAULT_PROFILE: DEFAULT_PROFILE, 'happy2': 'happy2', 'happy1': 'happy'}
MAX_LINE = 16 * 1024 * 1024


def digest(data):
    return hashlib.sha256(data).hexdigest()


def resolve_reasoning_effort(reasoning_effort=None):
    if reasoning_effort is None:
        return DEFAULT_REASONING_EFFORT
    if reasoning_effort not in REASONING_EFFORTS:
        raise ValueError('unknown_reasoning_effort')
    return reasoning_effort


def resolve_profile(profile=None, runtime=RUNTIME, reasoning_effort=None):
    """Return (profile, patch path) for a fixed subscription mapping.

    ``None`` is the compatibility default used by callers that predate the
    per-invocation ``profile`` option; it selects opencode-go. An explicit
    unknown profile or a missing patch is rejected here, before any model
    process is launched. Only the default keeps the legacy FileNotFoundError
    surfaced by the subsequent patch read.
    """
    reasoning_effort = resolve_reasoning_effort(reasoning_effort)
    if profile is None and reasoning_effort == DEFAULT_REASONING_EFFORT:
        return DEFAULT_PROFILE, runtime / PATCH_NAME
    if profile is None:
        profile = DEFAULT_PROFILE
    if not isinstance(profile, str) or profile not in PROFILE_PATCHES:
        raise ValueError('unknown_profile')
    patches = MAX_PROFILE_PATCHES if reasoning_effort == 'max' else PROFILE_PATCHES
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
    def __init__(self, expected_session=None):
        self.session_id = None
        self.expected_session = expected_session
        self.final = None
        self.error = None

    def feed(self, line):
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
                if self.final is not None or not isinstance(text, str) or not text.strip():
                    raise ValueError()
                self.final = text
            elif kind == 'error':
                self.error = 'dsh_error'
            # thinking, text, tool inputs/results and other metadata never persist.
        except (ValueError, TypeError, UnicodeError):
            self.error = 'invalid_event'

    def status(self, returncode):
        if self.error:
            return self.error
        if returncode != 0:
            return 'process_failed'
        if self.session_id is None or self.final is None:
            return 'incomplete'
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
    if require_exists and not target.is_dir():
        raise ValueError('cwd_not_directory')
    return target


def load_run(path, runtime):
    path = Path(path).resolve()
    if not path.is_relative_to((runtime / 'runs').resolve()):
        raise ValueError('run_outside_runtime')
    record = json.loads((path / 'result.json').read_text(encoding='utf-8'))
    instance_root = instances.root(runtime, record.get('instance_id'))
    recorded = record.get('cwd')
    if not isinstance(recorded, str) or not Path(recorded).is_absolute():
        raise ValueError('invalid_cwd')
    if recorded != str(resolve_cwd(recorded, instance_root, require_exists=False)):
        raise ValueError('cwd_mismatch')
    return path, record


def run(task_path, resume, timeout, runtime=RUNTIME, executable=None, allow_incomplete=False,
        on_created=None, cwd=None, profile=None, expected_config_hash=None,
        instance_id=instances.AUTO, reasoning_effort=None):
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
    # Reject invalid profiles before allocating a new home. Re-check at launch.
    initial_profile = prior.get('profile', DEFAULT_PROFILE) if prior and profile is None else profile
    initial_effort = (prior.get('reasoning_effort', DEFAULT_REASONING_EFFORT)
                      if prior and reasoning_effort is None else reasoning_effort)
    _, initial_patch = resolve_profile(initial_profile, runtime, initial_effort)
    initial_hash = digest(initial_patch.read_bytes())
    if expected_config_hash is not None and expected_config_hash != initial_hash:
        raise ValueError('config_changed')
    instance_id = instances.prepare(runtime, instance_id)
    instance_root = instances.root(runtime, instance_id)
    target = resolve_cwd(prior['cwd'] if prior else cwd, instance_root)
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
            profile, patch = resolve_profile(profile, runtime, reasoning_effort)
            patch_hash = digest(patch.read_bytes())
            cwd = resolve_cwd(prior['cwd'], instance_root, require_exists=False)
            if requested_cwd is not None and resolve_cwd(requested_cwd, instance_root) != cwd:
                raise ValueError('cwd_mismatch')
            if prior.get('status') != 'completed' and not allow_incomplete:
                raise ValueError('incomplete_resume_requires_review')
            if prior.get('config_sha256') != patch_hash:
                raise ValueError('config_changed')
            cwd = resolve_cwd(cwd, instance_root)
            sid = prior.get('session_id')
            if not sid:
                raise ValueError('session_unavailable')
            _, prior_rows = session_rows(sid, runtime, instance_id)
            if (not prior_rows or prior_rows[0].get('id') != sid or
                    prior_rows[0].get('cwd') != str(cwd)):
                raise ValueError('source_identity_mismatch')
            before_seq = max(r.get('seq', -1) for r in prior_rows)
        else:
            reasoning_effort = resolve_reasoning_effort(reasoning_effort)
            profile, patch = resolve_profile(profile, runtime, reasoning_effort)
            patch_hash = digest(patch.read_bytes())
            cwd = resolve_cwd(cwd, instance_root)
        # An async worker fixes the accepted profile hash; re-check it under the
        # same lock that serializes launch so a change between the worker check
        # and the actual exec cannot slip past.
        if expected_config_hash is not None and expected_config_hash != patch_hash:
            raise ValueError('config_changed')
        home = instance_root / 'home'
        home.mkdir(exist_ok=True, mode=0o700)
        dest = runtime / 'runs' / (datetime.now().strftime('%Y%m%d-%H%M%S-') + uuid.uuid4().hex[:8])
        dest.mkdir(parents=True, mode=0o700)
        (dest / 'task.md').write_bytes(task)
        record = {'status': 'running', 'started': now(), 'session_id': sid,
                  'cwd': str(cwd.resolve()), 'profile': profile, 'patch': patch.name,
                  'reasoning_effort': reasoning_effort,
                  'config_sha256': patch_hash,
                  'task_sha256': digest(task), 'source_before_seq': before_seq,
                  'resume_from': str(resume) if resume else None}
        # One logical task, including children and continuation turns, shares a
        # probe budget. Keep the original task bytes/hash and visible transcript.
        record['research_task_id'] = (prior.get('research_task_id', Path(resume).name)
                                      if prior else dest.name)
        if instance_id is not None:
            record['instance_id'] = instance_id
        write_json(dest / 'result.json', record)
        print(json.dumps({'run_dir': str(dest), 'status': 'running'}), flush=True)
        if on_created:
            on_created(dest)
        projection = Projection(sid)
        def on_session(value):
            record['session_id'] = value
            write_json(dest / 'result.json', record)
        def on_start(pid):
            record['child_pid'] = pid
            write_json(dest / 'result.json', record)
        argv = [executable, 'headless', '--patch', str(patch), '--json']
        if sid:
            argv.extend(['--session-id', sid])
        env = execution_env(executable)
        env['DSH_HOME'] = str(home)
        env['DSH_RESEARCH_RUN_ID'] = record['research_task_id']
        env['DSH_RESEARCH_PROFILE'] = profile
        try:
            status, code = run_process(argv, task, cwd, env, timeout, projection, on_session,
                                       lock_fds, on_start)
        except OSError:
            status, code = 'launch_failed', None
        record.update(status=status, returncode=code, ended=now())
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
    if not re.fullmatch(r'session-[a-zA-Z0-9-]+', sid):
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


def export_run(path, runtime=RUNTIME):
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
        starts = [i for i, m in enumerate(messages) if m['seq'] > record['source_before_seq'] and m['speaker'] == 'Codex' and m['text'].strip() == task.decode().strip()]
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
    call = sub.add_parser('run')
    call.add_argument('--task', required=True)
    call.add_argument('--resume')
    call.add_argument('--cwd', help='Existing workspace directory; default: dedicated work directory, or recorded directory on resume')
    call.add_argument('--timeout', type=float, default=300)
    call.add_argument('--resume-incomplete', action='store_true', help='Only after reviewing a failed/interrupted run; never auto-retry')
    call.add_argument('--profile', help='Fixed subscription: opencode-go (default), happy2 or happy1')
    call.add_argument('--reasoning-effort', choices=REASONING_EFFORTS,
                      help='Task reasoning effort: high by default; inherit on resume')
    export = sub.add_parser('export')
    export.add_argument('--run', required=True)
    args = parser.parse_args()
    try:
        scope()
        if args.command == 'doctor':
            requested = args.profile
            doctor_profile = requested or DEFAULT_PROFILE
            try:
                _, patch = resolve_profile(requested, RUNTIME, args.reasoning_effort)
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
            checks = {'dsh': dsh_ready,
                      'node': bool(shutil.which('node', path=launch_env.get('PATH'))), 'zstd': bool(shutil.which('zstd')),
                      'patch': patch_ready}
            print(json.dumps({'checks': checks, 'profile': doctor_profile,
                              'reasoning_effort': resolve_reasoning_effort(args.reasoning_effort),
                              'network_tested': False, 'auth_tested': False}))
            return 0 if all(checks.values()) else 1
        if args.command == 'run':
            if args.resume_incomplete and not args.resume:
                raise ValueError('resume_required')
            _, status = run(args.task, args.resume, args.timeout,
                            allow_incomplete=args.resume_incomplete, cwd=args.cwd,
                            profile=args.profile, reasoning_effort=args.reasoning_effort)
            return 0 if status == 'completed' else 1
        export_run(args.run)
        return 0
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as exc:
        # Do not echo paths or exception messages that could include task data.
        safe = str(exc) if isinstance(exc, ValueError) and str(exc).replace('_', '').isalpha() else type(exc).__name__
        print(json.dumps({'status': 'failed', 'code': safe}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
