#!/usr/bin/env python3
"""Local async DSH jobs with optional DingTalk completion notices; no task retries."""
import argparse
from contextlib import redirect_stdout
import fcntl
import io
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import uuid

import bridge
import instances
import notify
import ipc

ACTIVE = {'accepted', 'running'}


def job_path(runtime, key):
    if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}', key):
        raise ValueError('invalid_job_id')
    dest = runtime / 'jobs' / key
    if not dest.resolve().is_relative_to((runtime / 'jobs').resolve()):
        raise ValueError('job_outside_runtime')
    return dest


def lock_probe(path):
    """Observe without creating/removing a lock file or waiting on another job."""
    try:
        with path.open('rb') as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return 'held'
            return 'released'
    except OSError:
        return 'unavailable'


def pid_probe(pid, group=False):
    # Signal zero does not terminate anything. A reused pid or zombie can exist;
    # existence alone never establishes ownership, health, or permission to kill.
    if type(pid) is not int or pid <= 1:
        return 'not_recorded'
    try:
        (os.killpg if group else os.kill)(pid, 0)
        return 'exists_unverified'
    except ProcessLookupError:
        return 'absent'
    except (OSError, OverflowError):
        return 'unavailable'


def cleanup_status(record, dest, runtime):
    run = {}
    if record.get('run_dir'):
        try:
            _, run = bridge.load_run(record['run_dir'], runtime)
        except (OSError, ValueError, TypeError):
            pass
    instance_id = record.get('instance_id')
    instance_root = instances.root(runtime, instance_id)
    report = {
        'checked_at': bridge.now(),
        'scope': 'recorded_worker_dsh_pid_and_dsh_process_group',
        'worker_lock': lock_probe(dest / 'worker.lock'),
        ('shared_bridge_lock' if instance_id is None else 'instance_bridge_lock'):
            lock_probe(instance_root / 'bridge.lock'),
        'worker_process': pid_probe(record.get('worker_pid')),
        'dsh_process': pid_probe(run.get('child_pid')),
        # bridge starts DSH with start_new_session=True, so PGID equals child_pid.
        'dsh_process_group': pid_probe(run.get('child_pid'), group=True),
        'dsh_returncode': run.get('returncode'),
    }
    # A later run may already use this cwd. The lease is context only, never
    # evidence that this completed worker or DSH process remains alive.
    report['workspace_lock'] = (lock_probe(instances.workspace_lock_path(runtime, record['cwd']))
                                if record.get('cwd') else 'not_recorded')
    processes = [report[k] for k in ['worker_process', 'dsh_process', 'dsh_process_group']]
    if record['status'] in ACTIVE:
        report['state'] = 'active'
    elif record['status'] == 'orphaned':
        report['state'] = 'needs_review'
    elif report['worker_lock'] == 'held':
        # Result is persisted before notification; worker may still be finishing.
        report['state'] = 'pending_exit'
    elif 'exists_unverified' in processes:
        report['state'] = 'needs_review'
    elif report['worker_lock'] == 'released' and all(p == 'absent' for p in processes):
        report['state'] = 'released'
    else:
        report['state'] = 'unknown'
    return report


def status(key, runtime=bridge.RUNTIME):
    dest = job_path(runtime, key)
    # The submit lock covers publication of a new receipt, not the model run.
    with (runtime / 'jobs/submit.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        record = json.loads((dest / 'receipt.json').read_text(encoding='utf-8'))
    if record['status'] in ACTIVE:
        with (dest / 'worker.lock').open('a') as lease:
            try:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                # Re-read after acquiring: worker may have completed between reads.
                record = json.loads((dest / 'receipt.json').read_text(encoding='utf-8'))
                if record['status'] in ACTIVE:
                    record = record | {'status': 'orphaned', 'code': 'worker_not_running'}
    request = json.loads((dest / 'request.json').read_text(encoding='utf-8'))
    ipc.validate_record(record, request)
    bridge.validate_tool_policy(record, request)
    profile = request.get('profile', bridge.DEFAULT_PROFILE)
    if record.get('profile', bridge.DEFAULT_PROFILE) != profile:
        raise ValueError('profile_mismatch')
    reasoning_effort = bridge.resolve_reasoning_effort(
        request.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT))
    if record.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT) != reasoning_effort:
        raise ValueError('reasoning_effort_mismatch')
    instance_id = request.get('instance_id')
    instance_root = instances.root(runtime, instance_id)
    if record.get('instance_id') != instance_id:
        raise ValueError('instance_mismatch')
    target = request.get('cwd', str((instance_root / 'work').resolve()))
    if (record.get('cwd', target) != target or
            str(bridge.resolve_cwd(target, instance_root, require_exists=False)) != target):
        raise ValueError('cwd_mismatch')
    if record.get('run_dir'):
        _, run = bridge.load_run(record['run_dir'], runtime)
        bridge.validate_tool_policy(run, request)
        if run.get('status') == 'completed':
            bridge.validate_tool_policy_attestation(run)
        if run.get('profile', bridge.DEFAULT_PROFILE) != profile:
            raise ValueError('profile_mismatch')
        if run.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT) != reasoning_effort:
            raise ValueError('reasoning_effort_mismatch')
        if run.get('instance_id') != instance_id:
            raise ValueError('instance_mismatch')
        if run.get('cwd') != target:
            raise ValueError('cwd_mismatch')
    notification = notify.read(dest)
    if notification is not None:
        record = record | {'notification': notification}
    elif record.get('notification_requested'):
        record = record | {'notification': {'status': 'waiting' if record['status'] in ACTIVE else 'pending'}}
    record = record | {'cleanup': cleanup_status(record, dest, runtime)}
    if request.get('delivery') == 'conversation':
        record['conversation_delivery'] = ipc.read(dest) or {'status': 'not_recorded'}
        ack = dest / 'callback-ack.json'
        if ack.is_file():
            record['conversation_ack'] = json.loads(ack.read_text(encoding='utf-8'))
    return record


def submit(task_path, key, timeout=300, runtime=bridge.RUNTIME, executable=None, title='', cwd=None,
           profile=None, expected_config_hash=None, instance_id=instances.AUTO, reasoning_effort=None,
           delivery=None, callback_target=None, tool_policy=None):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('invalid_timeout')
    task = Path(task_path).read_bytes()
    task.decode('utf-8')
    if not task.strip() or len(task) > 2 * 1024 * 1024:
        raise ValueError('invalid_task_size')
    executable = executable or bridge.binary()
    # Validate explicit targets before claiming either a job id or an instance.
    target_root = runtime if instance_id == instances.AUTO else instances.root(runtime, instance_id)
    target = bridge.resolve_cwd(cwd, target_root) if cwd is not None else None
    reasoning_effort = bridge.resolve_reasoning_effort(reasoning_effort)
    profile, patch = bridge.resolve_profile(profile, runtime, reasoning_effort)
    requested_tool_policy = tool_policy
    tool_policy = bridge.resolve_tool_policy(tool_policy)
    bridge.validate_consultation_model(tool_policy, profile, reasoning_effort)
    policy_config, _ = bridge.execution_config(patch, tool_policy)
    config_hash = policy_config['config_sha256']
    if requested_tool_policy is not None and expected_config_hash is not None and config_hash != expected_config_hash:
        raise ValueError('config_changed')
    request = {'task_sha256': bridge.digest(task), 'timeout': timeout, 'profile': profile,
               'reasoning_effort': reasoning_effort,
               **policy_config,
               'executable': str(Path(executable).resolve())}
    title = notify.validate_title(title)
    if title:
        request['title'] = title
    dest = job_path(runtime, key)
    dest.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (dest.parent / 'submit.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if dest.exists():
            prior = json.loads((dest / 'request.json').read_text(encoding='utf-8'))
            if requested_tool_policy is None:
                tool_policy = bridge.resolve_tool_policy(prior.get('tool_policy'))
                bridge.validate_consultation_model(tool_policy, profile, reasoning_effort)
                policy_config, _ = bridge.execution_config(patch, tool_policy)
                if expected_config_hash is not None and expected_config_hash != policy_config['config_sha256']:
                    raise ValueError('config_changed')
                request.update(policy_config)
            request.update(ipc.configure(delivery, callback_target, prior))
            prior_id = prior.get('instance_id')
            prior_root = instances.root(runtime, prior_id)
            if instance_id != instances.AUTO and instance_id != prior_id:
                raise ValueError('job_id_conflict')
            # Omitted parameters reuse the accepted instance/cwd. Old receipts
            # without instance_id still mean legacy home, and are never rewritten.
            prior.setdefault('cwd', str((prior_root / 'work').resolve()))
            prior.setdefault('profile', bridge.DEFAULT_PROFILE)
            prior.setdefault('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT)
            prior.setdefault('tool_policy', 'default')
            request['cwd'] = str(target) if target is not None else prior['cwd']
            if 'instance_id' in prior:
                request['instance_id'] = prior_id
            if prior != request:
                raise ValueError('job_id_conflict')
            # A reused id only retrieves the receipt; never launches a second job.
            receipt = json.loads((dest / 'receipt.json').read_text(encoding='utf-8'))
            bridge.validate_tool_policy(receipt, prior)
            if receipt.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT) != reasoning_effort:
                raise ValueError('reasoning_effort_mismatch')
            return receipt | {'reused': True}
        if expected_config_hash is not None and config_hash != expected_config_hash:
            raise ValueError('config_changed')
        request.update(ipc.configure(delivery, callback_target))
        instance_id = instances.prepare(runtime, instance_id)
        instance_root = instances.root(runtime, instance_id)
        target = bridge.resolve_cwd(target, instance_root)
        request['cwd'] = str(target)
        if instance_id is not None:
            request['instance_id'] = instance_id
        record = {'job_id': key, 'status': 'accepted', 'submitted': bridge.now(),
                  'job_dir': str(dest), 'run_dir': None, 'callback': 'none', 'cwd': str(target),
                  'profile': profile, 'reasoning_effort': reasoning_effort,
                  **policy_config}
        if request.get('delivery') == 'conversation':
            record.update(delivery='conversation', callback=ipc.PROTOCOL,
                          callback_target=request['callback_target'])
        if instance_id is not None:
            record['instance_id'] = instance_id
        if title:
            record['title'] = title
        # Eligibility is fixed at acceptance; installing a webhook never backfills old jobs.
        record['notification_requested'] = (runtime / notify.CONFIG).is_file()
        # Failed preparation never publishes a half-written id. A crash after
        # rename but before spawn leaves a complete, queryable orphaned receipt.
        with tempfile.TemporaryDirectory(prefix='.partial-', dir=dest.parent) as staging:
            stage = Path(staging)
            (stage / 'task.md').write_bytes(task)
            bridge.write_json(stage / 'request.json', request)
            bridge.write_json(stage / 'receipt.json', record)
            ipc.prepare(stage, request)
            with (stage / 'worker.lock').open('a') as lease:
                fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                stage.rename(dest)
                try:
                    proc = subprocess.Popen(
                        [sys.executable, str(Path(__file__).resolve()), '_worker',
                         '--job', key, '--runtime', str(runtime.resolve()),
                         '--lease-fd', str(lease.fileno())],
                        cwd=bridge.WORKSPACE, stdin=subprocess.DEVNULL,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        start_new_session=True, pass_fds=(lease.fileno(),))
                except OSError:
                    record.update(status='launch_failed', ended=bridge.now())
                    bridge.write_json(dest / 'receipt.json', record)
                else:
                    # Worker owns subsequent writes; no parent/child last-write race.
                    record['worker_pid'] = proc.pid
    if record['status'] == 'launch_failed':
        notification = notify.finish(dest, record, runtime)
        if notification is not None:
            record['notification'] = notification
    if request.get('delivery') == 'conversation':
        record['conversation_delivery'] = ipc.launch(dest, runtime)
    return record


def worker(key, runtime, lease_fd):
    dest = job_path(runtime, key)
    # This descriptor was locked before Popen and survives the submitting process.
    with os.fdopen(lease_fd, 'a') as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        record = json.loads((dest / 'receipt.json').read_text(encoding='utf-8'))
        if record['status'] != 'accepted':
            raise ValueError('job_already_started')
        record.update(status='running', worker_pid=os.getpid(), started=bridge.now())
        bridge.write_json(dest / 'receipt.json', record)
        def attach(run_dir):
            record['run_dir'] = str(run_dir)
            bridge.write_json(dest / 'receipt.json', record)
        def interrupted(signum, frame):
            raise KeyboardInterrupt()
        signal.signal(signal.SIGTERM, interrupted)
        try:
            request = json.loads((dest / 'request.json').read_text(encoding='utf-8'))
            ipc.validate_record(record, request)
            bridge.validate_tool_policy(record, request)
            if bridge.digest((dest / 'task.md').read_bytes()) != request['task_sha256']:
                raise ValueError('artifact_modified')
            reasoning_effort = bridge.resolve_reasoning_effort(
                request.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT))
            profile, patch = bridge.resolve_profile(
                request.get('profile', bridge.DEFAULT_PROFILE), runtime, reasoning_effort)
            tool_policy = bridge.resolve_tool_policy(request.get('tool_policy'))
            bridge.validate_consultation_model(tool_policy, profile, reasoning_effort)
            if record.get('profile', bridge.DEFAULT_PROFILE) != profile:
                raise ValueError('profile_mismatch')
            if record.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT) != reasoning_effort:
                raise ValueError('reasoning_effort_mismatch')
            instance_id = request.get('instance_id')
            if record.get('instance_id') != instance_id:
                raise ValueError('instance_mismatch')
            instance_root = instances.root(runtime, instance_id)
            if record.get('config_sha256', request['config_sha256']) != request['config_sha256']:
                raise ValueError('config_changed')
            policy_config, _ = bridge.execution_config(patch, tool_policy)
            if policy_config['config_sha256'] != request['config_sha256']:
                raise ValueError('config_changed')
            bridge.validate_tool_policy(policy_config, request)
            target = request.get('cwd', str((instance_root / 'work').resolve()))
            if record.get('cwd', target) != target:
                raise ValueError('cwd_mismatch')
            # Resolve again before launching: a retargeted symlink must not redirect a job.
            if str(bridge.resolve_cwd(target, instance_root, require_exists=False)) != target:
                raise ValueError('cwd_mismatch')
            default = str((instance_root / 'work').resolve())
            with redirect_stdout(io.StringIO()):
                _, outcome = bridge.run(dest / 'task.md', None, request['timeout'],
                                        runtime, request['executable'], on_created=attach,
                                        cwd=None if target == default else target,
                                        profile=profile, expected_config_hash=request['config_sha256'],
                                        instance_id=instance_id, reasoning_effort=reasoning_effort,
                                        tool_policy=tool_policy)
            record['status'] = outcome
        except KeyboardInterrupt:
            record['status'] = 'interrupted'
        except Exception as exc:
            record['status'] = 'failed'
            record['code'] = safe_code(exc)
        record['ended'] = bridge.now()
        bridge.write_json(dest / 'receipt.json', record)
        notify.finish(dest, record, runtime)


def result(key, runtime=bridge.RUNTIME):
    record = status(key, runtime)
    if record['status'] != 'completed':
        raise ValueError('job_not_completed')
    dest, run = bridge.load_run(record['run_dir'], runtime)
    if run['status'] != 'completed':
        raise ValueError('run_not_completed')
    response = (dest / 'response.md').read_bytes()
    request = json.loads((job_path(runtime, key) / 'request.json').read_text(encoding='utf-8'))
    bridge.validate_tool_policy(run, request)
    bridge.validate_tool_policy_attestation(run)
    instance_root = instances.root(runtime, request.get('instance_id'))
    if run['cwd'] != request.get('cwd', str((instance_root / 'work').resolve())):
        raise ValueError('cwd_mismatch')
    if run['config_sha256'] != request['config_sha256']:
        raise ValueError('config_changed')
    if (bridge.digest(response) != run['response_sha256'] or
            run['task_sha256'] != request['task_sha256'] or
            bridge.digest((dest / 'task.md').read_bytes()) != request['task_sha256']):
        raise ValueError('artifact_modified')
    return record | {'response': response.decode('utf-8'), 'response_sha256': run['response_sha256']}


def safe_code(exc):
    code = str(exc)
    return code if isinstance(exc, ValueError) and re.fullmatch('[a-z_]+', code) else type(exc).__name__


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    p = commands.add_parser('submit')
    p.add_argument('--task', required=True)
    p.add_argument('--job', default=None, help='Reuse the same id to avoid duplicate submission')
    p.add_argument('--timeout', type=float, default=300)
    p.add_argument('--title', default='', help='Short non-sensitive title visible in DingTalk')
    p.add_argument('--cwd', help='Existing workspace directory; default: a new task instance work directory')
    p.add_argument('--profile', choices=bridge.PROFILE_PATCHES)
    p.add_argument('--reasoning-effort', choices=bridge.REASONING_EFFORTS)
    p.add_argument('--tool-policy', choices=bridge.TOOL_POLICIES,
                   help='none: Go/High text consultation with no model tools')
    p.add_argument('--delivery', choices=ipc.DELIVERIES,
                   help='notify (default) or return to the originating CC Connect conversation')
    for name in ['status', 'result']:
        commands.add_parser(name).add_argument('--job', required=True)
    commands.add_parser('list')
    p = commands.add_parser('_worker', help=argparse.SUPPRESS)
    p.add_argument('--job', required=True)
    p.add_argument('--runtime', required=True)
    p.add_argument('--lease-fd', type=int, required=True)
    args = parser.parse_args()
    try:
        bridge.scope()
        if args.command == '_worker':
            worker(args.job, Path(args.runtime), args.lease_fd)
            return 0
        if args.command == 'submit':
            value = submit(args.task, args.job or uuid.uuid4().hex, args.timeout,
                           title=args.title, cwd=args.cwd, profile=args.profile,
                           reasoning_effort=args.reasoning_effort, delivery=args.delivery,
                           tool_policy=args.tool_policy)
        elif args.command == 'status':
            value = status(args.job)
        elif args.command == 'result':
            value = result(args.job)
        else:
            value = [status(p.name) for p in sorted((bridge.RUNTIME / 'jobs').glob('*/'))
                     if not p.name.startswith('.') and (p / 'receipt.json').is_file()]
        print(json.dumps(value, ensure_ascii=False, indent=2))
        return 0
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'code': safe_code(exc)}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
