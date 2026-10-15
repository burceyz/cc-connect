#!/usr/bin/env python3
"""User-launched offline entry: verify DSH shell before submitting a real job."""
import argparse
import fcntl
import json
import math
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import time
import uuid

import bridge
import jobs
import notify
import instances
import ipc
import routing

LABELS = {
    'started': '离线执行检查通过，任务执行已启动。',
    'inventory.done': '成员报告：本轮资料与问题梳理已完成。',
    'implementation.done': '成员报告：本轮改动已落盘，待验证。',
    'checks.done': '成员报告：本轮验证记录已整理，通过与失败以报告为准。',
    'archive.done': '成员报告：本轮归档整理已完成。',
    'blocked.done': '成员报告：遇到影响交付的阻塞，可查询状态记录。',
}


def verify_probe(rows, record, cwd, command, nonce, profile=None, reasoning_effort=None, model=None):
    """Trust exact native tool execution, never the model's final success claim."""
    if record.get('status') != 'completed' or record.get('returncode') != 0:
        raise ValueError('probe_run_failed')
    if not rows or rows[0].get('id') != record['session_id'] or rows[0].get('cwd') != str(cwd):
        raise ValueError('probe_session_mismatch')
    modes = [r['data'].get('mode') for r in rows if r.get('type') == 'sandbox/mode']
    presets = [r['data'].get('preset') for r in rows if r.get('type') == 'permission/preset']
    if not modes or any(m != 'workspace-write' for m in modes + presets):
        raise ValueError('probe_permissions_mismatch')
    profile = bridge.DEFAULT_PROFILE if profile is None else profile
    if profile not in bridge.PROFILE_PATCHES:
        raise ValueError('unknown_profile')
    if record.get('profile', bridge.DEFAULT_PROFILE) != profile:
        raise ValueError('profile_mismatch')
    reasoning_effort = bridge.resolve_reasoning_effort(reasoning_effort)
    if record.get('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT) != reasoning_effort:
        raise ValueError('reasoning_effort_mismatch')
    model = bridge.resolve_model(model)
    if profile not in bridge.MODEL_PATCHES[model][reasoning_effort]:
        raise ValueError('model_profile_unavailable')
    if record.get('model', bridge.DEFAULT_MODEL) != model:
        raise ValueError('model_mismatch')
    configs = [r['data']['header']['config'] for r in rows if r.get('type') == 'request/header']
    if not configs or any((c.get('provider'), c.get('model'), c.get('reasoningEffort')) !=
                          (bridge.PROFILE_PROVIDERS[profile], model, reasoning_effort)
                          for c in configs):
        raise ValueError('probe_model_mismatch')
    calls = [r['data'] for r in rows if r.get('type') == 'tool/call']
    if len(calls) != 1 or calls[0].get('name') != 'bash':
        raise ValueError('probe_unexpected_tools')
    args = json.loads(calls[0]['arguments'])
    if args.get('command') != command or ('workdir' in args and args['workdir'] != str(cwd)):
        raise ValueError('probe_command_mismatch')
    allowed_args = {'command', 'description', 'timeout', 'timeoutMs', 'workdir'}
    if set(args) - allowed_args:
        raise ValueError('probe_arguments_invalid')
    results = [r['data'] for r in rows if r.get('type') == 'tool/result'
               and r['data'].get('message', {}).get('toolCallId') == calls[0]['callId']]
    if len(results) != 1 or results[0].get('error') or results[0]['message'].get('isError'):
        raise ValueError('probe_shell_failed')
    text = '\n'.join(b.get('text', '') for b in results[0]['message'].get('content', [])
                     if b.get('type') == 'text')
    if 'DSH_PREFLIGHT_OK::' + nonce not in text:
        raise ValueError('probe_success_evidence_missing')
    return {'session_id': record['session_id'], 'cwd': str(cwd),
            'mode': 'workspace-write', 'model': model,
            'profile': profile, 'provider': bridge.PROFILE_PROVIDERS[profile],
            'reasoning_effort': reasoning_effort, 'shell_call_id': calls[0]['callId']}


class ProbeFailure(ValueError):
    def __init__(self, code, record):
        super().__init__(code)
        self.probe_record = str(record)


def preflight(root, cwd, runtime=None, profile=None, expected_config_hash=None, instance_id=instances.AUTO,
              reasoning_effort=None, model=None):
    runtime = bridge.RUNTIME if runtime is None else runtime
    if profile is None:
        profile = routing.resolve_default_profile(runtime)
    routing.check_profile_enabled(profile, runtime)
    # Persist the run locator even when verification raises after a native run.
    # The outside probe remains parent-owned and outside the model's write roots.
    trace_path = root / 'probe-attempts' / (uuid.uuid4().hex + '.json')
    trace_path.parent.mkdir(parents=True, exist_ok=True)
    trace = {'status': 'preparing', 'started_at': bridge.now(), 'cwd': str(cwd),
             'instance_id': instance_id, 'profile': profile, 'reasoning_effort': reasoning_effort}
    bridge.write_json(trace_path, trace)

    def on_created(run_dir):
        trace.update(status='running', run_dir=str(run_dir))
        bridge.write_json(trace_path, trace)

    try:
        with tempfile.TemporaryDirectory(prefix='probe-', dir=root) as folder:
            folder = Path(folder).resolve()
            if folder.is_relative_to(cwd):
                raise ValueError('probe_requires_repository_target')
            control = folder / 'parent-write-check'
            control.write_text('ok')
            control.unlink()
            nonce = uuid.uuid4().hex
            outside = folder / 'must-not-be-writable'
            command = shlex.join([sys.executable, '-B', str(Path(__file__).with_name('shell_probe.py')),
                                  '--cwd', str(cwd), '--outside', str(outside), '--nonce', nonce])
            task = folder / 'task.md'
            task.write_text(
                '本轮仅做用户授权的离线执行检查。必须且只能调用一次 bash 工具，command 原样使用下行；\n'
                '调用参数只需 command 与 description，严禁携带 justification 或 sandbox_permissions，不提升权限；\n'
                '不要调用其他工具，不改配置、不重试、不改命令。该脚本只创建并清理唯一临时探针，\n'
                '检查当前仓库读写、Python测试和系统临时目录；对给定库外探针预期拒绝。\n'
                '失败如实报告，不要绕过。执行后简短回复结果。\n\n' + command + '\n', encoding='utf-8')
            trace.update(command=command, nonce=nonce)
            bridge.write_json(trace_path, trace)
            run_dir, _ = bridge.run(task, None, 300, runtime, cwd=cwd,
                                   profile=profile, expected_config_hash=expected_config_hash,
                                   instance_id=instance_id, reasoning_effort=reasoning_effort, model=model,
                                   on_created=on_created,
                                   mailbox_plan={'status': 'disabled'})
            _, record = bridge.load_run(run_dir, runtime)
            if instance_id != instances.AUTO and record.get('instance_id') != instance_id:
                raise ValueError('instance_mismatch')
            if jobs.pid_probe(record.get('child_pid')) != 'absent' or jobs.pid_probe(
                    record.get('child_pid'), group=True) != 'absent':
                raise ValueError('probe_process_cleanup_unconfirmed')
            if record.get('status') != 'completed':
                raise ValueError('probe_run_failed')
            _, rows = bridge.session_rows(record['session_id'], runtime, record.get('instance_id'))
            verified = verify_probe(rows, record, cwd, command, nonce, profile, reasoning_effort, model)
            if outside.exists() or outside.is_symlink():
                raise ValueError('outside_write_was_allowed')
            trace.update(status='verified', ended_at=bridge.now(), run_dir=str(run_dir))
            bridge.write_json(trace_path, trace)
            return verified | {'run_dir': str(run_dir), 'checked_at': bridge.now(),
                               'instance_id': record.get('instance_id'), 'probe_record': str(trace_path)}
    except Exception as error:
        code = jobs.safe_code(error)
        trace.update(status='failed', ended_at=bridge.now(), code=code)
        bridge.write_json(trace_path, trace)
        raise ProbeFailure(code, trace_path) from error


def emit_once(root, key, content, runtime=None):
    runtime = bridge.RUNTIME if runtime is None else runtime
    target = root / ('notice-' + key + '.json')
    if target.exists():
        return
    bridge.write_json(target, {'status': 'sending', 'at': bridge.now()})
    try:
        config = notify.load_config(runtime)
        result = notify.send(config, content) if config else {'status': 'skipped'}
    except Exception:
        result = {'status': 'failed', 'code': 'notification_config_error'}
    bridge.write_json(target, result | {'at': bridge.now()})


def milestone_content(key, title, job):
    return '\n'.join([f'任务进度｜{notify.MEMBER_LABEL}｜阶段进度', '任务：' + (title or job),
                      LABELS[key], '编号：' + job,
                      '阶段信息不代表需求已全部完成；任务结束后另有通知。',
                      f'按需查看：对 {notify.OWNER_LABEL} 说“查 DSH 结果 {job}”。'])


def monitor(key, runtime):
    root = runtime / 'offline' / key
    with (root / 'monitor.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        request = json.loads((root / 'request.json').read_text())
        signals = Path(request['signals'])
        deadline = time.monotonic() + request['timeout'] + 300
        while time.monotonic() < deadline:
            record = jobs.status(key, runtime)
            for name in LABELS:
                path = signals / name
                reached = bool(record.get('started')) if name == 'started' else (
                    not signals.is_symlink() and not path.is_symlink() and path.is_file()
                    and path.stat().st_size <= 32 and path.read_text().strip() == 'done')
                if reached:
                    emit_once(root, name, milestone_content(name, request['title'], key), runtime)
            if record['status'] not in jobs.ACTIVE:
                bridge.write_json(root / 'monitor-result.json',
                                  {'status': 'finished', 'job_status': record['status'], 'at': bridge.now()})
                return
            time.sleep(5)
        bridge.write_json(root / 'monitor-result.json', {'status': 'expired', 'at': bridge.now()})


def start(task, key, cwd, title, timeout, runtime=None, profile=None, reasoning_effort=None,
          delivery=None, model=None, team=None, attempt_dir=None):
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('invalid_timeout')
    runtime = bridge.RUNTIME if runtime is None else runtime
    cwd = bridge.resolve_cwd(cwd, runtime)
    dest = jobs.job_path(runtime, key)  # Validate the job id before constructing any paths.
    title = notify.validate_title(title)
    task = Path(task).resolve()
    body = task.read_bytes()
    body.decode('utf-8')
    if not body.strip() or len(body) > 2 * 1024 * 1024:
        raise ValueError('invalid_task_size')
    reasoning_effort = bridge.resolve_reasoning_effort(reasoning_effort)
    model = bridge.resolve_model(model)
    root = runtime / 'offline' / key
    prior = root / 'request.json'
    if profile is None:
        if prior.exists():
            profile = json.loads(prior.read_text()).get('profile') or bridge.DEFAULT_PROFILE
        elif dest.exists():
            profile = json.loads((dest / 'request.json').read_text()).get('profile') or bridge.DEFAULT_PROFILE
        else:
            profile = routing.resolve_default_profile(runtime)
    if not dest.exists():
        routing.check_profile_enabled(profile, runtime)
    profile, patch = bridge.resolve_profile(profile, runtime, reasoning_effort, model)
    request = {'task_sha256': bridge.digest(body), 'cwd': str(cwd), 'profile': profile,
               'reasoning_effort': reasoning_effort, 'model': model,
               'title': title, 'timeout': timeout,
               'config_sha256': bridge.digest(patch.read_bytes()),
               'signals': str(Path('/private/tmp') / ('dsh-' + key))}
    if team is not None:
        request['team'] = team
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / 'start.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError('offline_start_busy') from None
        prior = root / 'request.json'
        if prior.exists():
            previous = json.loads(prior.read_text())
            request.update(ipc.configure(delivery, prior=previous))
            previous.setdefault('profile', bridge.DEFAULT_PROFILE)
            previous.setdefault('reasoning_effort', bridge.DEFAULT_REASONING_EFFORT)
            previous.setdefault('model', bridge.DEFAULT_MODEL)
            if 'team' in previous:
                if team is None:
                    team = previous['team']
                    request['team'] = team
                elif team != previous['team']:
                    raise ValueError('offline_request_conflict')
            elif team is not None:
                raise ValueError('offline_request_conflict')
            if {k: v for k, v in previous.items() if k != 'instance_id'} != request:
                raise ValueError('offline_request_conflict')
            instance_id = previous.get('instance_id')
            instances.root(runtime, instance_id)
        else:
            # Reuse an existing direct job instead of allocating an unused home.
            if dest.exists():
                existing = json.loads((dest / 'request.json').read_text())
                request.update(ipc.configure(delivery, prior=existing))
                instance_id = existing.get('instance_id')
            else:
                request.update(ipc.configure(delivery))
                instance_id = instances.allocate(runtime)
            if instance_id is not None:
                request['instance_id'] = instance_id
            bridge.write_json(prior, request)
        if dest.exists():
            return jobs.submit(task, key, timeout, runtime, title=title, cwd=cwd,
                               profile=profile, expected_config_hash=request['config_sha256'],
                               instance_id=instance_id, reasoning_effort=reasoning_effort,
                               delivery=request.get('delivery'), callback_target=request.get('callback_target'), model=model,
                               team=team)
        attempt_root = root
        if attempt_dir is not None:
            attempt_root = Path(attempt_dir)
            if (attempt_root.parent != root / 'attempts' or attempt_root.is_symlink() or
                    attempt_root.resolve() != root.resolve() / 'attempts' / attempt_root.name or
                    not attempt_root.is_dir() or not prior.is_file()):
                raise ValueError('invalid_preflight_attempt_directory')
            if (attempt_root / 'state.json').exists() or (attempt_root / 'probe-attempts').exists():
                raise ValueError('preflight_attempt_already_used')
        # New sessions inherit a restrictive preset; never enable unconfined execution.
        if os.environ.get('DSH_PERMISSION_MODE') not in {None, 'workspace-write'}:
            raise ValueError('unexpected_permission_environment')
        if team:
            # An explicit team must fail before the preflight model ever runs.
            bridge.resolve_mailbox_plan(runtime, team, 'default', bridge.binary())
        os.environ['DSH_PERMISSION_MODE'] = 'workspace-write'
        try:
            evidence = preflight(attempt_root, cwd, runtime, profile, request['config_sha256'], instance_id,
                                 reasoning_effort, model)
            bridge.write_json(attempt_root / ('preflight-' + uuid.uuid4().hex + '.json'), evidence)
            if bridge.digest(patch.read_bytes()) != request['config_sha256']:
                raise ValueError('config_changed')
            signals = Path(request['signals'])
            if signals.is_symlink() or signals.exists():
                raise ValueError('signals_directory_already_exists')
            signals.mkdir(mode=0o700)
            record = jobs.submit(task, key, timeout, runtime, title=title, cwd=cwd,
                                 profile=profile, expected_config_hash=request['config_sha256'],
                                 instance_id=instance_id, reasoning_effort=reasoning_effort,
                                 delivery=request.get('delivery'), callback_target=request.get('callback_target'), model=model,
                                 team=team)
        except Exception as exc:
            bridge.write_json(attempt_root / 'state.json', {'status': 'not_submitted',
                              'code': jobs.safe_code(exc), 'at': bridge.now(),
                              'probe_record': getattr(exc, 'probe_record', None)})
            emit_once(root, 'preflight-failed', '\n'.join([
                f'任务进度｜{notify.MEMBER_LABEL}｜启动前检查未通过',
                '任务：' + (title or key), '离线执行检查未通过，正式任务未启动。',
                '编号：' + key,
                f'查看方式：对 {notify.OWNER_LABEL} 说“查 DSH 结果 {key}”。']), runtime)
            raise
        bridge.write_json(attempt_root / 'state.json', {'status': record['status'], 'at': bridge.now()})
        if record['status'] != 'launch_failed':
            try:
                proc = subprocess.Popen([sys.executable, '-B', str(Path(__file__).resolve()),
                                         '_monitor', '--job', key, '--runtime', str(runtime)],
                                        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL, start_new_session=True)
            except OSError:
                record = record | {'milestone_monitor': 'launch_failed'}
                bridge.write_json(root / 'monitor-result.json', {'status': 'launch_failed'})
            else:
                bridge.write_json(root / 'monitor-process.json', {'pid': proc.pid, 'at': bridge.now()})
        return record


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    p = sub.add_parser('start', help='Run from a user-controlled system terminal')
    p.add_argument('--task', required=True)
    p.add_argument('--job', required=True)
    p.add_argument('--cwd', required=True)
    p.add_argument('--title', required=True)
    p.add_argument('--timeout', type=float, default=14400)
    p.add_argument('--profile', choices=bridge.PROFILE_PATCHES)
    p.add_argument('--reasoning-effort', choices=bridge.REASONING_EFFORTS)
    p.add_argument('--model', choices=bridge.MODEL_PATCHES)
    p.add_argument('--delivery', choices=ipc.DELIVERIES)
    p.add_argument('--team', default=None, help='Collaborative team name')
    p = sub.add_parser('_monitor', help=argparse.SUPPRESS)
    p.add_argument('--job', required=True)
    p.add_argument('--runtime', required=True)
    args = parser.parse_args()
    try:
        bridge.scope()
        if args.command == '_monitor':
            monitor(args.job, Path(args.runtime))
            return 0
        record = start(args.task, args.job, args.cwd, args.title, args.timeout, profile=args.profile,
                       reasoning_effort=args.reasoning_effort, delivery=args.delivery, model=args.model,
                       team=args.team)
        print(json.dumps(record, ensure_ascii=False, indent=2))
        return 1 if record['status'] == 'launch_failed' else 0
    except Exception as exc:
        print(json.dumps({'status': 'failed', 'code': jobs.safe_code(exc)}), file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
