"""Real Dorothy background-worker tests using only a local fake CLI and credentials."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import warnings
from unittest.mock import patch

import dorothy


PROBE_ROOT = dorothy.WORKSPACE / '.local/dorothy-worker-implementation-20261012'
ENTRY = Path(dorothy.__file__).resolve()
LONG_TEXT = '  完整资料起点\n' + '中文资料🙂\n' * 270000 + '完整资料终点  \n'

FAKE_CLI = r'''
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
import uuid

arguments = sys.argv[1:]
task = json.loads(arguments[arguments.index('-p') + 1])
store = Path(next(arg.split('=', 1)[1] for arg in arguments if arg.startswith('--gemini_dir=')))
history_path = store / 'fixture-history.json'
requested = arguments[arguments.index('--conversation') + 1] if '--conversation' in arguments else None
cwd = str(Path.cwd().resolve())
if requested:
    history = json.loads(history_path.read_text(encoding='utf-8'))
    if history['conversation_id'] != requested or history['cwd'] != cwd:
        raise SystemExit(41)
else:
    history = {'conversation_id': 'fixture-' + uuid.uuid4().hex, 'cwd': cwd, 'labels': []}
history['labels'].append(task['label'])
history_path.write_text(json.dumps(history, ensure_ascii=False), encoding='utf-8')
sid = history['conversation_id']
marker = Path(task['marker'])
marker.write_text(json.dumps({'pid': os.getpid(), 'pgid': os.getpgrp(), 'cwd': cwd,
                              'conversation_id': sid, 'requested_session': requested,
                              'native_dir': str(store), 'arguments': arguments[:-2]}), encoding='utf-8')
with (Path.cwd() / 'fixture-launches.jsonl').open('a', encoding='utf-8') as handle:
    handle.write(json.dumps({'label': task['label'], 'pid': os.getpid()}) + '\n')

def emit(kind, **value):
    sys.stdout.write(json.dumps({'event': kind, kind: value}, ensure_ascii=False) + '\n')
    sys.stdout.flush()

def step(index, kind, state, text=''):
    emit('step_update', conversation_id=sid, step_index=index, step_type=kind,
         state=state, text_delta=text)

emit('init', conversation_id=sid, model=arguments[arguments.index('--model') + 1])
step(0, 'user_input', 'DONE')
mode = task.get('mode', 'normal')
if mode == 'blocked':
    step(1, 'agent_response', 'ACTIVE', '尚未完成的调查🙂\n')
    gate = Path(task['gate'])
    while not gate.exists():
        time.sleep(0.03)

if mode == 'leader_child':
    child_marker = Path(task['child_marker'])
    child_code = (
        'import os, signal, sys, time; from pathlib import Path; '
        'signal.signal(signal.SIGTERM, signal.SIG_IGN); '
        'Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(120)'
    )
    child = subprocess.Popen([sys.executable, '-B', '-c', child_code, str(child_marker)],
                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL, close_fds=False)
    deadline = time.monotonic() + 5
    while not child_marker.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    if not child_marker.exists():
        raise SystemExit(42)

report = (('  完整资料起点\n' + '中文资料🙂\n' * 270000 + '完整资料终点  \n')
          if mode == 'long' else json.dumps(history, ensure_ascii=False))
# A progress message and a tool occur before the report. Result.response is an
# aggregate placeholder, so treating it as the report loses the actual answer.
step(1 if mode != 'blocked' else 2, 'agent_response', 'DONE', '已完成资料核对。\n')
step(2 if mode != 'blocked' else 3, 'tool', 'DONE')
step(3 if mode != 'blocked' else 4, 'agent_response', 'DONE', report)
if mode == 'native_error':
    emit('result', conversation_id=sid, status='ERROR', response='原生聚合回复',
         error='Malformed function call: SECRET fixture error')
    time.sleep(0.2)  # The worker must observe exit 0 instead of killing on ERROR.
elif mode != 'missing_result':
    emit('result', conversation_id=sid, status='SUCCESS', response='原生聚合回复，不是最终报告')
if mode == 'result_then_wait':
    time.sleep(120)
if mode == 'exit_failure':
    raise SystemExit(23)
'''


@unittest.skipUnless(sys.platform == 'darwin', 'requires macOS Seatbelt and process groups')
class DorothyWorkerTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(Path('/usr/bin/sandbox-exec').is_file())
        dorothy.HERMES_RUNTIME.mkdir(parents=True, exist_ok=True)
        PROBE_ROOT.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='worker-tests-', dir=PROBE_ROOT)
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        self.work = self.root / '工作目录'
        self.work.mkdir()
        self.other_work = self.root / '独立目录'
        self.other_work.mkdir()
        self.fake = self.root / 'fake-agy'
        self.fake.write_text('#!' + sys.executable + '\n' + FAKE_CLI, encoding='utf-8')
        self.fake.chmod(0o700)
        self.auth = self.root / 'fake-oauth-token'
        self.auth.write_text('fixture-only-not-a-real-credential', encoding='utf-8')
        dorothy.write_json(self.runtime / 'config.json', {
            'executable': str(self.fake), 'auth_file': str(self.auth),
        })
        self.jobs = set()
        self.identities = {}

    def remember_process(self, pid):
        identity = dorothy.process_info(pid)
        if identity:
            self.identities[pid] = identity
        return identity

    def remember_job(self, job):
        dest = self.runtime / 'jobs' / job
        if not dest.is_dir():
            return
        self.jobs.add(job)
        launch = dest / 'launch.json'
        if launch.is_file():
            identity = dorothy.read_json(launch).get('worker')
            if identity:
                self.identities[identity['pid']] = identity
        state_path = dest / 'state.json'
        if state_path.is_file():
            state = dorothy.read_json(state_path)
            if state.get('native_pid'):
                self.remember_process(state['native_pid'])

    def cli(self, action, job, *options, expected=0):
        invocation = [sys.executable, '-B', str(ENTRY), action, '--job', job,
                      '--runtime', str(self.runtime), *map(str, options)]
        completed = subprocess.run(invocation, capture_output=True, text=True, timeout=15)
        self.remember_job(job)
        self.assertEqual(completed.returncode, expected, completed.stdout + completed.stderr)
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError:
            self.fail('CLI returned invalid JSON: ' + completed.stdout + completed.stderr)

    def task(self, job, mode='normal', directory=None):
        directory = directory or self.work
        path = self.root / (job + '.json')
        data = {'label': job, 'mode': mode, 'marker': str(directory / (job + '.started.json')),
                'gate': str(directory / (job + '.release')),
                'child_marker': str(directory / (job + '.child.pid'))}
        path.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
        return path

    def submit(self, job, mode='normal', timeout=15):
        return self.cli('submit', job, '--task', self.task(job, mode), '--cwd', self.work,
                        '--timeout', timeout)

    def wait_started(self, job, directory=None):
        path = (directory or self.work) / (job + '.started.json')
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            self.remember_job(job)
            if path.is_file():
                try:
                    value = json.loads(path.read_text(encoding='utf-8'))
                except json.JSONDecodeError:
                    time.sleep(0.02)
                    continue
                self.remember_process(value['pid'])
                return value
            state = dorothy.status(job, self.runtime)
            if state['status'] not in dorothy.ACTIVE:
                self.fail('native fixture did not start: ' + json.dumps(state, ensure_ascii=False))
            time.sleep(0.03)
        self.fail('native fixture did not start within deadline: ' + job)

    def wait_finished(self, job, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.remember_job(job)
            value = dorothy.status(job, self.runtime)
            if (value['status'] not in dorothy.ACTIVE and value['cleanup']['worker'] == 'absent'
                    and value['cleanup']['native_group'] == 'absent'):
                return value
            time.sleep(0.04)
        self.fail('worker or native group did not finish: ' + json.dumps(value, ensure_ascii=False))

    def assert_locks_released(self):
        for index in range(dorothy.SLOTS):
            with dorothy.locked(dorothy.slot_lock_path(self.runtime, index)):
                pass
        with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, self.work):
            pass

    def tearDown(self):
        # Every signal is restricted to a process identity observed in this test.
        # The test never kills an arbitrary saved PID or another task's group.
        for job in self.jobs:
            self.remember_job(job)
        for identity in list(self.identities.values()):
            if dorothy.process_matches(identity):
                try:
                    os.kill(identity['pid'], signal.SIGTERM)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if not any(dorothy.process_matches(item) for item in self.identities.values()):
                break
            time.sleep(0.05)
        for identity in self.identities.values():
            if dorothy.process_matches(identity):
                try:
                    os.killpg(identity['pgid'], signal.SIGKILL)
                except ProcessLookupError:
                    pass
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if not any(dorothy.process_matches(item) for item in self.identities.values()):
                break
            time.sleep(0.05)
        remaining = [item for item in self.identities.values() if dorothy.process_matches(item)]
        self.assertEqual(remaining, [], 'test-owned processes did not exit')
        for job in self.jobs:
            request = dorothy.checked_request(self.runtime / 'jobs' / job)
            store = Path(request['native_dir'])
            self.assertIn(store.parent, dorothy.native.system_temp_roots())
            self.assertTrue(store.name.startswith('dorothy-'))
            if store.exists():
                shutil.rmtree(store)
        self.temp.cleanup()

    def test_duplicate_submission_is_reused_and_changed_request_conflicts(self):
        self.submit('duplicate', 'blocked')
        started = self.wait_started('duplicate')
        repeated = self.cli('submit', 'duplicate', '--task', self.root / 'duplicate.json')
        self.assertTrue(repeated['reused'])
        self.assertEqual(repeated['conversation_id'], started['conversation_id'])
        conflict_task = self.task('different-input')
        conflict = self.cli('submit', 'duplicate', '--task', conflict_task, expected=1)
        self.assertEqual(conflict['error'], 'job_request_conflict')
        conflict = self.cli('submit', 'duplicate', '--task', self.root / 'duplicate.json',
                            '--timeout', 20, expected=1)
        self.assertEqual(conflict['error'], 'job_request_conflict')
        launches = (self.work / 'fixture-launches.jsonl').read_text().splitlines()
        self.assertEqual(len(launches), 1)
        self.cli('cancel', 'duplicate')
        self.assertEqual(self.wait_finished('duplicate')['status'], 'cancelled')

    def test_error_terminal_preserves_natural_exit_and_failure_diagnostic(self):
        self.submit('native-error', 'native_error')
        value = self.wait_finished('native-error')
        self.assertEqual(value['status'], 'native_run_failed')
        self.assertEqual(value['returncode'], 0)
        self.assertEqual(value['native_status'], 'ERROR')
        self.assertEqual(value['native_outcome']['error']['code'], 'malformed_function_call')
        self.assertNotIn('SECRET', json.dumps(value))
        dest = self.runtime / 'jobs/native-error'
        self.assertEqual(value['partial_sha256'], dorothy.file_digest(dest / 'partial.md'))
        self.assertFalse((dest / 'report.md').exists())
        with self.assertRaisesRegex(ValueError, 'result_not_complete'):
            dorothy.result('native-error', self.runtime)

    def test_six_slots_and_shared_nested_directory_exclusion(self):
        self.submit('exclusive', 'blocked')
        self.wait_started('exclusive')
        other = self.cli('submit', 'other', '--task', self.task('other', mode='blocked', directory=self.other_work),
                         '--cwd', self.other_work)
        self.assertNotEqual(dorothy.request_slot(dorothy.checked_request(self.runtime / 'jobs/other')),
                            dorothy.request_slot(dorothy.checked_request(self.runtime / 'jobs/exclusive')))
        self.wait_started('other', self.other_work)
        nested = self.work / 'nested'
        nested.mkdir()
        for cwd in (self.work, nested, self.root):
            with self.subTest(cwd=str(cwd)):
                with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                    with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, cwd):
                        pass
        same = self.cli('submit', 'same-dir', '--task', self.task('same-dir'),
                        '--cwd', self.work, expected=1)
        self.assertEqual(same['error'], 'workspace_busy')
        self.assertFalse((self.runtime / 'jobs/same-dir').exists())
        for job in ('exclusive', 'other'):
            self.cli('cancel', job)
            self.assertEqual(self.wait_finished(job)['status'], 'cancelled')
        self.assert_locks_released()

    def test_seven_distinct_directories_leave_the_seventh_busy(self):
        for index in range(dorothy.SLOTS):
            directory = self.root / ('slot-%d' % index)
            directory.mkdir()
            job = 'slot-%d' % index
            self.cli('submit', job, '--task', self.task(job, 'blocked', directory),
                     '--cwd', directory, '--timeout', 30)
            self.wait_started(job, directory)
        seventh = self.root / 'slot-seventh'
        seventh.mkdir()
        busy = self.cli('submit', 'slot-seventh', '--task', self.task('slot-seventh', 'blocked', seventh),
                        '--cwd', seventh, '--timeout', 30, expected=1)
        self.assertEqual(busy['error'], 'dorothy_busy')
        self.assertFalse((self.runtime / 'jobs/slot-seventh').exists())
        for index in range(dorothy.SLOTS):
            self.cli('cancel', 'slot-%d' % index)
        for index in range(dorothy.SLOTS):
            self.assertEqual(self.wait_finished('slot-%d' % index)['status'], 'cancelled')
        self.assert_locks_released()

    def test_followup_keeps_native_session_cwd_and_rejects_old_parent(self):
        self.submit('first')
        self.assertEqual(self.wait_finished('first')['status'], 'completed')
        first = dorothy.result('first', self.runtime)
        changed = self.cli('followup', 'wrong-cwd', '--from-job', 'first',
                           '--task', self.task('wrong-cwd'), '--cwd', self.other_work, expected=1)
        self.assertEqual(changed['error'], 'cwd_mismatch')
        second = self.cli('followup', 'second', '--from-job', 'first', '--task', self.task('second'))
        self.assertEqual(second['cwd'], str(self.work))
        self.assertEqual(second['parent_job'], 'first')
        self.assertEqual(second['root_job'], 'first')
        self.assertEqual(self.wait_finished('second')['status'], 'completed')
        current = dorothy.result('second', self.runtime)
        report = json.loads(current['report'])
        self.assertEqual(report['labels'], ['first', 'second'])
        self.assertEqual(report['conversation_id'], first['conversation_id'])
        self.assertEqual(report['cwd'], str(self.work))
        started = json.loads((self.work / 'second.started.json').read_text())
        self.assertEqual(started['requested_session'], first['conversation_id'])
        first_request = dorothy.checked_request(self.runtime / 'jobs/first')
        second_request = dorothy.checked_request(self.runtime / 'jobs/second')
        self.assertEqual(second_request['native_dir'], first_request['native_dir'])
        self.assertFalse(Path(second_request['native_dir']).exists())
        reused = self.cli('followup', 'second', '--from-job', 'first', '--task', self.root / 'second.json')
        self.assertTrue(reused['reused'])
        stale = self.cli('followup', 'stale', '--from-job', 'first', '--task', self.task('stale'), expected=1)
        self.assertEqual(stale['error'], 'parent_is_not_latest_turn')
        self.assertEqual(dorothy.result('first', self.runtime)['report_sha256'], first['report_sha256'])
        self.assert_locks_released()

    def test_cancel_cleans_worker_and_native_and_retains_partial_output(self):
        self.submit('cancelled', 'blocked')
        started = self.wait_started('cancelled')
        request = self.cli('cancel', 'cancelled')
        self.assertTrue(request['cancel_requested'])
        value = self.wait_finished('cancelled')
        self.assertEqual(value['status'], 'cancelled')
        self.assertEqual(dorothy.group_members(started['pgid']), [])
        self.assertTrue(value['cleanup']['native_archive'])
        self.assertIn('尚未完成的调查', (self.runtime / 'jobs/cancelled/partial.md').read_text())
        self.assertFalse(self.cli('cancel', 'cancelled')['cancel_requested'])
        with self.assertRaisesRegex(ValueError, 'result_not_complete'):
            dorothy.result('cancelled', self.runtime)
        self.assert_locks_released()

    def test_timeout_cannot_promote_partial_output_to_complete(self):
        self.submit('timeout', 'blocked', timeout=1)
        started = self.wait_started('timeout')
        value = self.wait_finished('timeout')
        self.assertEqual(value['status'], 'timeout')
        self.assertEqual(dorothy.group_members(started['pgid']), [])
        self.assertTrue((self.runtime / 'jobs/timeout/partial.md').is_file())
        with self.assertRaisesRegex(ValueError, 'result_not_complete'):
            dorothy.result('timeout', self.runtime)
        self.assert_locks_released()

    def test_terminal_result_does_not_hide_a_nonexiting_process(self):
        self.submit('still-running', 'result_then_wait', timeout=1)
        self.wait_started('still-running')
        self.assertEqual(self.wait_finished('still-running')['status'], 'timeout')
        with self.assertRaisesRegex(ValueError, 'result_not_complete'):
            dorothy.result('still-running', self.runtime)
        self.assert_locks_released()

    def test_exited_leader_does_not_leave_its_background_child_running(self):
        self.submit('background-child', 'leader_child')
        started = self.wait_started('background-child')
        marker = self.work / 'background-child.child.pid'
        deadline = time.monotonic() + 5
        while not marker.is_file() and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(marker.is_file(), 'fake CLI failed to create its background child')
        child_pid = int(marker.read_text())
        child = self.remember_process(child_pid)
        self.assertIsNotNone(child)
        self.assertEqual(child['pgid'], started['pgid'])
        value = self.wait_finished('background-child')
        self.assertEqual(value['status'], 'completed')
        self.assertEqual(dorothy.group_members(started['pgid']), [])
        self.assertFalse(dorothy.process_matches(child))
        self.assert_locks_released()

    def test_sigkill_worker_keeps_leases_with_native_until_actual_exit(self):
        self.submit('orphan', 'blocked', timeout=30)
        started = self.wait_started('orphan')
        state = dorothy.read_json(self.runtime / 'jobs/orphan/state.json')
        worker_identity = state['worker']
        self.assertTrue(dorothy.process_matches(worker_identity))
        os.kill(worker_identity['pid'], signal.SIGKILL)
        deadline = time.monotonic() + 5
        while dorothy.process_matches(worker_identity) and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertFalse(dorothy.process_matches(worker_identity))
        value = dorothy.status('orphan', self.runtime)
        self.assertEqual(value['status'], 'orphaned')
        self.assertEqual(value['cleanup']['native_group'], 'running')
        slot = dorothy.request_slot(dorothy.checked_request(self.runtime / 'jobs/orphan'))
        with self.assertRaisesRegex(ValueError, 'dorothy_busy'):
            with dorothy.locked(dorothy.slot_lock_path(self.runtime, slot)):
                pass
        with self.assertRaisesRegex(ValueError, 'workspace_busy'):
            with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, self.work):
                pass
        with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, self.other_work):
            pass
        with self.assertRaisesRegex(ValueError, 'result_not_complete'):
            dorothy.result('orphan', self.runtime)
        # Let the test-owned native process finish; no stale-PID cancellation is used.
        (self.work / 'orphan.release').write_text('release', encoding='utf-8')
        deadline = time.monotonic() + 5
        while dorothy.group_members(started['pgid']) and time.monotonic() < deadline:
            time.sleep(0.03)
        self.assertEqual(dorothy.group_members(started['pgid']), [])
        self.assertEqual(dorothy.status('orphan', self.runtime)['status'], 'interrupted')
        self.assert_locks_released()

    def test_long_chinese_report_preserves_all_bytes_and_can_be_read_repeatedly(self):
        self.submit('long-report', 'long')
        self.assertEqual(self.wait_finished('long-report')['status'], 'completed')
        value = dorothy.result('long-report', self.runtime)
        expected = LONG_TEXT.encode('utf-8')
        self.assertGreater(len(expected), 4 * 1024 * 1024)
        self.assertEqual(value['report'].encode('utf-8'), expected)
        self.assertEqual(value['report_bytes'], len(expected))
        self.assertEqual(value['report_sha256'], hashlib.sha256(expected).hexdigest())
        self.assertEqual(Path(value['report_path']).read_bytes(), expected)
        self.assertEqual(dorothy.result('long-report', self.runtime)['report'], LONG_TEXT)
        self.assertEqual(len((self.work / 'fixture-launches.jsonl').read_text().splitlines()), 1)
        self.assert_locks_released()

    def test_failed_or_incomplete_cli_never_publishes_a_report(self):
        for job, mode, expected in (('bad-exit', 'exit_failure', 'process_failed'),
                                    ('no-result', 'missing_result', 'incomplete')):
            with self.subTest(mode=mode):
                self.submit(job, mode)
                self.assertEqual(self.wait_finished(job)['status'], expected)
                with self.assertRaisesRegex(ValueError, 'result_not_complete'):
                    dorothy.result(job, self.runtime)
                self.assertTrue((self.runtime / 'jobs' / job / 'partial.md').is_file())
                self.assertFalse((self.runtime / 'jobs' / job / 'report.md').exists())
        self.assert_locks_released()

    def test_failed_followup_launch_keeps_last_resumable_parent_available(self):
        self.submit('resume-base')
        self.assertEqual(self.wait_finished('resume-base')['status'], 'completed')
        original_popen = subprocess.Popen

        def refuse_worker(arguments, *args, **kwargs):
            if '_worker' in arguments:
                raise OSError('fixture worker could not start')
            return original_popen(arguments, *args, **kwargs)

        with patch.object(dorothy.subprocess, 'Popen', side_effect=refuse_worker):
            failed = dorothy.submit('launch-failed', self.task('launch-failed'),
                                    parent='resume-base', runtime=self.runtime)
        self.remember_job('launch-failed')
        self.assertEqual(failed['status'], 'launch_failed')
        # No native invocation happened; the valid parent must remain usable.
        resumed = self.cli('followup', 'after-launch-failure', '--from-job', 'resume-base',
                           '--task', self.task('after-launch-failure'))
        self.assertFalse(resumed['reused'])
        self.assertEqual(self.wait_finished('after-launch-failure')['status'], 'completed')
        labels = json.loads(dorothy.result('after-launch-failure', self.runtime)['report'])['labels']
        self.assertEqual(labels, ['resume-base', 'after-launch-failure'])

    def callback_fixture(self, job):
        dest = self.runtime / 'jobs' / job
        dest.mkdir(parents=True)
        task = b'fixture callback task'
        (dest / 'task.md').write_bytes(task)
        binding = {'protocol': dorothy.ipc.PROTOCOL, 'socket': str(self.root / 'unused.sock'),
                   'target': {'project': 'fixture-atlas', 'session_key': 'fixture-session',
                              'session_id': 'fixture-host', 'agent_session_id': 'fixture-agent',
                              'cwd': str(self.work)}}
        request = {'task_sha256': dorothy.digest(task), 'timeout': 1,
                   'callback_target': binding, 'delivery': 'conversation'}
        dorothy.write_json(dest / 'request.json', request)
        dorothy.write_json(dest / 'callback.json', {'status': 'waiting_task'})
        return dest, request

    def test_uncertain_callback_keeps_unknown_state_at_watcher_deadline(self):
        dest, _ = self.callback_fixture('uncertain-delivery')
        finished = {'status': 'process_failed',
                    'cleanup': {'worker': 'absent', 'native_group': 'absent'}}
        with patch.object(dorothy, 'status', return_value=finished), \
                patch.object(dorothy.ipc, 'call', side_effect=dorothy.ipc.DeliveryUnknown()), \
                patch.object(dorothy.time, 'monotonic', side_effect=[0, 0, 10000]), \
                patch.object(dorothy.time, 'sleep'):
            dorothy.watch('uncertain-delivery', self.runtime)
        state = dorothy.read_json(dest / 'callback.json')
        self.assertEqual(state['status'], 'unknown')
        self.assertTrue(state.get('query_only'))

    def test_launch_receipt_write_failure_does_not_overwrite_running_worker_state(self):
        task = self.task('launch-receipt-failure', 'blocked')
        marker = self.work / 'launch-receipt-failure.started.json'
        original_write = dorothy.write_json
        observed = {}

        def fail_launch_receipt(path, value):
            if Path(path).name == 'launch.json':
                # Hold only the submitting process until its real detached worker
                # has started the fixture. Then fail this optional launch ledger.
                deadline = time.monotonic() + 6
                while time.monotonic() < deadline:
                    state_path = self.runtime / 'jobs/launch-receipt-failure/state.json'
                    state = dorothy.read_json(state_path)
                    if marker.is_file() and state.get('conversation_id') and state.get('native_pid'):
                        observed.update(state)
                        self.identities[state['worker']['pid']] = state['worker']
                        self.remember_process(state['native_pid'])
                        raise OSError('fixture launch receipt write failed after process start')
                    time.sleep(0.02)
                self.fail('fixture worker did not start before launch-ledger failure')
            return original_write(path, value)

        try:
            with patch.object(dorothy, 'write_json', side_effect=fail_launch_receipt), \
                    warnings.catch_warnings():
                warnings.simplefilter('ignore', ResourceWarning)
                try:
                    dorothy.submit('launch-receipt-failure', task, cwd=self.work,
                                    timeout=15, runtime=self.runtime)
                except OSError:
                    # Returning a save error is acceptable; erasing a live
                    # worker's identity or claiming no launch is not.
                    pass
        finally:
            self.remember_job('launch-receipt-failure')
        self.assertTrue(observed, 'the save failure must happen after real native startup')
        self.assertTrue(dorothy.process_matches(observed['worker']))
        self.assertTrue(dorothy.group_members(observed['native_pid']))
        value = dorothy.status('launch-receipt-failure', self.runtime)
        self.assertIn(value['status'], dorothy.ACTIVE)
        self.assertEqual(value['cleanup']['worker'], 'running')
        self.assertEqual(value['cleanup']['native_group'], 'running')
        self.cli('cancel', 'launch-receipt-failure')
        self.assertEqual(self.wait_finished('launch-receipt-failure')['status'], 'cancelled')

    def test_native_spawn_failure_preserves_parent_and_removes_unstarted_temp_store(self):
        self.submit('native-failure-base')
        self.assertEqual(self.wait_finished('native-failure-base')['status'], 'completed')
        wrapper = self.root / 'worker-with-native-spawn-failure.py'
        wrapper.write_text(
            'import subprocess, sys\n'
            'sys.path.insert(0, ' + repr(str(ENTRY.parent)) + ')\n'
            'import dorothy\n'
            'original = subprocess.Popen\n'
            'def refuse_native(arguments, *args, **kwargs):\n'
            '    if arguments[0] == "/usr/bin/sandbox-exec":\n'
            '        raise OSError("fixture native process could not start")\n'
            '    return original(arguments, *args, **kwargs)\n'
            'dorothy.subprocess.Popen = refuse_native\n'
            'dorothy.main()\n', encoding='utf-8')
        original_popen = subprocess.Popen

        def instrument_worker(arguments, *args, **kwargs):
            if '_worker' in arguments:
                arguments = list(arguments)
                arguments[arguments.index(str(ENTRY))] = str(wrapper)
            return original_popen(arguments, *args, **kwargs)

        try:
            with patch.object(dorothy.subprocess, 'Popen', side_effect=instrument_worker), \
                    warnings.catch_warnings():
                warnings.simplefilter('ignore', ResourceWarning)
                dorothy.submit('native-spawn-failed', self.task('native-spawn-failed'),
                                parent='native-failure-base', runtime=self.runtime)
        finally:
            self.remember_job('native-spawn-failed')
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            value = dorothy.status('native-spawn-failed', self.runtime)
            if value['status'] not in dorothy.ACTIVE and value['cleanup']['worker'] == 'absent':
                break
            time.sleep(0.03)
        self.assertNotIn(value['status'], dorothy.ACTIVE | {'completed'})
        self.assertEqual(value['cleanup']['worker'], 'absent')
        self.assertEqual(value['cleanup']['native_group'], 'absent')
        self.assertFalse((self.work / 'native-spawn-failed.started.json').exists())
        request = dorothy.checked_request(self.runtime / 'jobs/native-spawn-failed')
        self.assertFalse(Path(request['native_dir']).exists())
        saved = dorothy.read_json(self.runtime / 'jobs/native-spawn-failed/state.json')
        self.assertFalse(saved['native_store_preserved'])
        self.assert_locks_released()
        self.cli('followup', 'after-native-failure', '--from-job', 'native-failure-base',
                 '--task', self.task('after-native-failure'))
        self.assertEqual(self.wait_finished('after-native-failure')['status'], 'completed')
        labels = json.loads(dorothy.result('after-native-failure', self.runtime)['report'])['labels']
        self.assertEqual(labels, ['native-failure-base', 'after-native-failure'])

    def test_initial_state_save_failure_does_not_publish_or_block_other_jobs(self):
        original_write = dorothy.write_json

        def refuse_initial_state(path, value):
            if Path(path).name == 'state.json' and value.get('status') == 'queued':
                raise OSError('fixture initial state could not be saved')
            return original_write(path, value)

        try:
            with patch.object(dorothy, 'write_json', side_effect=refuse_initial_state):
                with self.assertRaises(OSError):
                    dorothy.submit('partial-submit', self.task('partial-submit'),
                                    cwd=self.work, runtime=self.runtime)
        finally:
            self.remember_job('partial-submit')
        self.assertFalse((self.runtime / 'jobs/partial-submit').exists())
        self.assertEqual(list((self.runtime / 'jobs').glob('.partial-*')), [])
        self.assertFalse((self.work / 'partial-submit.started.json').exists())
        self.submit('after-save-failure')
        self.assertEqual(self.wait_finished('after-save-failure')['status'], 'completed')
        self.assertEqual(len((self.work / 'fixture-launches.jsonl').read_text().splitlines()), 1)
        self.assert_locks_released()

    def test_queued_without_identity_distinguishes_submission_from_dead_start(self):
        dest = self.runtime / 'jobs/queued-start'
        dest.mkdir(parents=True)
        task = b'fixture queued task with no process'
        (dest / 'task.md').write_bytes(task)
        dorothy.write_json(dest / 'request.json', {
            'task_sha256': dorothy.digest(task), 'cwd': str(self.work),
            'root_job': 'queued-start', 'created_at': dorothy.now(),
        })
        dorothy.write_json(dest / 'state.json', {'status': 'queued'})
        with dorothy.locked(self.runtime / 'run.lock'):
            accepting = dorothy.status('queued-start', self.runtime)
            self.assertEqual(accepting['status'], 'queued')
            self.assertEqual(accepting['cleanup']['worker'], 'unknown')
        abandoned = dorothy.status('queued-start', self.runtime)
        self.assertEqual(abandoned['status'], 'interrupted')
        self.assertEqual(abandoned['cleanup']['worker'], 'absent')
        dorothy.write_json(dest / 'launch.json', {'worker': None, 'pid': 2147483647})
        with dorothy.locked(self.runtime / 'run.lock'):
            dead_start = dorothy.status('queued-start', self.runtime)
            self.assertEqual(dead_start['status'], 'interrupted')
            self.assertEqual(dead_start['cleanup']['worker'], 'absent')


if __name__ == '__main__':
    unittest.main()
