"""Actual bridge/worker subprocess regressions without model or network calls."""
from contextlib import redirect_stdout, ExitStack
import io
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

import bridge
import instances
import jobs


FAKE = r'''
import json, os, pathlib, subprocess, sys, time
task_text = sys.stdin.read()
task = json.loads(task_text)
home = pathlib.Path(os.environ['DSH_HOME'])
sid = 'session-fixture'
print(json.dumps({'type':'session','sessionId':sid}), flush=True)
pathlib.Path(task['marker']).write_text(json.dumps({'pid':os.getpid(), 'started':time.monotonic()}))
time.sleep(task['delay'])
final = json.dumps({'token':task['token'],'home':str(home),'cwd':os.getcwd()})
store = home / 'fixture-rows.json'
rows = json.loads(store.read_text()) if '--session-id' in sys.argv else [
    {'type':'session','id':sid,'cwd':os.getcwd(),'seq':0}]
seq = rows[-1]['seq'] + 1
rows.extend([
    {'type':'user/message','seq':seq,'data':{'source':{'kind':'user'},'content':[{'type':'text','text':task_text}]}},
    {'type':'assistant/message','seq':seq+1,'data':{'message':{'content':[{'type':'text','text':final}]}}}])
store.write_text(json.dumps(rows))
source = home / 'sessions/work' / sid / 'session.v4.jsonl.zstd'
source.parent.mkdir(parents=True, exist_ok=True)
encoded = ('\n'.join(json.dumps(row) for row in rows) + '\n').encode()
source.write_bytes(subprocess.run(['zstd','-q','-c'],input=encoded,stdout=subprocess.PIPE,check=True).stdout)
pathlib.Path(task['marker'] + '.ended').write_text(str(time.monotonic()))
print(json.dumps({'type':'final','text':final}), flush=True)
'''


@unittest.skipUnless(shutil.which('zstd'), 'native fixture archive requires zstd')
class MultiFailureTests(unittest.TestCase):
    def setUp(self):
        # The copy under test may live outside the installed workspace, so anchor
        # its temp area to this file's tree instead of the product path constant.
        workspace = Path(__file__).resolve().parents[3]
        self.temp = tempfile.TemporaryDirectory(prefix='.multi-failure-', dir=workspace)
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('fixture-config')
        self.fake = self.root / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\n' + FAKE)
        self.fake.chmod(0o700)
        self.worker_pids = set()
        self.dsh_pids = set()

    def tearDown(self):
        for pid in self.dsh_pids:
            try:
                os.killpg(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
        for pid in self.worker_pids:
            try:
                os.kill(pid, signal.SIGTERM)
                os.waitpid(pid, 0)
            except (ChildProcessError, ProcessLookupError):
                pass
        self.temp.cleanup()

    def task(self, token, delay=0):
        path = self.root / (token + '.md')
        path.write_text(json.dumps({'token':token, 'delay':delay,
                                   'marker':str(self.root / (token + '.started'))}))
        return path

    def submit(self, token, delay, timeout=5):
        # jobs intentionally detaches Popen; this fixture reaps workers by PID.
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', ResourceWarning)
            record = jobs.submit(self.task(token, delay), token, timeout,
                                 self.runtime, str(self.fake))
        self.worker_pids.add(record['worker_pid'])
        return record

    def wait_started(self, token):
        path = self.root / (token + '.started')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if path.is_file():
                value = json.loads(path.read_text())
                self.dsh_pids.add(value['pid'])
                return value
            time.sleep(0.01)
        self.fail('fake DSH did not start: ' + token)

    def wait_finished(self, token):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            record = jobs.status(token, self.runtime)
            if record['status'] not in jobs.ACTIVE:
                pid = record['worker_pid']
                os.waitpid(pid, 0)
                self.worker_pids.discard(pid)
                if record.get('run_dir'):
                    _, run = bridge.load_run(record['run_dir'], self.runtime)
                    self.dsh_pids.discard(run.get('child_pid'))
                return jobs.status(token, self.runtime)
            time.sleep(0.01)
        self.fail('worker did not finish: ' + token)

    def test_three_real_workers_overlap_with_independent_homes_and_results(self):
        tokens = ['parallel-a', 'parallel-b', 'parallel-c']
        receipts = [self.submit(token, 0.8) for token in tokens]
        starts = [self.wait_started(token)['started'] for token in tokens]
        results = [self.wait_finished(token) for token in tokens]
        ends = [float((self.root / (token + '.started.ended')).read_text()) for token in tokens]
        self.assertGreater(min(ends), max(starts))
        self.assertEqual(len({r['instance_id'] for r in receipts}), 3)
        homes = set()
        for token, record in zip(tokens, results):
            self.assertEqual(record['status'], 'completed')
            response = json.loads(jobs.result(token, self.runtime)['response'])
            self.assertEqual(response['token'], token)
            homes.add(response['home'])
            self.assertEqual(record['cleanup']['state'], 'released')
        self.assertEqual(len(homes), 3)

    def test_one_timeout_does_not_interrupt_another_worker(self):
        self.submit('times-out', 30, timeout=2)
        self.submit('survives', 4, timeout=8)
        self.wait_started('times-out')
        peer = self.wait_started('survives')
        failed = self.wait_finished('times-out')
        self.assertEqual(failed['status'], 'timeout')
        self.assertEqual(jobs.pid_probe(peer['pid']), 'exists_unverified')
        self.assertEqual(self.wait_finished('survives')['status'], 'completed')

    def test_killed_worker_leaves_three_dsh_leases_but_other_instance_runs(self):
        receipt = self.submit('orphan', 30, timeout=60)
        child = self.wait_started('orphan')['pid']
        os.kill(receipt['worker_pid'], signal.SIGKILL)
        os.waitpid(receipt['worker_pid'], 0)
        self.worker_pids.discard(receipt['worker_pid'])
        record = jobs.status('orphan', self.runtime)
        self.assertEqual(record['status'], 'orphaned')
        self.assertEqual(record['cleanup']['state'], 'needs_review')
        original = receipt['instance_id']
        with self.assertRaisesRegex(ValueError, 'bridge_busy'):
            with instances.locked(self.runtime, original):
                pass
        other = [instances.allocate(self.runtime) for _ in range(instances.CAPACITY)]
        with self.assertRaisesRegex(ValueError, 'workspace_busy'):
            with instances.execution(self.runtime, other[0], Path(receipt['cwd'])):
                pass
        with ExitStack() as stack:
            for value in other[:instances.CAPACITY - 1]:
                stack.enter_context(instances.execution(self.runtime, value,
                                                        instances.root(self.runtime, value) / 'work'))
            with self.assertRaisesRegex(ValueError, 'capacity_busy'):
                busy = other[instances.CAPACITY - 1]
                with instances.execution(self.runtime, busy, instances.root(self.runtime, busy) / 'work'):
                    pass
        self.submit('orphan-peer', 0)
        self.wait_started('orphan-peer')
        self.assertEqual(self.wait_finished('orphan-peer')['status'], 'completed')
        os.killpg(child, signal.SIGTERM)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if jobs.lock_probe(instances.root(self.runtime, original) / 'bridge.lock') == 'released':
                break
            time.sleep(0.01)
        self.assertEqual(jobs.lock_probe(instances.root(self.runtime, original) / 'bridge.lock'), 'released')
        self.dsh_pids.discard(child)
        with instances.execution(self.runtime, original, Path(receipt['cwd'])):
            pass

    def test_native_exports_and_resumes_stay_in_the_recorded_instance(self):
        with redirect_stdout(io.StringIO()):
            first, _ = bridge.run(self.task('first'), None, 5, self.runtime, str(self.fake))
            other, _ = bridge.run(self.task('other'), None, 5, self.runtime, str(self.fake))
            continued, status = bridge.run(self.task('continued'), first, 5, self.runtime, str(self.fake))
            manifest = bridge.export_run(continued, self.runtime)
            original_export = bridge.export_run(first, self.runtime)
            other_export = bridge.export_run(other, self.runtime)
        self.assertEqual(status, 'completed')
        first_record = bridge.load_run(first, self.runtime)[1]
        continued_record = bridge.load_run(continued, self.runtime)[1]
        self.assertEqual(first_record['instance_id'], continued_record['instance_id'])
        self.assertEqual(len(manifest['messages']), 4)
        self.assertEqual(len(original_export['messages']), 2)
        self.assertNotEqual(manifest['instance_id'], other_export['instance_id'])
        self.assertNotIn('continued', (Path(other_export['export_dir']) / 'transcript.md').read_text())

    def test_missing_default_work_can_export_but_cannot_resume(self):
        with redirect_stdout(io.StringIO()):
            dest, _ = bridge.run(self.task('removed-cwd'), None, 5, self.runtime, str(self.fake))
        record = bridge.load_run(dest, self.runtime)[1]
        Path(record['cwd']).rmdir()
        with redirect_stdout(io.StringIO()):
            self.assertEqual(bridge.export_run(dest, self.runtime)['status'], 'verified')
        with self.assertRaisesRegex(ValueError, 'cwd_not_directory'):
            bridge.run(self.task('cannot-resume'), dest, 5, self.runtime, str(self.fake))

    def test_legacy_native_export_and_resume_keep_legacy_home(self):
        with redirect_stdout(io.StringIO()):
            first, _ = bridge.run(self.task('legacy-first'), None, 5, self.runtime,
                                  str(self.fake), instance_id=None)
            second, status = bridge.run(self.task('legacy-second'), first, 5,
                                        self.runtime, str(self.fake))
            manifest = bridge.export_run(second, self.runtime)
        self.assertEqual(status, 'completed')
        self.assertNotIn('instance_id', bridge.load_run(second, self.runtime)[1])
        response = json.loads((second / 'response.md').read_text())
        self.assertEqual(response['home'], str(self.runtime / 'home'))
        self.assertNotIn('instance_id', manifest)
        self.assertEqual(len(manifest['messages']), 4)

    def test_resume_rechecks_identity_after_acquiring_leases(self):
        with redirect_stdout(io.StringIO()):
            first, _ = bridge.run(self.task('race-first'), None, 5, self.runtime, str(self.fake))
        original_load = bridge.load_run
        _, original = original_load(first, self.runtime)
        other = instances.allocate(self.runtime)
        changes = [{'instance_id': other}, {'cwd': str(instances.root(self.runtime, other) / 'work')}]
        for changed in changes:
            with self.subTest(changed=changed):
                records = [(first, original), (first, original | changed)]
                with patch.object(bridge, 'load_run', side_effect=records), \
                     patch.object(bridge, 'run_process') as process:
                    with self.assertRaisesRegex(ValueError, 'instance_mismatch'):
                        bridge.run(self.task('race-second'), first, 5, self.runtime, str(self.fake))
                    process.assert_not_called()

    def test_export_does_not_consume_capacity_or_lock_another_instance(self):
        with redirect_stdout(io.StringIO()):
            first, _ = bridge.run(self.task('export-only'), None, 5, self.runtime, str(self.fake))
        with ExitStack() as stack:
            for _ in range(instances.CAPACITY):
                value = instances.allocate(self.runtime)
                stack.enter_context(instances.execution(self.runtime, value,
                                                        instances.root(self.runtime, value) / 'work'))
            with redirect_stdout(io.StringIO()):
                self.assertEqual(bridge.export_run(first, self.runtime)['status'], 'verified')


if __name__ == '__main__':
    unittest.main()
