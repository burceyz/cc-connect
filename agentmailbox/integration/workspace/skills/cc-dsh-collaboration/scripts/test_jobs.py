import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import bridge
import instances
import jobs

# These legacy suites never touch a live mailbox service.
os.environ.setdefault('AGENT_MAILBOX_DISABLE', '1')


class JobTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name)
        (self.runtime / bridge.PATCH_NAME).write_text('test')
        self.task = self.runtime / 'input.md'
        self.task.write_text('仅返回中文 $() `literal`')
        self.fake = self.runtime / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\nimport sys,time,json\n'
                             's=sys.stdin.read()\ntime.sleep(0.4)\n'
                             'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                             'print(json.dumps({"type":"final","text":s}))\n')
        self.fake.chmod(0o700)

    def tearDown(self):
        # Reap completed children of this test process without waiting on live jobs.
        while True:
            try:
                if os.waitpid(-1, os.WNOHANG)[0] == 0:
                    break
            except ChildProcessError:
                break
        self.temp.cleanup()

    def wait(self, key):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            r = jobs.status(key, self.runtime)
            if r['status'] not in jobs.ACTIVE:
                return r
            time.sleep(0.03)
        self.fail('worker did not finish')

    def submit(self, key='one', timeout=3, instance_id=instances.AUTO):
        return jobs.submit(self.task, key, timeout, self.runtime, str(self.fake), instance_id=instance_id)

    def test_detached_worker_survives_submitter_exit(self):
        script = ('import sys;sys.path.insert(0,sys.argv[1]);import jobs;from pathlib import Path;'
                  'jobs.submit(Path(sys.argv[2]),"detached",3,Path(sys.argv[3]),sys.argv[4])')
        subprocess.run([sys.executable, '-c', script, str(Path(jobs.__file__).parent),
                        str(self.task), str(self.runtime), str(self.fake)],
                       check=True, timeout=2, cwd=bridge.WORKSPACE)
        self.assertEqual(self.wait('detached')['status'], 'completed')
        self.assertEqual(jobs.result('detached', self.runtime)['response'], self.task.read_text())

    def test_idempotency_and_conflicting_reuse(self):
        self.assertEqual(self.submit()['status'], 'accepted')
        self.assertTrue(self.submit()['reused'])
        self.task.write_text('different')
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit()
        self.assertEqual(self.wait('one')['status'], 'completed')
        self.assertEqual(len(list((self.runtime / 'runs').iterdir())), 1)

    def test_timeout_and_no_final_cannot_be_results(self):
        self.submit('timeout', timeout=0.1)
        self.assertEqual(self.wait('timeout')['status'], 'timeout')
        with self.assertRaisesRegex(ValueError, 'job_not_completed'):
            jobs.result('timeout', self.runtime)
        self.fake.write_text('#!' + sys.executable + '\nimport sys\nsys.stdin.read()\n')
        self.submit('empty')
        self.assertEqual(self.wait('empty')['status'], 'incomplete')

    def test_busy_does_not_queue_or_retry(self):
        with bridge.locked(self.runtime):
            self.submit(instance_id=None)
            r = self.wait('one')
        self.assertEqual(r['code'], 'bridge_busy')
        self.assertFalse((self.runtime / 'runs').exists())
        self.assertTrue(self.submit()['reused'])
        self.assertEqual(jobs.status('one', self.runtime)['status'], 'failed')

    def test_dead_worker_is_orphaned_and_keeps_dsh_global_lock(self):
        self.fake.write_text('#!' + sys.executable + '\nimport sys,time\nsys.stdin.read()\ntime.sleep(10)\n')
        self.submit(instance_id=None)
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            r = jobs.status('one', self.runtime)
            if r.get('run_dir'):
                run = json.loads((Path(r['run_dir']) / 'result.json').read_text())
                if run.get('child_pid'):
                    break
            time.sleep(0.02)
        else:
            self.fail('no child pid')
        try:
            os.kill(r['worker_pid'], signal.SIGKILL)
            os.waitpid(r['worker_pid'], 0)
            self.assertEqual(jobs.status('one', self.runtime)['status'], 'orphaned')
            with self.assertRaisesRegex(ValueError, 'bridge_busy'):
                with bridge.locked(self.runtime):
                    pass
        finally:
            os.killpg(run['child_pid'], signal.SIGTERM)

    def test_result_tampering_is_detected(self):
        self.submit()
        r = self.wait('one')
        (Path(r['run_dir']) / 'response.md').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'artifact_modified'):
            jobs.result('one', self.runtime)

    def test_invalid_ids_and_timeouts(self):
        for key in ['../escape', '/tmp/out', '']:
            with self.assertRaisesRegex(ValueError, 'invalid_job_id'):
                self.submit(key)
        for seconds in [0, -1, float('nan'), float('inf')]:
            with self.assertRaisesRegex(ValueError, 'invalid_timeout'):
                self.submit(timeout=seconds)

    def test_failed_preparation_does_not_publish_partial_job(self):
        with patch.object(bridge, 'write_json', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                self.submit()
        self.assertFalse((self.runtime / 'jobs/one').exists())
        self.submit()
        self.assertEqual(self.wait('one')['status'], 'completed')

    def test_launch_failure_is_queryable_without_retry(self):
        with patch.object(jobs.subprocess, 'Popen', side_effect=OSError('failed')):
            self.assertEqual(self.submit()['status'], 'launch_failed')
        self.assertEqual(jobs.status('one', self.runtime)['status'], 'launch_failed')
        self.assertTrue(self.submit()['reused'])

    def test_bridge_rejects_nonfinite_timeout(self):
        for seconds in [float('nan'), float('inf')]:
            with self.assertRaisesRegex(ValueError, 'invalid_timeout'):
                bridge.run(self.task, None, seconds, self.runtime, str(self.fake))

    def test_completed_queries_include_exit_check_without_mutating_receipt(self):
        self.submit()
        r = self.wait('one')
        receipt = self.runtime / 'jobs/one/receipt.json'
        before = receipt.read_bytes()
        with patch.object(jobs, 'pid_probe', return_value='absent'), patch.object(jobs, 'lock_probe', return_value='released'):
            checked = jobs.result('one', self.runtime)
        self.assertEqual(checked['cleanup']['state'], 'released')
        self.assertEqual(checked['cleanup']['dsh_returncode'], 0)
        self.assertEqual(receipt.read_bytes(), before)
        self.assertEqual(checked['run_dir'], r['run_dir'])

    def test_exit_check_flags_group_after_leader_disappears(self):
        record = {'status': 'completed', 'worker_pid': 123}
        with patch.object(jobs, 'lock_probe', return_value='released'):
            with patch.object(jobs, 'pid_probe', side_effect=['absent', 'absent', 'exists_unverified']):
                checked = jobs.cleanup_status(record, self.runtime, self.runtime)
        self.assertEqual(checked['state'], 'needs_review')

    def test_exit_check_handles_finishing_unknown_and_shared_lock(self):
        record = {'status': 'completed', 'worker_pid': 123}
        with patch.object(jobs, 'pid_probe', return_value='absent'):
            with patch.object(jobs, 'lock_probe', side_effect=['held', 'released']):
                self.assertEqual(jobs.cleanup_status(record, self.runtime, self.runtime)['state'], 'pending_exit')
            with patch.object(jobs, 'lock_probe', side_effect=['released', 'held']):
                report = jobs.cleanup_status(record, self.runtime, self.runtime)
                self.assertEqual(report['state'], 'released')
                self.assertEqual(report['shared_bridge_lock'], 'held')
        with patch.object(jobs, 'pid_probe', return_value='unavailable'):
            with patch.object(jobs, 'lock_probe', return_value='released'):
                self.assertEqual(jobs.cleanup_status(record, self.runtime, self.runtime)['state'], 'unknown')

    def test_worker_records_failure_when_request_is_unreadable(self):
        # Regression: the worker read request.json inside the try but referenced
        # `request` in the except path, so a missing/corrupt request raised
        # UnboundLocalError and left the receipt stuck at "running".
        for label, content in [('missing', None), ('invalid', '{not json'), ('nondict', '[]')]:
            with self.subTest(label=label):
                key = 'req-' + label
                dest = self.runtime / 'jobs' / key
                dest.mkdir(parents=True)
                bridge.write_json(dest / 'receipt.json', {'status': 'accepted', 'job_id': key})
                if content is not None:
                    (dest / 'request.json').write_text(content)
                lease_fd = os.open(dest / 'lease.lock', os.O_RDWR | os.O_CREAT, 0o600)
                jobs.worker(key, self.runtime, lease_fd)
                receipt = json.loads((dest / 'receipt.json').read_text())
                expected = 'request_missing' if label == 'missing' else 'request_invalid'
                self.assertEqual(receipt['status'], 'failed')
                self.assertEqual(receipt['code'], expected)
                self.assertTrue(receipt.get('ended'))
        self.assertFalse((self.runtime / 'runs').exists())

    def test_pid_probe_only_observes_and_does_not_claim_identity(self):
        with patch.object(jobs.os, 'kill') as kill:
            self.assertEqual(jobs.pid_probe(234), 'exists_unverified')
            kill.assert_called_once_with(234, 0)
            for pid in [None, -1, 0, 1, True, '234']:
                self.assertEqual(jobs.pid_probe(pid), 'not_recorded')
            kill.assert_called_once()
        with patch.object(jobs.os, 'killpg', side_effect=ProcessLookupError) as killpg:
            self.assertEqual(jobs.pid_probe(234, group=True), 'absent')
            killpg.assert_called_once_with(234, 0)
        with patch.object(jobs.os, 'kill', side_effect=PermissionError):
            self.assertEqual(jobs.pid_probe(234), 'unavailable')

    def test_lock_probe_does_not_create_file_and_detects_held_lock(self):
        path = self.runtime / 'missing.lock'
        self.assertEqual(jobs.lock_probe(path), 'unavailable')
        self.assertFalse(path.exists())
        with bridge.locked(self.runtime):
            self.assertEqual(jobs.lock_probe(self.runtime / 'bridge.lock'), 'held')
        self.assertEqual(jobs.lock_probe(self.runtime / 'bridge.lock'), 'released')


if __name__ == '__main__':
    unittest.main()
