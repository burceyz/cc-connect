"""Async instance identity, independent workers, and legacy receipt coverage."""
import json
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

import bridge
import instances
import jobs
import test_jobs


class JobInstanceTests(unittest.TestCase):
    setUp = test_jobs.JobTests.setUp
    tearDown = test_jobs.JobTests.tearDown
    wait = test_jobs.JobTests.wait
    submit = test_jobs.JobTests.submit

    def test_two_default_jobs_run_simultaneously_with_distinct_homes(self):
        self.fake.write_text('#!' + sys.executable + '\n'
                             'import os,sys,json,time\nfrom pathlib import Path\n'
                             'sys.stdin.read()\nhome=Path(os.environ["DSH_HOME"])\n'
                             '(home/"started").touch()\n'
                             'deadline=time.monotonic()+4\n'
                             'while not (home.parents[2]/"release").exists():\n'
                             ' if time.monotonic()>deadline: sys.exit(2)\n'
                             ' time.sleep(.02)\n'
                             'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                             'print(json.dumps({"type":"final","text":json.dumps({'
                             '"cwd":os.getcwd(),"home":str(home)})}))\n')
        first, second = self.submit('first'), self.submit('second')
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if len(list((self.runtime / 'instances').glob('*/home/started'))) == 2:
                break
            time.sleep(.02)
        else:
            (self.runtime / 'release').touch()
            self.fail('both independent DSH workers did not start')
        self.assertNotEqual(first['instance_id'], second['instance_id'])
        self.assertNotEqual(first['cwd'], second['cwd'])
        (self.runtime / 'release').touch()
        for key, record in [('first', first), ('second', second)]:
            self.assertEqual(self.wait(key)['status'], 'completed')
            actual = jobs.result(key, self.runtime)
            response = json.loads(actual['response'])
            self.assertEqual(response['cwd'], record['cwd'])
            self.assertEqual(response['home'], str(instances.root(self.runtime, record['instance_id']) / 'home'))
            self.assertIn('instance_bridge_lock', actual['cleanup'])
            self.assertNotIn('shared_bridge_lock', actual['cleanup'])

    def test_reused_request_allocates_no_second_instance(self):
        first = self.submit()
        self.assertTrue(self.submit()['reused'])
        self.assertEqual(len(list((self.runtime / 'instances').iterdir())), 1)
        self.assertEqual(self.wait('one')['status'], 'completed')
        self.assertEqual(self.submit()['instance_id'], first['instance_id'])
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit(instance_id=None)

    def test_receipt_and_run_instance_mismatch_are_rejected(self):
        self.submit()
        record = self.wait('one')
        other = instances.allocate(self.runtime)
        receipt_path = self.runtime / 'jobs/one/receipt.json'
        original = json.loads(receipt_path.read_text())
        bridge.write_json(receipt_path, original | {'instance_id': other})
        with self.assertRaisesRegex(ValueError, 'instance_mismatch'):
            jobs.status('one', self.runtime)
        bridge.write_json(receipt_path, original)
        run_path = Path(record['run_dir']) / 'result.json'
        run = json.loads(run_path.read_text())
        # A plausible different instance/cwd pair still must match the request.
        run.update(instance_id=other, cwd=str(instances.root(self.runtime, other) / 'work'))
        bridge.write_json(run_path, run)
        with self.assertRaisesRegex(ValueError, 'instance_mismatch'):
            jobs.result('one', self.runtime)

    def test_preallocated_instance_is_retained_by_worker(self):
        selected = instances.allocate(self.runtime)
        record = self.submit(instance_id=selected)
        self.assertEqual(record['instance_id'], selected)
        self.assertEqual(self.wait('one')['status'], 'completed')
        _, run = bridge.load_run(jobs.result('one', self.runtime)['run_dir'], self.runtime)
        self.assertEqual(run['instance_id'], selected)
        self.assertEqual(len(list((self.runtime / 'instances').iterdir())), 1)

    def test_legacy_job_creates_no_instance_and_reuse_keeps_original_request(self):
        self.submit(instance_id=None)
        self.assertEqual(self.wait('one')['status'], 'completed')
        request = self.runtime / 'jobs/one/request.json'
        before = request.read_bytes()
        self.assertNotIn('instance_id', json.loads(before))
        self.assertTrue(self.submit()['reused'])
        self.assertEqual(request.read_bytes(), before)
        self.assertFalse((self.runtime / 'instances').exists())
        self.assertIn('shared_bridge_lock', jobs.result('one', self.runtime)['cleanup'])

    def test_status_rejects_receipt_cwd_mismatch(self):
        self.submit()
        self.assertEqual(self.wait('one')['status'], 'completed')
        path = self.runtime / 'jobs/one/receipt.json'
        receipt = json.loads(path.read_text())
        bridge.write_json(path, receipt | {'cwd': str(self.runtime / 'unexpected')})
        with self.assertRaisesRegex(ValueError, 'cwd_mismatch'):
            jobs.status('one', self.runtime)

    def test_workspace_lease_held_by_another_run_is_only_context(self):
        self.submit()
        record = self.wait('one')
        def probe(path):
            return 'held' if path.parent.name == 'workspaces' else 'released'
        with patch.object(jobs, 'pid_probe', return_value='absent'), \
             patch.object(jobs, 'lock_probe', side_effect=probe):
            checked = jobs.status('one', self.runtime)['cleanup']
        self.assertEqual(checked['workspace_lock'], 'held')
        self.assertEqual(checked['state'], 'released')


if __name__ == '__main__':
    unittest.main()
