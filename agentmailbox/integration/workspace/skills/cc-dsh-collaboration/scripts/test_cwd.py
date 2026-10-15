"""Invocation directory, persistence and legacy compatibility checks."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import bridge
import instances
import jobs


class CwdTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='.cwd-test-', dir=bridge.WORKSPACE)
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test config')
        self.target = self.root / '知识库 with spaces'
        self.target.mkdir()
        self.other = self.root / 'docs'
        self.other.mkdir()
        self.task = self.root / 'task.md'
        self.task.write_text('echo cwd')
        self.fake = self.root / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\nimport os,sys,json\n'
                             'sys.stdin.read()\n'
                             'print(json.dumps({"type":"session","sessionId":"session-cwd"}))\n'
                             'print(json.dumps({"type":"final","text":json.dumps({'
                             '"cwd":os.getcwd(),"home":os.environ["DSH_HOME"]})}))\n')
        self.fake.chmod(0o700)

    def tearDown(self):
        self.temp.cleanup()

    def run_task(self, cwd=None, resume=None):
        with redirect_stdout(io.StringIO()):
            return bridge.run(self.task, resume, 3, self.runtime, str(self.fake), cwd=cwd)

    def submit(self, cwd=None, instance_id=instances.AUTO):
        return jobs.submit(self.task, 'cwd-test', 3, self.runtime, str(self.fake), cwd=cwd,
                           instance_id=instance_id)

    def wait_job(self):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            record = jobs.status('cwd-test', self.runtime)
            if record['status'] not in jobs.ACTIVE:
                os.waitpid(record['worker_pid'], 0)
                return record
            time.sleep(0.03)
        self.fail('worker did not finish')

    def test_custom_directory_and_runtime_are_separate(self):
        dest, state = self.run_task(str(self.target))
        self.assertEqual(state, 'completed')
        actual = json.loads((dest / 'response.md').read_text())
        record = bridge.load_run(dest, self.runtime)[1]
        root = bridge.instances.root(self.runtime, record['instance_id'])
        self.assertEqual(actual, {'cwd': str(self.target), 'home': str(root / 'home')})
        self.assertEqual(list(self.target.iterdir()), [])
        self.assertTrue(dest.is_relative_to(self.runtime))
        default, _ = self.run_task()
        other = bridge.load_run(default, self.runtime)[1]
        self.assertNotEqual(other['instance_id'], record['instance_id'])
        self.assertEqual(json.loads((default / 'response.md').read_text())['cwd'],
                         str(bridge.instances.root(self.runtime, other['instance_id']) / 'work'))

    def test_relative_path_and_symlink_normalize_to_real_directory(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.target, target_is_directory=True)
        relative = alias.relative_to(Path.cwd())
        dest, _ = self.run_task(str(relative))
        self.assertEqual(bridge.load_run(dest, self.runtime)[1]['cwd'], str(self.target))

    def test_invalid_targets_fail_before_launch(self):
        for target, error in [(self.root / 'missing', 'cwd_not_directory'),
                              (self.task, 'cwd_not_directory'), ('', 'invalid_cwd'),
                              ('/Users', 'cwd_outside_workspace'),
                              (bridge.WORKSPACE, 'cwd_too_broad'),
                              (bridge.WORKSPACE / 'workdir', 'cwd_too_broad'),
                              (bridge.WORKSPACE / '.worktrees', 'cwd_too_broad'),
                              (bridge.WORKSPACE / '.task-clones', 'cwd_too_broad'),
                              (bridge.WORKSPACE / '.local/dorothy-work', 'cwd_too_broad')]:
            with self.subTest(target=str(target)), self.assertRaisesRegex(ValueError, error):
                self.run_task(target)
        alias = self.root / 'outside'
        alias.symlink_to('/Users', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'cwd_outside_workspace'):
            self.run_task(alias)
        alias_workdir = self.root / 'alias-workdir'
        alias_workdir.symlink_to(bridge.WORKSPACE / 'workdir', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'cwd_too_broad'):
            self.run_task(alias_workdir)
        alias_dorothy_work = self.root / 'alias-dorothy-work'
        alias_dorothy_work.symlink_to(bridge.WORKSPACE / '.local/dorothy-work', target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'cwd_too_broad'):
            self.run_task(alias_dorothy_work)
        self.assertFalse((self.runtime / 'runs').exists())

    def test_resume_inherits_cwd_and_rejects_other_directory(self):
        first, _ = self.run_task(self.target)
        rows = [{'id': 'session-cwd', 'cwd': str(self.target), 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            second, state = self.run_task(resume=first)
            self.assertEqual(state, 'completed')
            self.assertEqual(bridge.load_run(second, self.runtime)[1]['cwd'], str(self.target))
            self.assertEqual(self.run_task(self.target, first)[1], 'completed')
            with self.assertRaisesRegex(ValueError, 'cwd_mismatch'):
                self.run_task(self.other, first)
        record = json.loads((first / 'result.json').read_text())
        record['cwd'] = str(self.other)
        bridge.write_json(first / 'result.json', record)
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            with self.assertRaisesRegex(ValueError, 'source_identity_mismatch'):
                self.run_task(resume=first)

    def test_async_persists_cwd_and_rejects_id_reuse_for_other_target(self):
        self.assertEqual(self.submit(self.target)['cwd'], str(self.target))
        self.assertEqual(self.wait_job()['status'], 'completed')
        result = jobs.result('cwd-test', self.runtime)
        self.assertEqual(json.loads(result['response'])['cwd'], str(self.target))
        self.assertTrue(self.submit(self.target)['reused'])
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit(self.other)
        request = self.runtime / 'jobs/cwd-test/request.json'
        data = json.loads(request.read_text())
        data['cwd'] = str(self.other)
        bridge.write_json(request, data)
        with self.assertRaisesRegex(ValueError, 'cwd_mismatch'):
            jobs.result('cwd-test', self.runtime)

    def test_legacy_default_request_reuses_without_rewriting(self):
        self.submit(instance_id=None)
        self.assertEqual(self.wait_job()['status'], 'completed')
        request = self.runtime / 'jobs/cwd-test/request.json'
        data = json.loads(request.read_text())
        del data['cwd']
        bridge.write_json(request, data)
        before = request.read_bytes()
        self.assertTrue(self.submit()['reused'])
        self.assertEqual(request.read_bytes(), before)
        self.assertEqual(jobs.result('cwd-test', self.runtime)['status'], 'completed')

    def test_export_uses_custom_session_directory_even_after_target_removed(self):
        dest, _ = self.run_task(self.target)
        record = json.loads((dest / 'result.json').read_text())
        final = (dest / 'response.md').read_text()
        rows = [{'type': 'session', 'id': 'session-cwd', 'cwd': str(self.target), 'seq': 0},
                {'type': 'user/message', 'seq': 1, 'data': {'source': {'kind': 'user'},
                 'content': [{'type': 'text', 'text': self.task.read_text()}]}},
                {'type': 'assistant/message', 'seq': 2, 'data': {'message': {
                 'content': [{'type': 'text', 'text': final}]}}}]
        source = self.root / 'source.zstd'
        source.write_bytes(b'test source')
        self.target.rmdir()
        with patch.object(bridge, 'session_rows', return_value=(source, rows)), redirect_stdout(io.StringIO()):
            self.assertEqual(bridge.export_run(dest, self.runtime)['status'], 'verified')
        with self.assertRaisesRegex(ValueError, 'cwd_not_directory'):
            self.run_task(resume=dest)

    def test_cli_forwards_cwd_to_both_entry_points(self):
        with patch.object(bridge, 'scope'), patch.object(bridge, 'run', return_value=(None, 'completed')) as run, \
             patch.object(sys, 'argv', ['bridge.py', 'run', '--task', str(self.task), '--cwd', str(self.target)]):
            self.assertEqual(bridge.main(), 0)
            self.assertEqual(run.call_args.kwargs['cwd'], str(self.target))
        with patch.object(bridge, 'scope'), patch.object(jobs, 'submit', return_value={}) as submit, \
             patch.object(sys, 'argv', ['jobs.py', 'submit', '--task', str(self.task), '--cwd', str(self.target)]), \
             redirect_stdout(io.StringIO()):
            self.assertEqual(jobs.main(), 0)
            self.assertEqual(submit.call_args.kwargs['cwd'], str(self.target))


if __name__ == '__main__':
    unittest.main()
