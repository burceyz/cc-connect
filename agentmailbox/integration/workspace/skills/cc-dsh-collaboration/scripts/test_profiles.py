"""Subscription selection, immutable handoff and native evidence gates."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import bridge
import jobs
import offline
from test_offline import evidence


class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=bridge.WORKSPACE, prefix='.profile-test-')
        self.root = Path(self.temp.name)
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        for name in bridge.PROFILE_PATCHES.values():
            (self.runtime / name).write_text(name)
        self.task = self.root / 'task.md'
        self.task.write_text('public fixture')
        self.fake = self.root / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\nimport sys,json\n'
                             'sys.stdin.read()\n'
                             'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                             'print(json.dumps({"type":"final","text":json.dumps(sys.argv[1:])}))\n')
        self.fake.chmod(0o700)

    def tearDown(self):
        self.temp.cleanup()

    def run_profile(self, profile=None, resume=None, **kwargs):
        with redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, resume, 3, self.runtime, str(self.fake),
                                      profile=profile, **kwargs)
        self.assertEqual(status, 'completed')
        return dest, json.loads((dest / 'result.json').read_text())

    def finished_job(self):
        jobs.submit(self.task, 'test', 3, self.runtime, str(self.fake), profile='happy2')
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            result = jobs.status('test', self.runtime)
            if result['status'] not in jobs.ACTIVE:
                os.waitpid(result['worker_pid'], 0)
                self.assertEqual(result['status'], 'completed')
                return result
            time.sleep(.03)
        self.fail('worker did not finish')

    def test_both_profiles_select_actual_child_argv(self):
        for profile in [None, 'opencode-go', 'happy2']:
            dest, record = self.run_profile(profile)
            selected = profile or 'opencode-go'
            argv = json.loads((dest / 'response.md').read_text())
            self.assertEqual(argv[2], str(self.runtime / bridge.PROFILE_PATCHES[selected]))
            self.assertEqual(record['profile'], selected)

    def test_unknown_missing_and_changed_config_never_launch(self):
        for profile in ['', '../model.patch.yml', 'unknown']:
            with self.assertRaisesRegex(ValueError, 'unknown_profile'):
                self.run_profile(profile)
        (self.runtime / 'happy2-high.patch.yml').unlink()
        with self.assertRaisesRegex(ValueError, 'profile_patch_unavailable'):
            self.run_profile('happy2')
        with self.assertRaisesRegex(ValueError, 'config_changed'):
            self.run_profile(expected_config_hash='obsolete')
        self.assertFalse((self.runtime / 'runs').exists())

    def test_resume_inherits_profile_and_rejects_cross_profile_even_same_hash(self):
        dest, record = self.run_profile('happy2')
        (self.runtime / bridge.PATCH_NAME).write_bytes((self.runtime / 'happy2-high.patch.yml').read_bytes())
        with self.assertRaisesRegex(ValueError, 'profile_mismatch'):
            self.run_profile('opencode-go', dest)
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_profile(resume=dest)
        self.assertEqual(resumed['profile'], 'happy2')

    def test_legacy_resume_remains_go_and_does_not_rewrite(self):
        dest, record = self.run_profile(instance_id=None)
        record.pop('profile')
        bridge.write_json(dest / 'result.json', record)
        before = (dest / 'result.json').read_bytes()
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_profile(resume=dest)
        self.assertEqual(resumed['profile'], 'opencode-go')
        self.assertEqual((dest / 'result.json').read_bytes(), before)

    def test_worker_uses_happy_and_same_id_other_profile_conflicts(self):
        result = self.finished_job()
        actual = json.loads(jobs.result('test', self.runtime)['response'])
        self.assertEqual(actual[2], str(self.runtime / 'happy2-high.patch.yml'))
        self.assertEqual(result['profile'], 'happy2')
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            jobs.submit(self.task, 'test', 3, self.runtime, str(self.fake), profile='opencode-go')
        self.assertTrue(jobs.submit(self.task, 'test', 3, self.runtime, str(self.fake),
                                    profile='happy2')['reused'])

    def test_query_detects_profile_mismatch_without_loading_live_patch(self):
        result = self.finished_job()
        (self.runtime / 'happy2-high.patch.yml').unlink()
        self.assertEqual(jobs.result('test', self.runtime)['status'], 'completed')
        path = Path(result['run_dir']) / 'result.json'
        record = json.loads(path.read_text())
        record['profile'] = 'opencode-go'
        bridge.write_json(path, record)
        with self.assertRaisesRegex(ValueError, 'profile_mismatch'):
            jobs.status('test', self.runtime)

    def test_submit_expected_hash_blocks_publication(self):
        with self.assertRaisesRegex(ValueError, 'config_changed'):
            jobs.submit(self.task, 'test', 3, self.runtime, str(self.fake),
                        profile='happy2', expected_config_hash='old')
        self.assertFalse((self.runtime / 'jobs/test').exists())

    def test_happy_native_evidence_requires_happy_high(self):
        rows, record = evidence('/repo')
        record['profile'] = 'happy2'
        config = rows[3]['data']['header']['config']
        config['provider'] = 'happy2'
        result = offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce', 'happy2')
        self.assertEqual(result['provider'], 'happy2')
        for field, value in [('provider', 'opencode-go'), ('reasoningEffort', 'max'),
                             ('reasoningEffort', 'low')]:
            altered = deepcopy(rows)
            altered[3]['data']['header']['config'][field] = value
            with self.assertRaisesRegex(ValueError, 'probe_model_mismatch'):
                offline.verify_probe(altered, record, Path('/repo'), 'exact command', 'nonce', 'happy2')

    def test_offline_handoff_keeps_profile_and_hash(self):
        target = self.root / 'repo'
        target.mkdir()
        key = 'profile-' + self.root.name.lstrip('.')
        signals = Path('/private/tmp') / ('dsh-' + key)
        try:
            with patch.dict(os.environ, {'DSH_PERMISSION_MODE': 'workspace-write'}), \
                    patch.object(offline, 'preflight', return_value={}) as preflight, \
                    patch.object(jobs, 'submit', return_value={'status': 'launch_failed'}) as submit:
                offline.start(self.task, key, target, 'test', 3, self.runtime, 'happy2')
            expected = bridge.digest((self.runtime / 'happy2-high.patch.yml').read_bytes())
            self.assertEqual(preflight.call_args.args[3:5], ('happy2', expected))
            self.assertEqual(preflight.call_args.args[5], submit.call_args.kwargs['instance_id'])
            self.assertEqual(submit.call_args.kwargs['profile'], 'happy2')
            self.assertEqual(submit.call_args.kwargs['expected_config_hash'], expected)
        finally:
            if signals.exists():
                signals.rmdir()

    def test_cli_passes_profile_to_each_entry(self):
        with patch.object(bridge, 'run', return_value=(None, 'completed')) as call, \
                patch.object(sys, 'argv', ['bridge.py', 'run', '--task', str(self.task), '--profile', 'happy2']):
            self.assertEqual(bridge.main(), 0)
            self.assertEqual(call.call_args.kwargs['profile'], 'happy2')
        with patch.object(jobs, 'submit', return_value={}) as call, redirect_stdout(io.StringIO()), \
                patch.object(sys, 'argv', ['jobs.py', 'submit', '--task', str(self.task), '--profile', 'happy2']):
            self.assertEqual(jobs.main(), 0)
            self.assertEqual(call.call_args.kwargs['profile'], 'happy2')
        with patch.object(offline, 'start', return_value={'status': 'accepted'}) as call, redirect_stdout(io.StringIO()), \
                patch.object(sys, 'argv', ['offline.py', 'start', '--task', str(self.task), '--job', 'test',
                                          '--cwd', str(self.root), '--title', 'test', '--profile', 'happy2']):
            self.assertEqual(offline.main(), 0)
            self.assertEqual(call.call_args.kwargs['profile'], 'happy2')

    def test_worker_launch_rechecks_hash_under_bridge_lock(self):
        with patch.object(jobs.subprocess, 'Popen'):
            jobs.submit(self.task, 'race', 3, self.runtime, str(self.fake), profile='happy2')
        original = bridge.run
        def changed(*args, **kwargs):
            (self.runtime / 'happy2-high.patch.yml').write_text('changed during handoff')
            return original(*args, **kwargs)
        fd = os.open(self.runtime / 'jobs/race/worker.lock', os.O_WRONLY)
        with patch.object(bridge, 'run', side_effect=changed), patch.object(jobs.signal, 'signal'):
            jobs.worker('race', self.runtime, fd)
        self.assertEqual(jobs.status('race', self.runtime)['code'], 'config_changed')
        self.assertFalse((self.runtime / 'runs').exists())


if __name__ == '__main__':
    unittest.main()
