"""Friendly Happy1 profile, native happy route and immutable subscription."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

import bridge
import jobs
import offline
import test_profiles
from test_offline import evidence


class Happy1Tests(unittest.TestCase):
    setUp = test_profiles.ProfileTests.setUp
    tearDown = test_profiles.ProfileTests.tearDown
    run_profile = test_profiles.ProfileTests.run_profile

    def test_selects_own_patch_and_keeps_go_default(self):
        dest, record = self.run_profile('happy1')
        self.assertEqual(record['profile'], 'happy1')
        argv = json.loads((dest / 'response.md').read_text())
        self.assertEqual(argv[2], str(self.runtime / 'happy1-high.patch.yml'))
        self.assertEqual(bridge.resolve_profile(None, self.runtime)[0], 'opencode-go')
        (self.runtime / 'happy1-high.patch.yml').unlink()
        with self.assertRaisesRegex(ValueError, 'profile_patch_unavailable'):
            self.run_profile('happy1')

    def test_native_probe_must_use_happy_not_friendly_alias(self):
        rows, record = evidence('/repo')
        record['profile'] = 'happy1'
        config = rows[3]['data']['header']['config']
        config['provider'] = 'happy'
        result = offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce', 'happy1')
        self.assertEqual((result['profile'], result['provider']), ('happy1', 'happy'))
        for provider in ['happy1', 'happy2', 'opencode-go']:
            config['provider'] = provider
            with self.assertRaisesRegex(ValueError, 'probe_model_mismatch'):
                offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce', 'happy1')

    def test_resume_inherits_happy1_and_rejects_cross_subscription(self):
        dest, record = self.run_profile('happy1')
        (self.runtime / 'happy2-high.patch.yml').write_bytes((self.runtime / 'happy1-high.patch.yml').read_bytes())
        with self.assertRaisesRegex(ValueError, 'profile_mismatch'):
            self.run_profile('happy2', dest)
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_profile(resume=dest)
        self.assertEqual(resumed['profile'], 'happy1')

    def test_async_request_pins_profile_and_rejects_reselection(self):
        with patch.object(jobs.subprocess, 'Popen'):
            first = jobs.submit(self.task, 'happy1', 3, self.runtime, str(self.fake), profile='happy1')
        repeated = jobs.submit(self.task, 'happy1', 3, self.runtime, str(self.fake), profile='happy1')
        self.assertEqual(first['instance_id'], repeated['instance_id'])
        self.assertTrue(repeated['reused'])
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            jobs.submit(self.task, 'happy1', 3, self.runtime, str(self.fake), profile='happy2')

    def test_doctor_recognizes_happy1_without_model_call(self):
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(sys, 'argv', ['bridge.py', 'doctor', '--profile', 'happy1']), \
                patch.object(bridge.shutil, 'which', return_value='/executable'), redirect_stdout(io.StringIO()):
            self.assertEqual(bridge.main(), 0)


if __name__ == '__main__':
    unittest.main()
