#!/usr/bin/env python3
"""Comprehensive test suite for subscription routing, profile gating, and behavior counter-examples."""
from contextlib import redirect_stdout, redirect_stderr
import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

import bridge
import instances
import jobs
import offline
import routing


class SubscriptionRoutingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=bridge.WORKSPACE, prefix='.routing-test-')
        self.root = Path(self.temp.name)
        self.repo = self.root / 'repo'
        self.repo.mkdir(exist_ok=True)
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        for name in bridge.PROFILE_PATCHES.values():
            (self.runtime / name).write_text('patch ' + name)
        (self.runtime / 'opencode-go-max.patch.yml').write_text('patch max')
        self.task = self.root / 'task.md'
        self.task.write_text('test task content')
        self.fake = self.root / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\nimport sys,json\n'
                             'sys.stdin.read()\n'
                             'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                             'print(json.dumps({"type":"final","text":json.dumps(sys.argv[1:])}))\n')
        self.fake.chmod(0o700)
        self.created_signals = []

    def tearDown(self):
        for p in getattr(self, 'created_signals', []):
            try:
                if p.is_dir() and not p.is_symlink():
                    for child in p.iterdir():
                        try:
                            child.unlink()
                        except OSError:
                            pass
                    p.rmdir()
                elif p.is_file() or p.is_symlink():
                    p.unlink()
            except OSError:
                pass
        self.temp.cleanup()

    def set_routing(self, config_dict):
        routing_file = self.runtime / routing.ROUTING_CONFIG
        routing_file.write_text(json.dumps(config_dict, indent=2))

    # 1. 缺配置兼容
    def test_missing_routing_config_maintains_compatibility(self):
        cfg = routing.load_routing(self.runtime)
        self.assertEqual(cfg['default_profile'], 'opencode-go')
        self.assertEqual(cfg['disabled_profiles'], [])

        # Default bridge.run uses opencode-go
        with redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, None, 3, self.runtime, str(self.fake))
        self.assertEqual(status, 'completed')
        record = json.loads((dest / 'result.json').read_text())
        self.assertEqual(record['profile'], 'opencode-go')

        # Default jobs.submit uses opencode-go
        with patch.object(jobs.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            receipt = jobs.submit(self.task, 'job-missing-cfg', 300, self.runtime, str(self.fake))
        self.assertEqual(receipt['profile'], 'opencode-go')

        # Default doctor reports opencode-go enabled
        out = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)), \
                patch.object(bridge.shutil, 'which', return_value='/executable'), \
                patch.object(sys, 'argv', ['bridge.py', 'doctor']), redirect_stdout(out):
            code = bridge.main()
        self.assertEqual(code, 0)
        doc = json.loads(out.getvalue())
        self.assertEqual(doc['profile'], 'opencode-go')
        self.assertEqual(doc['default_profile'], 'opencode-go')
        self.assertEqual(doc['disabled_profiles'], [])
        self.assertTrue(doc['checks']['profile_enabled'])

    # 2. 默认Happy2
    def test_default_profile_happy2_when_configured(self):
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})
        cfg = routing.load_routing(self.runtime)
        self.assertEqual(cfg['default_profile'], 'happy2')
        self.assertEqual(cfg['disabled_profiles'], ['opencode-go'])

        # bridge.run without profile uses happy2
        with redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, None, 3, self.runtime, str(self.fake))
        self.assertEqual(status, 'completed')
        record = json.loads((dest / 'result.json').read_text())
        self.assertEqual(record['profile'], 'happy2')

        # jobs.submit without profile uses happy2
        with patch.object(jobs.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            receipt = jobs.submit(self.task, 'job-happy2-default', 300, self.runtime, str(self.fake))
        self.assertEqual(receipt['profile'], 'happy2')

        # offline.preflight without profile uses happy2
        with patch.object(offline.jobs, 'pid_probe', return_value='absent'), \
                patch.object(bridge, 'session_rows', return_value=(Path('/session'), [{'id': 'session-test', 'cwd': str(self.repo)}])), \
                patch.object(offline, 'verify_probe', return_value={'mode': 'workspace-write', 'profile': 'happy2'}), \
                patch.object(bridge, 'binary', return_value=str(self.fake)), \
                redirect_stdout(io.StringIO()):
            pref = offline.preflight(self.root, self.repo, self.runtime)
        self.assertEqual(pref['profile'], 'happy2')

        # offline.start without profile uses happy2
        key = 'offline-happy2-' + uuid.uuid4().hex
        self.created_signals.append(Path('/private/tmp') / ('dsh-' + key))
        with patch.object(offline, 'preflight', return_value={'mode': 'workspace-write'}), \
                patch.object(offline.jobs.subprocess, 'Popen') as popen, \
                redirect_stdout(io.StringIO()):
            popen.return_value.pid = 99999
            start_rec = offline.start(self.task, key, self.repo, 'Test', 60, self.runtime)
        self.assertEqual(start_rec['profile'], 'happy2')

        # doctor without profile reports happy2 as default and enabled
        out = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)), \
                patch.object(bridge.shutil, 'which', return_value='/executable'), \
                patch.object(sys, 'argv', ['bridge.py', 'doctor']), redirect_stdout(out):
            code = bridge.main()
        self.assertEqual(code, 0)
        doc = json.loads(out.getvalue())
        self.assertEqual(doc['profile'], 'happy2')
        self.assertEqual(doc['default_profile'], 'happy2')
        self.assertEqual(doc['disabled_profiles'], ['opencode-go'])
        self.assertTrue(doc['checks']['profile_enabled'])

    # 3. 显式Happy1
    def test_explicit_profile_happy1_allowed(self):
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # bridge.run with profile='happy1'
        with redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, None, 3, self.runtime, str(self.fake), profile='happy1')
        self.assertEqual(status, 'completed')
        record = json.loads((dest / 'result.json').read_text())
        self.assertEqual(record['profile'], 'happy1')

        # jobs.submit with profile='happy1'
        with patch.object(jobs.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            receipt = jobs.submit(self.task, 'job-happy1-explicit', 300, self.runtime, str(self.fake), profile='happy1')
        self.assertEqual(receipt['profile'], 'happy1')

        # offline.start with profile='happy1'
        key = 'offline-happy1-' + uuid.uuid4().hex
        self.created_signals.append(Path('/private/tmp') / ('dsh-' + key))
        with patch.object(offline, 'preflight', return_value={'mode': 'workspace-write'}), \
                patch.object(offline.jobs.subprocess, 'Popen') as popen, \
                redirect_stdout(io.StringIO()):
            popen.return_value.pid = 99998
            start_rec = offline.start(self.task, key, self.repo, 'Test', 60, self.runtime, profile='happy1')
        self.assertEqual(start_rec['profile'], 'happy1')

        # doctor with --profile happy1 reports happy1 enabled
        out = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)), \
                patch.object(bridge.shutil, 'which', return_value='/executable'), \
                patch.object(sys, 'argv', ['bridge.py', 'doctor', '--profile', 'happy1']), redirect_stdout(out):
            code = bridge.main()
        self.assertEqual(code, 0)
        doc = json.loads(out.getvalue())
        self.assertEqual(doc['profile'], 'happy1')
        self.assertTrue(doc['checks']['profile_enabled'])

    # 4. 禁Go新run/submit/preflight
    def test_disabled_go_rejects_new_run_submit_preflight_and_start(self):
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # bridge.run with explicit Go rejected before instance allocation
        initial_instances = list((self.runtime / 'instances').glob('*')) if (self.runtime / 'instances').exists() else []
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            bridge.run(self.task, None, 3, self.runtime, str(self.fake), profile='opencode-go')
        after_instances = list((self.runtime / 'instances').glob('*')) if (self.runtime / 'instances').exists() else []
        self.assertEqual(initial_instances, after_instances)

        # jobs.submit with explicit Go rejected before instance allocation
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            jobs.submit(self.task, 'job-go-rejected', 300, self.runtime, str(self.fake), profile='opencode-go')
        self.assertFalse((self.runtime / 'jobs' / 'job-go-rejected').exists())

        # offline.preflight with Go rejected
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            offline.preflight(self.root, self.repo, self.runtime, profile='opencode-go')

        # offline.start with Go rejected before signals or lock creation
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            offline.start(self.task, 'offline-go-rejected', self.repo, 'Test', 60, self.runtime, profile='opencode-go')
        self.assertFalse((Path('/private/tmp') / 'dsh-offline-go-rejected').exists())

        # CLI entry point checks
        err = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(sys, 'argv', ['bridge.py', 'run', '--task', str(self.task), '--profile', 'opencode-go']), \
                redirect_stderr(err):
            self.assertEqual(bridge.main(), 1)
        self.assertIn('"profile_disabled"', err.getvalue())

        err = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(sys, 'argv', ['jobs.py', 'submit', '--task', str(self.task), '--profile', 'opencode-go']), \
                redirect_stderr(err):
            self.assertEqual(jobs.main(), 1)
        self.assertIn('"profile_disabled"', err.getvalue())

        err = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(sys, 'argv', ['offline.py', 'start', '--task', str(self.task), '--job', 'test-cli',
                                          '--cwd', str(self.repo), '--title', 'Test', '--profile', 'opencode-go']), \
                redirect_stderr(err):
            self.assertEqual(offline.main(), 1)
        self.assertIn('"profile_disabled"', err.getvalue())

    # 5. 旧Go记录可查询/重投而不重启动、resume拒绝
    def test_legacy_go_records_queryable_and_idempotent_resubmit_allowed_resume_refused(self):
        # 1. Create a legacy Go job without routing.json
        job_id = 'legacy-go-job'
        self.created_signals.append(Path('/private/tmp') / ('dsh-' + job_id))
        with patch.object(jobs.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            receipt = jobs.submit(self.task, job_id, 60, self.runtime, executable=str(self.fake),
                                  title='Test', cwd=str(self.repo), profile='opencode-go')
        self.assertEqual(receipt['profile'], 'opencode-go')

        # 2. Complete a run for that job
        dest, status = bridge.run(self.task, None, 3, self.runtime, str(self.fake), profile='opencode-go',
                                  cwd=str(self.repo), instance_id=receipt.get('instance_id'))
        self.assertEqual(status, 'completed')
        # Update job receipt to completed with run_dir
        job_dir = self.runtime / 'jobs' / job_id
        receipt.update(status='completed', run_dir=str(dest))
        bridge.write_json(job_dir / 'receipt.json', receipt)

        # 3. Now activate routing that disables Go and defaults to happy2
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # jobs.status query succeeds and returns opencode-go
        st = jobs.status(job_id, self.runtime)
        self.assertEqual(st['profile'], 'opencode-go')
        self.assertEqual(st['status'], 'completed')

        # jobs.result query succeeds
        res = jobs.result(job_id, self.runtime)
        self.assertEqual(res['profile'], 'opencode-go')

        # Unparameterized idempotent resubmit of the old Go job succeeds and returns reused receipt
        with patch.object(jobs.subprocess, 'Popen') as popen:
            resubmitted = jobs.submit(self.task, job_id, 60, self.runtime, executable=str(self.fake),
                                      title='Test', cwd=str(self.repo))
            self.assertTrue(resubmitted.get('reused'))
            self.assertEqual(resubmitted['profile'], 'opencode-go')
            popen.assert_not_called()

        # Unparameterized offline.start on existing Go job succeeds without running model
        with patch.object(offline, 'preflight') as preflight, patch.object(jobs.subprocess, 'Popen'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)):
            off_res = offline.start(self.task, job_id, self.repo, 'Test', 60, self.runtime)
            self.assertTrue(off_res.get('reused'))
            self.assertEqual(off_res['profile'], 'opencode-go')
            preflight.assert_not_called()

        # Resuming the legacy Go run without profile is REFUSED (profile_disabled)
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            bridge.run(self.task, str(dest), 3, self.runtime, str(self.fake))

        # Resuming the legacy Go run with explicit happy2 is REFUSED (profile_mismatch)
        with self.assertRaisesRegex(ValueError, 'profile_mismatch'):
            bridge.run(self.task, str(dest), 3, self.runtime, str(self.fake), profile='happy2')

    # 6. Happy旧job无参重投固定原订阅
    def test_happy_legacy_jobs_unparameterized_resubmit_preserves_subscription(self):
        # Create a job with happy1 and a job with happy2
        with patch.object(jobs.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            r1 = jobs.submit(self.task, 'job-h1', 300, self.runtime, str(self.fake), profile='happy1')
            r2 = jobs.submit(self.task, 'job-h2', 300, self.runtime, str(self.fake), profile='happy2')
        self.assertEqual(r1['profile'], 'happy1')
        self.assertEqual(r2['profile'], 'happy2')

        # Enable routing with default_profile=happy2, disabled_profiles=[opencode-go]
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # Resubmit job-h1 without --profile: must preserve happy1!
        with patch.object(jobs.subprocess, 'Popen'):
            reused1 = jobs.submit(self.task, 'job-h1', 300, self.runtime, str(self.fake), profile=None)
        self.assertTrue(reused1.get('reused'))
        self.assertEqual(reused1['profile'], 'happy1')

        # Resubmit job-h2 without --profile: preserves happy2!
        with patch.object(jobs.subprocess, 'Popen'):
            reused2 = jobs.submit(self.task, 'job-h2', 300, self.runtime, str(self.fake), profile=None)
        self.assertTrue(reused2.get('reused'))
        self.assertEqual(reused2['profile'], 'happy2')

    # 7. 非法策略fail closed
    def test_invalid_routing_config_fails_closed(self):
        invalid_payloads = [
            'not valid json',
            '{}',
            json.dumps(['not a dict']),
            json.dumps({'default_profile': None}),
            json.dumps({'disabled_profiles': ['opencode-go']}),  # missing default_profile key
            json.dumps({'default_profile': 'happy2', 'disabled_profiles': None}),  # explicit null disabled_profiles
            json.dumps({'default_profile': 'unknown_profile'}),
            json.dumps({'default_profile': 'happy2', 'disabled_profiles': ['happy2']}),  # default is disabled
            json.dumps({'default_profile': 'happy2', 'disabled_profiles': 'opencode-go'}),  # not a list
            json.dumps({'default_profile': 'happy2', 'disabled_profiles': ['unknown_profile']}),
            json.dumps({'default_profile': 'happy2', 'disabled_profiles': ['opencode-go'], 'extra_key': True}),
            json.dumps({'default_profile': 123}),
        ]
        routing_file = self.runtime / routing.ROUTING_CONFIG
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                routing_file.write_text(payload)
                with self.assertRaisesRegex(ValueError, 'invalid_routing_config'):
                    routing.load_routing(self.runtime)
                with self.assertRaisesRegex(ValueError, 'invalid_routing_config'):
                    bridge.run(self.task, None, 3, self.runtime, str(self.fake))
                with self.assertRaisesRegex(ValueError, 'invalid_routing_config'):
                    jobs.submit(self.task, 'fail-closed-job', 300, self.runtime, str(self.fake))
                with self.assertRaisesRegex(ValueError, 'invalid_routing_config'):
                    offline.preflight(self.root, self.repo, self.runtime)

        # Test broken symlink fails closed instead of falling back to Go compatibility
        routing_file.unlink()
        broken_target = self.root / 'nonexistent_target.json'
        routing_file.symlink_to(broken_target)
        self.assertTrue(routing_file.is_symlink())
        self.assertFalse(routing_file.exists())
        with self.assertRaisesRegex(ValueError, 'invalid_routing_config'):
            routing.load_routing(self.runtime)

        # Test valid symlink also fails closed
        routing_file.unlink()
        target_file = self.root / 'other_routing.json'
        target_file.write_text(json.dumps({'default_profile': 'happy2'}))
        routing_file.symlink_to(target_file)
        self.assertTrue(routing_file.is_symlink())
        self.assertTrue(routing_file.exists())
        with self.assertRaisesRegex(ValueError, 'invalid_routing_config'):
            routing.load_routing(self.runtime)

        # Test missing disabled_profiles key is accepted and defaults to empty list
        routing_file.unlink()
        routing_file.write_text(json.dumps({'default_profile': 'happy2'}))
        cfg = routing.load_routing(self.runtime)
        self.assertEqual(cfg['default_profile'], 'happy2')
        self.assertEqual(cfg['disabled_profiles'], [])

        # Test truly non-existent file maintains Go compatibility
        routing_file.unlink()
        self.assertFalse(routing_file.is_symlink())
        self.assertFalse(routing_file.exists())
        cfg = routing.load_routing(self.runtime)
        self.assertEqual(cfg['default_profile'], 'opencode-go')
        self.assertEqual(cfg['disabled_profiles'], [])

    # 8. 配置在受理/执行之间暂停能阻止worker
    def test_worker_blocked_when_profile_disabled_between_submission_and_execution(self):
        # 1. Job is submitted when happy2 is enabled
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})
        with patch.object(jobs.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            receipt = jobs.submit(self.task, 'job-worker-pause', 300, self.runtime, str(self.fake))
        self.assertEqual(receipt['status'], 'accepted')
        self.assertEqual(receipt['profile'], 'happy2')

        # 2. Before worker runs, happy2 is disabled in routing.json
        self.set_routing({"default_profile": "happy1", "disabled_profiles": ["happy2", "opencode-go"]})

        # 3. Simulate worker execution under lease lock
        dest = self.runtime / 'jobs' / 'job-worker-pause'
        with (dest / 'worker.lock').open('a') as lease:
            jobs.worker('job-worker-pause', self.runtime, os.dup(lease.fileno()))

        # 4. Check worker receipt: must be failed with profile_disabled
        final_receipt = json.loads((dest / 'receipt.json').read_text())
        self.assertEqual(final_receipt['status'], 'failed')
        self.assertEqual(final_receipt['code'], 'profile_disabled')
        # Model run was never created
        self.assertIsNone(final_receipt.get('run_dir'))

    # 9. doctor只读显示当前新任务默认和暂停项、有效配置，显式检查历史Go配置可展示其已停用
    def test_doctor_reports_defaults_and_disabled_profiles_and_flags_disabled_go(self):
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # Default doctor check
        out = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)), \
                patch.object(bridge.shutil, 'which', return_value='/executable'), \
                patch.object(sys, 'argv', ['bridge.py', 'doctor']), redirect_stdout(out):
            code = bridge.main()
        self.assertEqual(code, 0)
        doc = json.loads(out.getvalue())
        self.assertEqual(doc['profile'], 'happy2')
        self.assertEqual(doc['default_profile'], 'happy2')
        self.assertEqual(doc['disabled_profiles'], ['opencode-go'])
        self.assertTrue(doc['checks']['profile_enabled'])

        # Explicit Go doctor check
        out = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)), \
                patch.object(bridge.shutil, 'which', return_value='/executable'), \
                patch.object(sys, 'argv', ['bridge.py', 'doctor', '--profile', 'opencode-go']), redirect_stdout(out):
            code = bridge.main()
        self.assertEqual(code, 1)  # Fails because profile is disabled!
        doc = json.loads(out.getvalue())
        self.assertEqual(doc['profile'], 'opencode-go')
        self.assertEqual(doc['default_profile'], 'happy2')
        self.assertEqual(doc['disabled_profiles'], ['opencode-go'])
        self.assertTrue(doc['checks']['patch'])  # Patch exists
        self.assertFalse(doc['checks']['profile_enabled'])  # But profile is disabled!

    # 10. 原consultation none/research专用入口明确暂停，不能悄悄改成Happy
    def test_consultation_policy_pauses_explicitly_when_go_is_disabled(self):
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # Calling consultation none without profile explicitly attempts Go and raises profile_disabled
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            bridge.run(self.task, None, 3, self.runtime, str(self.fake), tool_policy='none')

        # Calling consultation research without profile explicitly attempts Go and raises profile_disabled
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            bridge.run(self.task, None, 3, self.runtime, str(self.fake), tool_policy='research')

        # Explicit Happy2/Happy1 with tool_policy='none' is allowed to proceed,
        # but because this fake binary emits no attestation, the result must be
        # tool_policy_unverified (not completed), while binding the requested profile.
        for profile in ('happy2', 'happy1'):
            dest, status = bridge.run(self.task, None, 3, self.runtime, str(self.fake),
                                      tool_policy='none', profile=profile)
            self.assertEqual(status, 'tool_policy_unverified')
            record = json.loads((dest / 'result.json').read_text())
            self.assertEqual(record['profile'], profile)
            self.assertEqual(record['tool_policy'], 'none')

        # Explicit Happy2/Happy1 with tool_policy='research' still strictly requires Go and raises consultation_model_mismatch
        for profile in ('happy2', 'happy1'):
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                bridge.run(self.task, None, 3, self.runtime, str(self.fake), tool_policy='research', profile=profile)

    # 11. 历史缺profile记录解释为opencode-go：查询/重投保持原订阅，Go停用时启动拒绝
    def test_legacy_record_missing_profile_interpreted_as_go_and_gated(self):
        # 1. Complete a run using fake binary
        dest, status = bridge.run(self.task, None, 3, self.runtime, str(self.fake), cwd=str(self.repo))
        self.assertEqual(status, 'completed')
        run_res = json.loads((dest / 'result.json').read_text())
        run_res.pop('profile', None)
        bridge.write_json(dest / 'result.json', run_res)

        # 2. Create a legacy completed job where request.json and receipt.json lack 'profile'
        job_id = 'job-no-profile-legacy'
        self.created_signals.append(Path('/private/tmp') / ('dsh-' + job_id))
        job_dir = self.runtime / 'jobs' / job_id
        job_dir.mkdir(parents=True)
        req_data = {
            'task_sha256': run_res['task_sha256'],
            'timeout': 60,
            'cwd': str(self.repo.resolve()),
            'title': 'Legacy Job No Profile',
            'executable': str(self.fake.resolve()),
            'config_sha256': run_res['config_sha256'],
            'reasoning_effort': run_res.get('reasoning_effort', 'high'),
            'model': run_res.get('model', 'v4.1-high'),
            'tool_policy': 'default',
            'instance_id': run_res.get('instance_id'),
            'mailbox': {'status': 'disabled', 'reason': 'legacy_request'},
        }
        rec_data = {
            'job_id': job_id,
            'status': 'completed',
            'submitted': bridge.now(),
            'job_dir': str(job_dir),
            'run_dir': str(dest),
            'cwd': str(self.repo.resolve()),
            'reasoning_effort': run_res.get('reasoning_effort', 'high'),
            'model': run_res.get('model', 'v4.1-high'),
            'tool_policy': 'default',
            'instance_id': run_res.get('instance_id'),
            'mailbox': {'status': 'disabled', 'reason': 'legacy_request'},
        }
        bridge.write_json(job_dir / 'request.json', req_data)
        bridge.write_json(job_dir / 'receipt.json', rec_data)
        original_req_bytes = (job_dir / 'request.json').read_bytes()
        original_rec_bytes = (job_dir / 'receipt.json').read_bytes()

        # 3. Activate routing that disables Go and defaults to happy2
        self.set_routing({"default_profile": "happy2", "disabled_profiles": ["opencode-go"]})

        # jobs.status query succeeds and returns opencode-go
        st = jobs.status(job_id, self.runtime)
        self.assertEqual(st['profile'], 'opencode-go')
        self.assertEqual(st['status'], 'completed')

        # jobs.result query succeeds and returns opencode-go
        res = jobs.result(job_id, self.runtime)
        self.assertEqual(res['profile'], 'opencode-go')

        # Unparameterized idempotent resubmit via jobs.submit: returns reused receipt with opencode-go
        with patch.object(jobs.subprocess, 'Popen') as popen:
            resubmitted = jobs.submit(self.task, job_id, 60, self.runtime, executable=str(self.fake),
                                      title='Legacy Job No Profile', cwd=str(self.repo))
            self.assertTrue(resubmitted.get('reused'))
            self.assertEqual(resubmitted['profile'], 'opencode-go')
            popen.assert_not_called()

        # Unparameterized idempotent resubmit via offline.start: returns reused receipt without running model
        with patch.object(offline, 'preflight') as preflight, patch.object(jobs.subprocess, 'Popen'), \
                patch.object(bridge, 'binary', return_value=str(self.fake)):
            off_res = offline.start(self.task, job_id, self.repo, 'Legacy Job No Profile', 60, self.runtime)
            self.assertTrue(off_res.get('reused'))
            self.assertEqual(off_res['profile'], 'opencode-go')
            preflight.assert_not_called()

        # Verify historical files on disk were not rewritten
        self.assertEqual((job_dir / 'request.json').read_bytes(), original_req_bytes)
        self.assertEqual((job_dir / 'receipt.json').read_bytes(), original_rec_bytes)

        # 4. An existing offline request missing profile without accepted job
        # requires launching model; with Go disabled, must fail with profile_disabled
        unaccepted_key = 'unaccepted-offline-no-profile'
        self.created_signals.append(Path('/private/tmp') / ('dsh-' + unaccepted_key))
        off_dir = self.runtime / 'offline' / unaccepted_key
        off_dir.mkdir(parents=True)
        bridge.write_json(off_dir / 'request.json', {
            'task_sha256': run_res['task_sha256'],
            'cwd': str(self.repo.resolve()),
            'title': 'Unaccepted Legacy',
            'timeout': 60,
            'config_sha256': run_res['config_sha256'],
            'signals': str(Path('/private/tmp') / ('dsh-' + unaccepted_key)),
            'reasoning_effort': 'high',
            'model': 'v4.1-high',
        })
        with self.assertRaisesRegex(ValueError, 'profile_disabled'):
            offline.start(self.task, unaccepted_key, self.repo, 'Unaccepted Legacy', 60, self.runtime)


if __name__ == '__main__':
    unittest.main()
