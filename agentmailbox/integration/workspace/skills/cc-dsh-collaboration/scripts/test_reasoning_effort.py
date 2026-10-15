"""Task-scoped effort selection and evidence gates, without live model requests."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import shlex
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import bridge
import jobs
import offline
from test_offline import evidence


class ReasoningEffortTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=bridge.WORKSPACE, prefix='.effort-test-')
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        for mapping in (bridge.PROFILE_PATCHES, bridge.MAX_PROFILE_PATCHES):
            for name in mapping.values():
                (self.runtime / name).write_text(name)
        self.target = self.root / 'repo'
        self.target.mkdir()
        self.task = self.root / 'task.md'
        self.task.write_text('public fixture')
        self.fake = self.root / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\nimport sys,json\n'
                             'sys.stdin.read()\n'
                             'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                             'print(json.dumps({"type":"final","text":json.dumps(sys.argv[1:])}))\n')
        self.fake.chmod(0o700)
        self.key = 'effort-' + self.root.name.lstrip('.')
        self.signals = Path('/private/tmp') / ('dsh-' + self.key)

    def tearDown(self):
        if self.signals.exists():
            self.signals.rmdir()
        self.temp.cleanup()

    def run_task(self, **kwargs):
        with redirect_stdout(io.StringIO()):
            dest, outcome = bridge.run(self.task, kwargs.pop('resume', None), 3,
                                       self.runtime, str(self.fake), **kwargs)
        self.assertEqual(outcome, 'completed')
        return dest, json.loads((dest / 'result.json').read_text())

    def submit_task(self, key='test', **kwargs):
        return jobs.submit(self.task, key, 3, self.runtime, str(self.fake), **kwargs)

    def finished_max_job(self):
        self.submit_task(reasoning_effort='max')
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            result = jobs.status('test', self.runtime)
            if result['status'] not in jobs.ACTIVE:
                os.waitpid(result['worker_pid'], 0)
                self.assertEqual(result['status'], 'completed')
                return result
            time.sleep(.03)
        self.fail('worker did not finish')

    def test_max_selects_actual_patch_without_changing_high_defaults(self):
        original = {name: (self.runtime / name).read_bytes() for name in bridge.PROFILE_PATCHES.values()}
        for profile in bridge.PROFILE_PATCHES:
            with self.subTest(profile=profile):
                dest, record = self.run_task(profile=profile, reasoning_effort='max')
                argv = json.loads((dest / 'response.md').read_text())
                self.assertEqual(argv[2], str(self.runtime / bridge.MAX_PROFILE_PATCHES[profile]))
                self.assertEqual(record['reasoning_effort'], 'max')
                dest, record = self.run_task(profile=profile)
                argv = json.loads((dest / 'response.md').read_text())
                self.assertEqual(argv[2], str(self.runtime / bridge.PROFILE_PATCHES[profile]))
                self.assertEqual(record['reasoning_effort'], 'high')
        self.assertEqual(original, {name: (self.runtime / name).read_bytes() for name in original})
        self.assertEqual(bridge.resolve_profile(None, self.runtime, 'max')[0], 'opencode-go')

    def test_unknown_effort_and_missing_max_patch_fail_before_allocation(self):
        for effort in ['', 'low', 'MAX', '../model.patch.yml']:
            with self.subTest(effort=effort), self.assertRaisesRegex(ValueError, 'unknown_reasoning_effort'):
                self.run_task(reasoning_effort=effort)
        (self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).unlink()
        with self.assertRaisesRegex(ValueError, 'profile_patch_unavailable'):
            self.run_task(reasoning_effort='max')
        self.assertFalse((self.runtime / 'instances').exists())
        self.assertFalse((self.runtime / 'runs').exists())

    def test_max_resume_inherits_and_rejects_effort_change_even_equal_hash(self):
        dest, record = self.run_task(reasoning_effort='max')
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_task(resume=dest)
        self.assertEqual(resumed['reasoning_effort'], 'max')
        (self.runtime / bridge.PATCH_NAME).write_bytes(
            (self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).read_bytes())
        with self.assertRaisesRegex(ValueError, 'reasoning_effort_mismatch'):
            self.run_task(resume=dest, reasoning_effort='high')

    def test_legacy_resume_defaults_high_without_rewriting_old_record(self):
        dest, record = self.run_task(instance_id=None)
        record.pop('reasoning_effort')
        bridge.write_json(dest / 'result.json', record)
        original = (dest / 'result.json').read_bytes()
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_task(resume=dest)
        self.assertEqual(resumed['reasoning_effort'], 'high')
        self.assertEqual((dest / 'result.json').read_bytes(), original)

    def test_old_max_hash_protection_is_not_bypassed_by_new_option(self):
        dest, record = self.run_task(instance_id=None)
        record.pop('reasoning_effort')
        record['config_sha256'] = bridge.digest(
            (self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).read_bytes())
        bridge.write_json(dest / 'result.json', record)
        with self.assertRaisesRegex(ValueError, 'config_changed'):
            self.run_task(resume=dest)
        with self.assertRaisesRegex(ValueError, 'reasoning_effort_mismatch'):
            self.run_task(resume=dest, reasoning_effort='max')

    def test_async_worker_max_receipts_reuse_and_conflict(self):
        record = self.finished_max_job()
        self.assertEqual(record['reasoning_effort'], 'max')
        request = json.loads((self.runtime / 'jobs/test/request.json').read_text())
        self.assertEqual(request['reasoning_effort'], 'max')
        actual = json.loads(jobs.result('test', self.runtime)['response'])
        self.assertEqual(actual[2], str(self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']))
        self.assertTrue(self.submit_task(reasoning_effort='max')['reused'])
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit_task()
        # Queries use recorded state, even if the current patch is unavailable.
        (self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).unlink()
        self.assertEqual(jobs.status('test', self.runtime)['status'], 'completed')
        run_path = Path(record['run_dir']) / 'result.json'
        run = json.loads(run_path.read_text())
        run['reasoning_effort'] = 'high'
        bridge.write_json(run_path, run)
        with self.assertRaisesRegex(ValueError, 'reasoning_effort_mismatch'):
            jobs.status('test', self.runtime)

    def test_legacy_job_request_and_receipt_default_high_without_rewrite(self):
        with patch.object(jobs.subprocess, 'Popen'):
            self.submit_task(instance_id=None)
        paths = [self.runtime / 'jobs/test' / name for name in ('request.json', 'receipt.json')]
        for path in paths:
            record = json.loads(path.read_text())
            record.pop('reasoning_effort')
            bridge.write_json(path, record)
        original = [path.read_bytes() for path in paths]
        self.assertTrue(self.submit_task(instance_id=None)['reused'])
        jobs.status('test', self.runtime)
        self.assertEqual([path.read_bytes() for path in paths], original)

    def test_worker_rejects_receipt_effort_mismatch_before_run(self):
        with patch.object(jobs.subprocess, 'Popen'):
            self.submit_task(reasoning_effort='max')
        path = self.runtime / 'jobs/test/receipt.json'
        record = json.loads(path.read_text())
        record['reasoning_effort'] = 'high'
        bridge.write_json(path, record)
        with self.assertRaisesRegex(ValueError, 'reasoning_effort_mismatch'):
            self.submit_task(reasoning_effort='max')
        fd = os.open(self.runtime / 'jobs/test/worker.lock', os.O_WRONLY)
        with patch.object(bridge, 'run') as run, patch.object(jobs.signal, 'signal'):
            jobs.worker('test', self.runtime, fd)
        run.assert_not_called()
        self.assertEqual(json.loads(path.read_text())['code'], 'reasoning_effort_mismatch')

    def test_missing_max_patch_rejects_async_and_offline_before_acceptance(self):
        (self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).unlink()
        with self.assertRaisesRegex(ValueError, 'profile_patch_unavailable'):
            self.submit_task(reasoning_effort='max')
        with patch.object(offline, 'preflight') as probe, \
                self.assertRaisesRegex(ValueError, 'profile_patch_unavailable'):
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime, reasoning_effort='max')
        probe.assert_not_called()
        self.assertFalse((self.runtime / 'instances').exists())
        self.assertFalse((self.runtime / 'jobs/test').exists())
        self.assertFalse((self.runtime / 'offline').exists())

    def test_worker_rechecks_max_hash_during_handoff(self):
        with patch.object(jobs.subprocess, 'Popen'):
            self.submit_task(reasoning_effort='max')
        original = bridge.run
        def change(*args, **kwargs):
            self.assertEqual(kwargs['reasoning_effort'], 'max')
            (self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).write_text('changed')
            return original(*args, **kwargs)
        fd = os.open(self.runtime / 'jobs/test/worker.lock', os.O_WRONLY)
        with patch.object(bridge, 'run', side_effect=change), patch.object(jobs.signal, 'signal'):
            jobs.worker('test', self.runtime, fd)
        self.assertEqual(jobs.status('test', self.runtime)['code'], 'config_changed')
        self.assertFalse((self.runtime / 'runs').exists())

    def test_max_probe_requires_record_and_every_actual_request_to_be_max(self):
        rows, record = evidence('/repo')
        record['reasoning_effort'] = 'max'
        rows[3]['data']['header']['config']['reasoningEffort'] = 'max'
        verified = offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce',
                                        reasoning_effort='max')
        self.assertEqual(verified['reasoning_effort'], 'max')
        for effort in ['high', 'low', None]:
            changed = deepcopy(rows)
            changed[3]['data']['header']['config']['reasoningEffort'] = effort
            with self.assertRaisesRegex(ValueError, 'probe_model_mismatch'):
                offline.verify_probe(rows + [changed[3]], record, Path('/repo'), 'exact command',
                                     'nonce', reasoning_effort='max')
        record.pop('reasoning_effort')
        with self.assertRaisesRegex(ValueError, 'reasoning_effort_mismatch'):
            offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce', reasoning_effort='max')

    def test_preflight_passes_max_and_verifies_native_effort(self):
        root = self.runtime / 'preflight'
        root.mkdir()
        rows, record = evidence(self.target)
        record['reasoning_effort'] = 'max'
        record['child_pid'] = 123
        def simulate(task, *args, **kwargs):
            self.assertEqual(kwargs['reasoning_effort'], 'max')
            self.assertEqual(kwargs['expected_config_hash'], 'accepted-hash')
            command = Path(task).read_text().splitlines()[-1]
            nonce = shlex.split(command)[-1]
            rows[4]['data']['arguments'] = json.dumps({'command': command})
            rows[5]['data']['message']['content'][0]['text'] = 'DSH_PREFLIGHT_OK::' + nonce
            return root, 'completed'
        with patch.object(bridge, 'run', side_effect=simulate), \
                patch.object(bridge, 'load_run', return_value=(root, record)), \
                patch.object(bridge, 'session_rows', return_value=(None, rows)), \
                patch.object(jobs, 'pid_probe', return_value='absent'):
            with self.assertRaisesRegex(ValueError, 'probe_model_mismatch'):
                offline.preflight(root, self.target, self.runtime, expected_config_hash='accepted-hash',
                                  reasoning_effort='max')
            rows[3]['data']['header']['config']['reasoningEffort'] = 'max'
            result = offline.preflight(root, self.target, self.runtime, expected_config_hash='accepted-hash',
                                       reasoning_effort='max')
        self.assertEqual(result['reasoning_effort'], 'max')

    def test_offline_max_handoff_and_repeat_conflict(self):
        with patch.dict(os.environ, {'DSH_PERMISSION_MODE': 'workspace-write'}), \
                patch.object(offline, 'preflight', return_value={'reasoning_effort': 'max'}) as probe, \
                patch.object(jobs, 'submit', return_value={'status': 'launch_failed'}) as submit:
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime,
                          reasoning_effort='max')
            expected = bridge.digest((self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']).read_bytes())
            self.assertEqual(probe.call_args.args[3:5], ('opencode-go', expected))
            self.assertEqual(probe.call_args.args[6], 'max')
            self.assertEqual(submit.call_args.kwargs['reasoning_effort'], 'max')
            self.assertEqual(submit.call_args.kwargs['expected_config_hash'], expected)
            request = json.loads((self.runtime / 'offline' / self.key / 'request.json').read_text())
            self.assertEqual(request['reasoning_effort'], 'max')
            with self.assertRaisesRegex(ValueError, 'offline_request_conflict'):
                offline.start(self.task, self.key, self.target, 'test', 3, self.runtime)

    def test_existing_offline_job_handoff_still_passes_max(self):
        dest = self.runtime / 'jobs' / self.key
        dest.mkdir(parents=True)
        bridge.write_json(dest / 'request.json', {'reasoning_effort': 'max'})
        with patch.object(offline, 'preflight') as probe, \
                patch.object(jobs, 'submit', return_value={'status': 'running', 'reused': True}) as submit:
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime, reasoning_effort='max')
        probe.assert_not_called()
        self.assertEqual(submit.call_args.kwargs['reasoning_effort'], 'max')

    def test_legacy_offline_request_defaults_high_without_rewrite(self):
        dest = self.runtime / 'jobs' / self.key
        dest.mkdir(parents=True)
        bridge.write_json(dest / 'request.json', {})
        with patch.object(jobs, 'submit', return_value={'status': 'running', 'reused': True}):
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime)
            path = self.runtime / 'offline' / self.key / 'request.json'
            request = json.loads(path.read_text())
            request.pop('reasoning_effort')
            bridge.write_json(path, request)
            original = path.read_bytes()
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime)
        self.assertEqual(path.read_bytes(), original)

    def test_cli_passes_explicit_max_to_all_entry_points(self):
        commands = [
            (bridge, 'run', ['run', '--task', str(self.task)], (None, 'completed')),
            (jobs, 'submit', ['submit', '--task', str(self.task)], {}),
            (offline, 'start', ['start', '--task', str(self.task), '--job', self.key,
                                '--cwd', str(self.target), '--title', 'test'], {'status': 'accepted'}),
        ]
        for module, method, arguments, result in commands:
            with self.subTest(module=module.__name__), redirect_stdout(io.StringIO()), \
                    patch.object(module, method, return_value=result) as call, \
                    patch.object(sys, 'argv', [module.__name__ + '.py'] + arguments + ['--reasoning-effort', 'max']):
                self.assertEqual(module.main(), 0)
                self.assertEqual(call.call_args.kwargs['reasoning_effort'], 'max')

    def test_doctor_max_checks_selected_patch_without_changing_default(self):
        for effort, expected in [('max', 1), ('high', 0)]:
            max_patch = self.runtime / bridge.MAX_PROFILE_PATCHES['opencode-go']
            max_patch.unlink(missing_ok=True)
            output = io.StringIO()
            with patch.object(bridge, 'RUNTIME', self.runtime), redirect_stdout(output), \
                    patch.object(bridge.shutil, 'which', return_value='/fixture/tool'), \
                    patch.object(sys, 'argv', ['bridge.py', 'doctor', '--reasoning-effort', effort]):
                self.assertEqual(bridge.main(), expected)
            self.assertEqual(json.loads(output.getvalue())['reasoning_effort'], effort)


if __name__ == '__main__':
    unittest.main()
