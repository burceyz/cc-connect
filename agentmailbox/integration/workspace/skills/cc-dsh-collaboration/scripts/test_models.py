"""Manual model selection stays bound across workers, probes and continuation."""
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import sys
import time
import unittest
from unittest.mock import patch

import bridge
import jobs
import offline
import test_reasoning_effort
from test_offline import evidence


GLM = 'glm-5.3'


class ModelTests(unittest.TestCase):
    tearDown = test_reasoning_effort.ReasoningEffortTests.tearDown
    run_task = test_reasoning_effort.ReasoningEffortTests.run_task
    submit_task = test_reasoning_effort.ReasoningEffortTests.submit_task

    def setUp(self):
        test_reasoning_effort.ReasoningEffortTests.setUp(self)
        for mapping in bridge.MODEL_PATCHES[GLM].values():
            for name in mapping.values():
                (self.runtime / name).write_text(name)

    def test_manual_model_selects_both_happy_routes_and_defaults_high(self):
        original = {name: (self.runtime / name).read_bytes() for name in bridge.PROFILE_PATCHES.values()}
        for profile in ['happy1', 'happy2']:
            for effort in [None, 'max']:
                with self.subTest(profile=profile, effort=effort):
                    dest, record = self.run_task(profile=profile, model=GLM, reasoning_effort=effort)
                    selected = effort or 'high'
                    self.assertEqual((record['model'], record['reasoning_effort']), (GLM, selected))
                    argv = json.loads((dest / 'response.md').read_text())
                    expected = bridge.MODEL_PATCHES[GLM][selected][profile]
                    self.assertEqual(argv[2], str(self.runtime / expected))
            dest, record = self.run_task(profile=profile)
            self.assertEqual((record['model'], record['reasoning_effort']), (bridge.DEFAULT_MODEL, 'high'))
            self.assertEqual(record['patch'], bridge.PROFILE_PATCHES[profile])
        _, record = self.run_task()
        self.assertEqual((record['profile'], record['model']), (bridge.DEFAULT_PROFILE, bridge.DEFAULT_MODEL))
        self.assertEqual(original, {name: (self.runtime / name).read_bytes() for name in original})

    def test_invalid_model_route_or_missing_patch_never_allocates(self):
        for model in ['', 'glm-5.3-flash', '../patch.yml', [], 'GLM-5.3']:
            with self.subTest(model=model), self.assertRaisesRegex(ValueError, 'unknown_model'):
                self.run_task(profile='happy2', model=model)
        for profile in [None, 'opencode-go']:
            with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, 'model_profile_unavailable'):
                self.run_task(profile=profile, model=GLM)
        (self.runtime / bridge.MODEL_PATCHES[GLM]['high']['happy2']).unlink()
        with self.assertRaisesRegex(ValueError, 'profile_patch_unavailable'):
            self.run_task(profile='happy2', model=GLM)
        self.assertFalse((self.runtime / 'instances').exists())
        self.assertFalse((self.runtime / 'runs').exists())

    def test_resume_inherits_model_and_refuses_change_even_with_equal_hash(self):
        dest, record = self.run_task(profile='happy2', model=GLM, reasoning_effort='max')
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_task(resume=dest)
        self.assertEqual((resumed['profile'], resumed['model'], resumed['reasoning_effort']),
                         ('happy2', GLM, 'max'))
        (self.runtime / bridge.MAX_PROFILE_PATCHES['happy2']).write_bytes(
            (self.runtime / record['patch']).read_bytes())
        with self.assertRaisesRegex(ValueError, 'model_mismatch'):
            self.run_task(resume=dest, model=bridge.DEFAULT_MODEL)
        with self.assertRaisesRegex(ValueError, 'reasoning_effort_mismatch'):
            self.run_task(resume=dest, reasoning_effort='high')

    def test_legacy_resume_keeps_deepseek_and_does_not_rewrite_history(self):
        dest, record = self.run_task(profile='happy1', instance_id=None)
        record.pop('model')
        bridge.write_json(dest / 'result.json', record)
        original = (dest / 'result.json').read_bytes()
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, resumed = self.run_task(resume=dest)
        self.assertEqual(resumed['model'], bridge.DEFAULT_MODEL)
        self.assertEqual((dest / 'result.json').read_bytes(), original)
        (self.runtime / bridge.MODEL_PATCHES[GLM]['high']['happy1']).write_bytes(
            (self.runtime / record['patch']).read_bytes())
        with self.assertRaisesRegex(ValueError, 'model_mismatch'):
            self.run_task(resume=dest, model=GLM)

    def test_glm_patch_change_still_refuses_resume(self):
        dest, record = self.run_task(profile='happy2', model=GLM)
        (self.runtime / record['patch']).write_text('changed')
        with self.assertRaisesRegex(ValueError, 'config_changed'):
            self.run_task(resume=dest)

    def finished_job(self, profile='happy1', effort='high'):
        self.submit_task(profile=profile, model=GLM, reasoning_effort=effort)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            record = jobs.status('test', self.runtime)
            if record['status'] not in jobs.ACTIVE:
                os.waitpid(record['worker_pid'], 0)
                self.assertEqual(record['status'], 'completed', record)
                return record
            time.sleep(.03)
        self.fail('worker did not finish')

    def test_real_worker_uses_accepted_glm_max_patch(self):
        record = self.finished_job(effort='max')
        request = json.loads((self.runtime / 'jobs/test/request.json').read_text())
        run_dir, run = bridge.load_run(record['run_dir'], self.runtime)
        for value in [request, record, run]:
            self.assertEqual((value['model'], value['reasoning_effort']), (GLM, 'max'))
        argv = json.loads((run_dir / 'response.md').read_text())
        self.assertEqual(argv[2], str(self.runtime / bridge.MODEL_PATCHES[GLM]['max']['happy1']))
        self.assertTrue(self.submit_task(profile='happy1', model=GLM, reasoning_effort='max')['reused'])
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit_task(profile='happy1', reasoning_effort='max')

    def test_status_refuses_mixed_model_receipt_or_run(self):
        record = self.finished_job()
        paths = [self.runtime / 'jobs/test/receipt.json', Path(record['run_dir']) / 'result.json']
        for path in paths:
            original = path.read_bytes()
            for model in [bridge.DEFAULT_MODEL, None]:
                value = json.loads(original)
                if model is None:
                    value.pop('model')
                else:
                    value['model'] = model
                bridge.write_json(path, value)
                with self.subTest(path=path.name, model=model), self.assertRaisesRegex(ValueError, 'model_mismatch'):
                    jobs.status('test', self.runtime)
            path.write_bytes(original)

    def test_worker_refuses_changed_model_before_launch(self):
        with patch.object(jobs.subprocess, 'Popen'):
            self.submit_task(profile='happy2', model=GLM)
        dest = self.runtime / 'jobs/test'
        receipt = json.loads((dest / 'receipt.json').read_text())
        receipt['model'] = bridge.DEFAULT_MODEL
        bridge.write_json(dest / 'receipt.json', receipt)
        with (dest / 'worker.lock').open('a') as lease, patch.object(jobs.bridge, 'run') as launch:
            jobs.worker('test', self.runtime, os.dup(lease.fileno()))
        launch.assert_not_called()
        self.assertEqual(json.loads((dest / 'receipt.json').read_text())['code'], 'model_mismatch')

    def test_legacy_repeat_defaults_model_without_rewriting_request(self):
        with patch.object(jobs.subprocess, 'Popen'):
            self.submit_task()
        dest = self.runtime / 'jobs/test'
        for name in ['request.json', 'receipt.json']:
            value = json.loads((dest / name).read_text())
            value.pop('model')
            bridge.write_json(dest / name, value)
        original = (dest / 'request.json').read_bytes()
        self.assertTrue(self.submit_task()['reused'])
        self.assertEqual((dest / 'request.json').read_bytes(), original)

    def test_native_probe_checks_route_model_and_effort_for_each_selection(self):
        for profile in ['happy1', 'happy2']:
            for effort in ['high', 'max']:
                rows, record = evidence('/repo')
                record.update(profile=profile, model=GLM, reasoning_effort=effort)
                config = rows[3]['data']['header']['config']
                config.update(provider=bridge.PROFILE_PROVIDERS[profile], model=GLM, reasoningEffort=effort)
                result = offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce',
                                              profile, effort, GLM)
                self.assertEqual((result['model'], result['reasoning_effort']), (GLM, effort))
                for key, value in [('model', bridge.DEFAULT_MODEL), ('provider', 'opencode-go'),
                                   ('reasoningEffort', 'low')]:
                    altered = deepcopy(rows)
                    altered[3]['data']['header']['config'][key] = value
                    with self.subTest(profile=profile, effort=effort, key=key), \
                            self.assertRaisesRegex(ValueError, 'probe_model_mismatch'):
                        offline.verify_probe(altered, record, Path('/repo'), 'exact command', 'nonce',
                                             profile, effort, GLM)
                record.pop('model')
                with self.assertRaisesRegex(ValueError, 'model_mismatch'):
                    offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce',
                                         profile, effort, GLM)

    def test_offline_pins_model_through_probe_and_formal_submission(self):
        with patch.object(offline, 'preflight', return_value={'model': GLM}) as probe, \
                patch.object(jobs, 'submit', return_value={'status': 'accepted'}) as submit, \
                patch.object(offline.subprocess, 'Popen') as spawn:
            spawn.return_value.pid = 12345
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime,
                          profile='happy2', model=GLM)
        self.assertEqual(probe.call_args.args[-2:], ('high', GLM))
        self.assertEqual(submit.call_args.kwargs['model'], GLM)
        expected = bridge.digest((self.runtime / bridge.MODEL_PATCHES[GLM]['high']['happy2']).read_bytes())
        self.assertEqual(submit.call_args.kwargs['expected_config_hash'], expected)
        request = json.loads((self.runtime / 'offline' / self.key / 'request.json').read_text())
        self.assertEqual((request['model'], request['reasoning_effort']), (GLM, 'high'))
        with self.assertRaisesRegex(ValueError, 'offline_request_conflict'):
            offline.start(self.task, self.key, self.target, 'test', 3, self.runtime, profile='happy2')

    def test_preflight_passes_model_to_real_bridge_and_evidence_gate(self):
        rows, record = evidence(self.target)
        record.update(profile='happy2', model=GLM)
        rows[3]['data']['header']['config'].update(provider='happy2', model=GLM)
        def run_probe(task, *args, **kwargs):
            self.assertEqual(kwargs['model'], GLM)
            command = Path(task).read_text().split('\n\n')[1].strip()
            rows[4]['data']['arguments'] = json.dumps({'command': command})
            rows[5]['data']['message']['content'][0]['text'] = 'DSH_PREFLIGHT_OK::' + command.split()[-1]
            return self.root / 'probe-run', 'completed'
        with patch.object(bridge, 'run', side_effect=run_probe), \
                patch.object(bridge, 'load_run', return_value=(None, record)), \
                patch.object(bridge, 'session_rows', return_value=(None, rows)), \
                patch.object(jobs, 'pid_probe', return_value='absent'):
            result = offline.preflight(self.root, self.target, self.runtime, 'happy2', model=GLM)
        self.assertEqual(result['model'], GLM)

    def test_cli_model_reaches_every_entry(self):
        commands = [(bridge, 'run', ['run', '--task', str(self.task)], (self.root, 'completed')),
                    (jobs, 'submit', ['submit', '--task', str(self.task), '--job', 'test'], {'status': 'accepted'}),
                    (offline, 'start', ['start', '--task', str(self.task), '--job', self.key,
                                       '--cwd', str(self.target), '--title', 'test'], {'status': 'accepted'})]
        for module, method, argv, result in commands:
            with self.subTest(entry=module.__name__), patch.object(bridge, 'scope'), \
                    patch.object(module, method, return_value=result) as call, \
                    patch.object(sys, 'argv', [module.__name__] + argv + ['--profile', 'happy2', '--model', GLM]), \
                    redirect_stdout(io.StringIO()):
                self.assertEqual(module.main(), 0)
            self.assertEqual(call.call_args.kwargs['model'], GLM)
            self.assertIsNone(call.call_args.kwargs['reasoning_effort'])

    def test_doctor_reports_selected_model_without_invoking_provider(self):
        for profile, expected in [('happy1', 0), ('happy2', 0), ('opencode-go', 1)]:
            output = io.StringIO()
            with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
                    patch.object(bridge, 'binary', return_value=str(self.fake)), \
                    patch.object(bridge.shutil, 'which', return_value='/executable'), \
                    patch.object(sys, 'argv', ['bridge.py', 'doctor', '--profile', profile, '--model', GLM]), \
                    redirect_stdout(output):
                self.assertEqual(bridge.main(), expected)
            self.assertEqual(json.loads(output.getvalue())['model'], GLM)

    def test_consultation_policy_remains_deepseek_only(self):
        for policy in ['none', 'research']:
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                bridge.validate_consultation_model(policy, 'opencode-go', 'high', GLM)
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                self.run_task(profile='happy2', model=GLM, tool_policy=policy)


if __name__ == '__main__':
    unittest.main()
