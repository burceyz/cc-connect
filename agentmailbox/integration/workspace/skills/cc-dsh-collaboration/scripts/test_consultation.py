"""No-tool policy propagation, immutable handoff, and native offline coverage."""
from contextlib import ExitStack, redirect_stdout
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import bridge
import instances
import jobs


FAKE = '''import json, os, sys
from pathlib import Path
task = sys.stdin.read()
args = sys.argv[1:]
patches = [Path(args[i+1]) for i, value in enumerate(args) if value == '--patch']
record = json.loads((patches[-1].parent / 'result.json').read_text()) if len(patches) == 2 else {}
sid = 'session-consultation-test'
print(json.dumps({'type': 'session', 'sessionId': sid}))
if record.get('tool_policy') == 'none':
    print(json.dumps({'type': 'tool_policy', 'policy': 'none', 'version': 1,
        'sessionId': sid, 'schema_count': 0, 'presentation': 'native', 'guard': 'deny_all',
        'plugin_sha256': record['tool_policy_plugin_sha256'],
        'overlay_sha256': record['tool_policy_overlay_sha256'],
        'config_sha256': record['config_sha256']}))
print(json.dumps({'type': 'final', 'text': json.dumps({'argv': args,
    'home': os.environ['DSH_HOME'], 'mode': os.environ.get('DSH_TOOLS_MODE'), 'task': task})}))
'''


class ConsultationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('model fixture')
        self.task = self.root / 'task.md'
        self.task.write_text('仅根据内嵌材料回答；不要读取其他文件。')
        self.fake = self.root / 'fake-dsh'
        self.fake.write_text('#!' + sys.executable + '\n' + FAKE)
        self.fake.chmod(0o700)

    def tearDown(self):
        while True:
            try:
                if os.waitpid(-1, os.WNOHANG)[0] == 0:
                    break
            except ChildProcessError:
                break
        self.temp.cleanup()

    def run_policy(self, policy='none', resume=None, **kwargs):
        with redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, resume, 3, self.runtime, str(self.fake),
                                      tool_policy=policy, **kwargs)
        return dest, status, json.loads((dest / 'result.json').read_text())

    def submit(self, key='consult', **kwargs):
        return jobs.submit(self.task, key, 3, self.runtime, str(self.fake), **kwargs)

    def finished(self, key='consult'):
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            record = jobs.status(key, self.runtime)
            if record['status'] not in jobs.ACTIVE:
                pid = record.get('worker_pid')
                if pid:
                    try:
                        os.waitpid(pid, 0)
                    except ChildProcessError:
                        pass
                return record
            time.sleep(.02)
        self.fail('offline worker did not finish')

    def policy_copies(self):
        plugin = self.root / 'policy.mjs'
        overlay = self.root / 'policy.yml'
        shutil.copyfile(bridge.POLICY_PLUGIN, plugin)
        shutil.copyfile(bridge.POLICY_OVERLAY, overlay)
        return plugin, overlay

    def test_fixed_overlay_and_exact_plugin_are_executed_and_attested(self):
        with patch.dict(os.environ, {'DSH_TOOLS_MODE': 'ptc'}):
            dest, status, record = self.run_policy()
        self.assertEqual(status, 'completed')
        bridge.validate_tool_policy_attestation(record)
        actual = json.loads((dest / 'response.md').read_text())
        self.assertEqual(actual['argv'], ['headless', '--patch', str(dest / 'model.patch.yml'),
                                          '--patch', str(dest / 'consultation-policy.patch.yml'), '--json'])
        self.assertEqual(actual['mode'], 'native')
        self.assertEqual(Path(actual['home']), instances.root(self.runtime, record['instance_id']) / 'home')
        self.assertEqual((dest / 'consultation-policy.mjs').read_bytes(), bridge.POLICY_PLUGIN.read_bytes())
        self.assertEqual((dest / 'model.patch.yml').read_bytes(), (self.runtime / bridge.PATCH_NAME).read_bytes())
        overlay = (dest / 'consultation-policy.patch.yml').read_text()
        self.assertIn(json.dumps(str(dest / 'consultation-policy.mjs')), overlay)
        self.assertNotIn('__CONSULTATION_', overlay)
        self.assertEqual(record['tool_policy_runtime_overlay_sha256'], bridge.digest(overlay.encode()))
        self.assertNotEqual(record['config_sha256'], record['model_config_sha256'])

    def test_missing_native_attestation_never_publishes_success(self):
        self.fake.write_text('#!' + sys.executable + '\nimport json,sys\nsys.stdin.read()\n'
                             'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                             'print(json.dumps({"type":"final","text":"unverified"}))\n')
        dest, status, _ = self.run_policy()
        self.assertEqual(status, 'tool_policy_unverified')
        self.assertFalse((dest / 'response.md').exists())

    def test_violation_cannot_be_hidden_by_a_later_final(self):
        self.fake.write_text('#!' + sys.executable + '\n' + FAKE +
                             '\nprint(json.dumps({"type":"tool_policy_violation"}))\n')
        dest, status, _ = self.run_policy()
        self.assertEqual(status, 'tool_policy_violation')
        self.assertFalse((dest / 'response.md').exists())

    def test_tool_violation_releases_the_slot_without_waiting_for_timeout(self):
        self.fake.write_text('#!' + sys.executable + '\nimport json,sys,time\nsys.stdin.read()\n'
                             'print(json.dumps({"type":"tool_policy_violation"}),flush=True)\n'
                             'time.sleep(20)\n')
        started = time.monotonic()
        dest, status, record = self.run_policy()
        self.assertEqual(status, 'tool_policy_violation')
        self.assertLess(time.monotonic() - started, 2)
        self.assertFalse((dest / 'response.md').exists())
        with instances.execution(self.runtime, record['instance_id'], Path(record['cwd'])):
            pass

    def test_forged_schema_or_plugin_proof_is_rejected(self):
        for old, new in [("'schema_count': 0", "'schema_count': False"),
                         ("'schema_count': 0", "'schema_count': 1"),
                         ("record['tool_policy_plugin_sha256']", "'0' * 64")]:
            with self.subTest(new=new):
                self.fake.write_text('#!' + sys.executable + '\n' + FAKE.replace(old, new))
                _, status, _ = self.run_policy()
                self.assertEqual(status, 'invalid_event')

    def test_unknown_policy_and_wrong_model_rejected_before_allocation(self):
        for policy, kwargs, code in [('/tmp/policy.yml', {}, 'unknown_tool_policy'),
                                     ('none', {'reasoning_effort': 'max'}, 'consultation_model_mismatch'),
                                     ('none', {'profile': 'happy2', 'reasoning_effort': 'max'}, 'consultation_model_mismatch'),
                                     ('none', {'profile': 'happy1', 'reasoning_effort': 'max'}, 'consultation_model_mismatch'),
                                     ('none', {'profile': 'happy2', 'model': 'glm-5.3'}, 'consultation_model_mismatch'),
                                     ('none', {'profile': 'happy1', 'model': 'glm-5.3'}, 'consultation_model_mismatch'),
                                     ('none', {'profile': 'unknown_profile'}, 'unknown_profile')]:
            with self.subTest(policy=policy, kwargs=kwargs):
                for name in ['opencode-go-max.patch.yml', 'happy2-high.patch.yml',
                             'happy1-high.patch.yml', 'happy2-max.patch.yml', 'happy1-max.patch.yml',
                             'happy2-glm-5.3-high.patch.yml', 'happy1-glm-5.3-high.patch.yml']:
                    (self.runtime / name).write_text('fixture')
                with self.assertRaisesRegex(ValueError, code):
                    self.run_policy(policy, **kwargs)
        self.assertFalse((self.runtime / 'instances').exists())

    def test_explicit_happy_profiles_with_none_policy_succeed(self):
        for profile, patch_file in [('happy2', 'happy2-high.patch.yml'),
                                    ('happy1', 'happy1-high.patch.yml')]:
            with self.subTest(profile=profile):
                (self.runtime / patch_file).write_text(f'fixture for {profile}')
                dest, status, record = self.run_policy(policy='none', profile=profile)
                self.assertEqual(status, 'completed')
                bridge.validate_tool_policy_attestation(record)
                self.assertEqual(record['profile'], profile)
                self.assertEqual(record['tool_policy'], 'none')
                self.assertEqual(record['tool_policy_version'], 1)
                self.assertEqual((dest / 'model.patch.yml').read_text(), f'fixture for {profile}')

    def test_async_worker_happy_profiles_propagate_and_attest(self):
        for profile, patch_file in [('happy2', 'happy2-high.patch.yml'),
                                    ('happy1', 'happy1-high.patch.yml')]:
            with self.subTest(profile=profile):
                key = f'consult-{profile}'
                (self.runtime / patch_file).write_text(f'fixture for {profile}')
                accepted = self.submit(key=key, tool_policy='none', profile=profile)
                self.assertEqual(accepted['tool_policy'], 'none')
                self.assertEqual(accepted['profile'], profile)
                record = self.finished(key=key)
                self.assertEqual(record['status'], 'completed')
                result = jobs.result(key, self.runtime)
                _, run = bridge.load_run(result['run_dir'], self.runtime)
                bridge.validate_tool_policy(run, accepted)
                bridge.validate_tool_policy_attestation(run)
                self.assertEqual(run['profile'], profile)

    def test_validate_consultation_model_matrix(self):
        # none allows opencode-go, happy2, happy1 with High and DeepSeek
        for profile in ('opencode-go', 'happy2', 'happy1'):
            bridge.validate_consultation_model('none', profile, 'high', 'deepseek-v4.1-flash')
            bridge.validate_consultation_model('none', profile, 'high', None)
        # none rejects Max, other models, and unknown profiles
        for profile in ('opencode-go', 'happy2', 'happy1'):
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                bridge.validate_consultation_model('none', profile, 'max', 'deepseek-v4.1-flash')
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                bridge.validate_consultation_model('none', profile, 'high', 'glm-5.3')
        for bad_profile in ('unknown', 'happy', 'claude', None):
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                bridge.validate_consultation_model('none', bad_profile, 'high', 'deepseek-v4.1-flash')

        # research allows only opencode-go with High and DeepSeek
        bridge.validate_consultation_model('research', 'opencode-go', 'high', 'deepseek-v4.1-flash')
        for bad in [('happy2', 'high', 'deepseek-v4.1-flash'),
                    ('happy1', 'high', 'deepseek-v4.1-flash'),
                    ('opencode-go', 'max', 'deepseek-v4.1-flash'),
                    ('opencode-go', 'high', 'glm-5.3')]:
            with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                bridge.validate_consultation_model('research', *bad)

    def test_missing_plugin_rejects_without_starting_a_process(self):
        with patch.object(bridge, 'POLICY_PLUGIN', self.root / 'missing.mjs'):
            with self.assertRaisesRegex(ValueError, 'tool_policy_unavailable'):
                self.run_policy()
        self.assertFalse((self.runtime / 'instances').exists())

    def test_resume_inherits_policy_but_never_changes_it(self):
        dest, _, original = self.run_policy()
        rows = [{'id': original['session_id'], 'cwd': original['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, status, resumed = self.run_policy(None, dest)
        self.assertEqual(status, 'completed')
        self.assertEqual(resumed['tool_policy'], 'none')
        self.assertEqual(resumed['instance_id'], original['instance_id'])
        self.assertEqual(resumed['config_sha256'], original['config_sha256'])
        with self.assertRaisesRegex(ValueError, 'tool_policy_mismatch'):
            self.run_policy('default', dest)
        default, _, record = self.run_policy('default')
        with self.assertRaisesRegex(ValueError, 'tool_policy_mismatch'):
            self.run_policy('none', default)
        record.pop('tool_policy')
        bridge.write_json(default / 'result.json', record)
        rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, status, resumed = self.run_policy(None, default)
        self.assertEqual(status, 'completed')
        self.assertEqual(resumed['tool_policy'], 'default')

    def test_changed_plugin_or_overlay_prevents_resume(self):
        for field in ['POLICY_PLUGIN', 'POLICY_OVERLAY']:
            with self.subTest(field=field):
                plugin, overlay = self.policy_copies()
                with patch.object(bridge, 'POLICY_PLUGIN', plugin), patch.object(bridge, 'POLICY_OVERLAY', overlay):
                    dest, _, _ = self.run_policy()
                    target = getattr(bridge, field)
                    target.write_bytes(target.read_bytes() + b'\n')
                    with self.assertRaisesRegex(ValueError, 'tool_policy_mismatch|config_changed'):
                        self.run_policy(None, dest)

    def test_async_worker_propagates_policy_and_reuse_keeps_binding(self):
        accepted = self.submit(tool_policy='none')
        self.assertEqual(accepted['tool_policy'], 'none')
        record = self.finished()
        self.assertEqual(record['status'], 'completed')
        result = jobs.result('consult', self.runtime)
        _, run = bridge.load_run(result['run_dir'], self.runtime)
        bridge.validate_tool_policy(run, accepted)
        bridge.validate_tool_policy_attestation(run)
        request_path = self.runtime / 'jobs/consult/request.json'
        before = request_path.read_bytes()
        reused = self.submit(expected_config_hash=accepted['config_sha256'])
        self.assertTrue(reused['reused'])
        self.assertEqual(request_path.read_bytes(), before)
        self.assertEqual(len(list((self.runtime / 'instances').iterdir())), 1)
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit(tool_policy='default')

    def test_acceptance_hash_is_rechecked_by_worker_and_bridge(self):
        for field, during_handoff in [('POLICY_PLUGIN', False), ('POLICY_OVERLAY', False),
                                       ('POLICY_PLUGIN', True)]:
            with self.subTest(field=field, during_handoff=during_handoff):
                key = field.lower() + str(during_handoff)
                plugin, overlay = self.policy_copies()
                with patch.object(bridge, 'POLICY_PLUGIN', plugin), patch.object(bridge, 'POLICY_OVERLAY', overlay):
                    with patch.object(jobs.subprocess, 'Popen'):
                        self.submit(key, tool_policy='none')
                    target = getattr(bridge, field)
                    original = bridge.run
                    def changed(*args, **kwargs):
                        target.write_bytes(target.read_bytes() + b'\n')
                        return original(*args, **kwargs)
                    if not during_handoff:
                        target.write_bytes(target.read_bytes() + b'\n')
                    fd = os.open(self.runtime / 'jobs' / key / 'worker.lock', os.O_WRONLY)
                    with patch.object(jobs.signal, 'signal'), \
                            patch.object(bridge, 'run', side_effect=changed if during_handoff else original):
                        jobs.worker(key, self.runtime, fd)
                    record = jobs.status(key, self.runtime)
                    self.assertEqual(record['status'], 'failed')
                    self.assertEqual(record['code'], 'config_changed')
                    self.assertIsNone(record['run_dir'])

    def test_query_rejects_receipt_downgrade_or_missing_native_proof(self):
        self.submit(tool_policy='none')
        record = self.finished()
        receipt_path = self.runtime / 'jobs/consult/receipt.json'
        original = json.loads(receipt_path.read_text())
        bridge.write_json(receipt_path, original | {'tool_policy': 'default'})
        with self.assertRaisesRegex(ValueError, 'tool_policy_mismatch'):
            jobs.status('consult', self.runtime)
        bridge.write_json(receipt_path, original)
        run_path = Path(record['run_dir']) / 'result.json'
        run = json.loads(run_path.read_text())
        run.pop('tool_policy_attestation')
        bridge.write_json(run_path, run)
        with self.assertRaisesRegex(ValueError, 'tool_policy_unverified'):
            jobs.result('consult', self.runtime)

    def test_legacy_job_reuse_does_not_rewrite_the_request(self):
        self.submit()
        self.assertEqual(self.finished()['status'], 'completed')
        for name in ['request.json', 'receipt.json']:
            path = self.runtime / 'jobs/consult' / name
            record = json.loads(path.read_text())
            record.pop('tool_policy')
            bridge.write_json(path, record)
        path = self.runtime / 'jobs/consult/request.json'
        before = path.read_bytes()
        self.assertTrue(self.submit()['reused'])
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(jobs.result('consult', self.runtime)['status'], 'completed')

    def test_consultation_respects_the_same_capacity_slots(self):
        with ExitStack() as stack:
            for _ in range(instances.CAPACITY):
                instance_id = instances.allocate(self.runtime)
                root = instances.root(self.runtime, instance_id)
                stack.enter_context(instances.execution(self.runtime, instance_id, root / 'work'))
            with self.assertRaisesRegex(ValueError, 'capacity_busy'):
                self.run_policy()
        self.assertFalse((self.runtime / 'runs').exists())

    def test_cli_propagates_tool_policy(self):
        for policy in ('none', 'research'):
            with self.subTest(policy=policy):
                with patch.object(bridge, 'run', return_value=(None, 'completed')) as call, \
                        patch.object(sys, 'argv', ['bridge.py', 'run', '--task', str(self.task), '--tool-policy', policy]):
                    self.assertEqual(bridge.main(), 0)
                self.assertEqual(call.call_args.kwargs['tool_policy'], policy)
                with patch.object(jobs, 'submit', return_value={}) as call, redirect_stdout(io.StringIO()), \
                        patch.object(sys, 'argv', ['jobs.py', 'submit', '--task', str(self.task), '--tool-policy', policy]):
                    self.assertEqual(jobs.main(), 0)
                self.assertEqual(call.call_args.kwargs['tool_policy'], policy)

    def test_research_policy_uses_separate_frozen_artifacts_and_verified_tools(self):
        self.enable_research_fake()
        dest, status, record = self.run_policy('research')
        self.assertEqual(status, 'completed')
        bridge.validate_tool_policy_attestation(record)
        self.assertEqual((dest / 'consultation-policy.mjs').read_bytes(), bridge.RESEARCH_PLUGIN.read_bytes())
        self.assertNotEqual(record['tool_policy_plugin_sha256'], bridge.digest(bridge.POLICY_PLUGIN.read_bytes()))
        proof = record['tool_policy_attestation']
        names = ['mcp__duckduckgo__search', 'web_fetch']
        self.assertEqual(proof['tools'], names)
        for bad in [[], ['web_fetch'], ['mcp__duckduckgo__search', 'shell'], names + names,
                    names + ['mcp__duckduckgo__fetch_content']]:
            with self.subTest(tools=bad):
                forged = record | {'tool_policy_attestation': proof | {'tools': bad, 'schema_count': len(bad)}}
                with self.assertRaisesRegex(ValueError, 'tool_policy_unverified'):
                    bridge.validate_tool_policy_attestation(forged)
        with self.assertRaisesRegex(ValueError, 'tool_policy_mismatch'):
            self.run_policy('none', dest)

    def enable_research_fake(self):
        names = ['mcp__duckduckgo__search', 'web_fetch']
        self.fake.write_text('#!' + sys.executable + '\n' + FAKE.replace(
            "if record.get('tool_policy') == 'none':", "if record.get('tool_policy') == 'research':").replace(
            "'policy': 'none'", "'policy': 'research'").replace(
            "'schema_count': 0", "'schema_count': 2, 'tools': " + repr(names)).replace(
            "'guard': 'deny_all'", "'guard': 'allow_research_only'"))
    def test_async_research_reuse_and_result_keep_the_accepted_policy(self):
        self.enable_research_fake()
        accepted = self.submit(tool_policy='research')
        self.assertEqual(self.finished()['status'], 'completed')
        self.assertEqual(accepted['tool_policy'], 'research')
        self.assertTrue(self.submit()['reused'])
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit(tool_policy='none')
        result = jobs.result('consult', self.runtime)
        run_path = Path(result['run_dir']) / 'result.json'
        record = json.loads(run_path.read_text())
        record['tool_policy_attestation']['tools'] = ['mcp__duckduckgo__search', 'shell']
        bridge.write_json(run_path, record)
        with self.assertRaisesRegex(ValueError, 'tool_policy_unverified'):
            jobs.result('consult', self.runtime)

    def test_research_resume_inherits_and_binds_both_policy_artifacts(self):
        self.enable_research_fake()
        plugin = self.root / 'research.mjs'
        overlay = self.root / 'research.yml'
        shutil.copyfile(bridge.RESEARCH_PLUGIN, plugin)
        shutil.copyfile(bridge.RESEARCH_OVERLAY, overlay)
        with patch.object(bridge, 'RESEARCH_PLUGIN', plugin), patch.object(bridge, 'RESEARCH_OVERLAY', overlay):
            dest, _, record = self.run_policy('research')
            rows = [{'id': record['session_id'], 'cwd': record['cwd'], 'seq': 0}]
            with patch.object(bridge, 'session_rows', return_value=(None, rows)):
                _, status, resumed = self.run_policy(None, dest)
            self.assertEqual(status, 'completed')
            self.assertEqual(resumed['tool_policy'], 'research')
            for artifact in (plugin, overlay):
                before = artifact.read_bytes()
                artifact.write_bytes(before + b'\n')
                with self.assertRaisesRegex(ValueError, 'tool_policy_mismatch|config_changed'):
                    self.run_policy(None, dest)
                artifact.write_bytes(before)
    def test_research_requires_native_proof_and_fixed_model_before_success(self):
        _, status, record = self.run_policy('research')
        self.assertEqual(status, 'tool_policy_unverified')
        self.assertNotIn('response_sha256', record)
        for kwargs in [{'profile': 'happy2'}, {'profile': 'happy1'}, {'reasoning_effort': 'max'}]:
            with self.subTest(kwargs=kwargs):
                for name in ['happy2-high.patch.yml', 'happy1-high.patch.yml', 'opencode-go-max.patch.yml']:
                    (self.runtime / name).write_text('fixture')
                with self.assertRaisesRegex(ValueError, 'consultation_model_mismatch'):
                    self.run_policy('research', **kwargs)


class NativeConsultationTests(unittest.TestCase):
    def test_native_research_tool_scope_and_arguments(self):
        try:
            executable = bridge.binary()
        except ValueError:
            self.skipTest('DSH is not installed')
        script = Path(__file__).with_name('test_research_policy.mjs')
        if not script.is_file():
            self.skipTest('test_research_policy.mjs is not present in this worktree')
        root = Path(executable).resolve().parent.parent
        node = shutil.which('node', path=bridge.execution_env(executable)['PATH'])
        self.assertIsNotNone(node)
        result = subprocess.run([node, str(script), str(root)],
                                cwd=Path(__file__).parent, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout.splitlines()[-1]),
                         {'fixture': 'native_research', 'allowed_tool_bodies': 3,
                          'rejected_tool_bodies': 0, 'network_requests': 0})

    def test_native_headless_boot_stops_before_the_model_boundary(self):
        for provider in ('opencode-go', 'happy2', 'happy'):
            with self.subTest(provider=provider):
                self.native_headless_boot('none', provider=provider)

    def test_native_research_headless_boot_stops_before_the_model_boundary(self):
        self.native_headless_boot('research')

    def native_headless_boot(self, tool_policy, provider='opencode-go'):
        try:
            executable = bridge.binary()
        except ValueError:
            self.skipTest('DSH is not installed')
        env = bridge.execution_env(executable)
        node = shutil.which('node', path=env['PATH'])
        if node is None:
            self.skipTest('Node is not installed')
        stopper = Path(__file__).with_name('test_consultation_stop.mjs').resolve()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = root / 'fixture.patch.yml'
            model.write_text(json.dumps([
                {'id': 'agent-default-model', 'config': {'provider': provider,
                    'model': 'deepseek-v4.1-flash', 'reasoningEffort': 'high'}},
                {'id': 'llm-pi-ai', 'config': {'providers': {provider: {
                    'apiKeyEnv': 'CONSULTATION_OFFLINE_UNUSED', 'models': [{
                        'id': 'deepseek-v4.1-flash', 'name': 'Offline fixture',
                        'contextWindow': 1000000, 'maxTokens': 384000, 'input': ['text'],
                        'reasoningEfforts': {'high': 'high'}}]}}}},
                {'id': 'credentials', 'disabled': True},
                {'insert': [{'id': name, 'name': '@deepseek-ai/dsh-mcp-client'} for name in
                    ['mcp-research-status', 'mcp-context7', 'mcp-deepwiki', 'mcp-tavily', 'mcp-duckduckgo']]},
            ]))
            config, artifacts = bridge.execution_config(model, tool_policy)
            launch_model, overlay = bridge.snapshot_tool_policy(root, artifacts, config)
            fixture = root / 'stop.patch.yml'
            entries = [
                {'id': 'headless-runner', 'inject': ['headlessStartup', 'consultationPolicy',
                                                    'consultationFixtureStop']},
                {'insert': [{'id': 'consultation-offline-stop', 'name': str(stopper)}]},
            ]
            if tool_policy == 'research':
                tools_script = Path(__file__).with_name('test_research_tools.mjs')
                if not tools_script.is_file():
                    self.skipTest('test_research_tools.mjs is not present in this worktree')
                entries += [{'id': name, 'disabled': True} for name in ['mcp-research-status', 'mcp-duckduckgo']]
                entries += [{'insert': [{'id': 'research-fixture-tools',
                    'name': str(tools_script.resolve())}]}]
            fixture.write_text(json.dumps(entries))
            # Block all network at Node startup, then terminate in the request
            # waterfall after policy proof and before provider preparation.
            env.update(DSH_HOME=str(root / 'home'), DSH_TELEMETRY_DISABLED='1', DSH_TOOLS_MODE='ptc')
            result = subprocess.run([node, '--import', str(stopper), str(Path(executable).resolve()),
                                      'headless', '--patch', str(launch_model), '--patch', str(overlay),
                                      '--patch', str(fixture), '--json'], input='Offline boot fixture only.',
                                     cwd=root, env=env, capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 1, result.stderr)
            events = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            proofs = [event for event in events if event.get('type') == 'tool_policy']
            self.assertEqual(len(proofs), 1, result.stderr + result.stdout)
            self.assertEqual(proofs[0]['config_sha256'], config['config_sha256'])
            self.assertEqual(proofs[0]['policy'], tool_policy)
            stops = [event for event in events if event.get('type') == 'offline_fixture_stop']
            self.assertEqual(stops, [{'type': 'offline_fixture_stop', 'provider': provider,
                'model': 'deepseek-v4.1-flash', 'reasoning_effort': 'high', 'schema_count': 2 if tool_policy == 'research' else 0,
                'network_attempts': 0}])

    def test_native_overlay_composes_and_disables_automatic_readers_and_mcp(self):
        try:
            executable = bridge.binary()
        except ValueError:
            self.skipTest('DSH is not installed')
        mcp_ids = ['mcp-research-status', 'mcp-context7', 'mcp-deepwiki', 'mcp-tavily', 'mcp-duckduckgo']
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = root / 'fixture.patch.yml'
            model.write_text('- id: agent-default-model\n  config:\n    provider: opencode-go\n'
                             '    model: deepseek-v4.1-flash\n    reasoningEffort: high\n- insert:\n' +
                             ''.join('    - id: ' + name + '\n      name: "@deepseek-ai/dsh-mcp-client"\n'
                                     for name in mcp_ids))
            config, artifacts = bridge.execution_config(model, 'none')
            _, overlay = bridge.snapshot_tool_policy(root, artifacts, config)
            env = bridge.execution_env(executable) | {'DSH_HOME': str(root / 'home'),
                                                     'DSH_TOOLS_MODE': 'ptc'}
            # Native dump writes only this disposable profile and never mounts
            # providers, evaluates lazy JS, reads credentials, or requests a model.
            result = subprocess.run([executable, 'headless', '--patch', str(model),
                                      '--patch', str(overlay), '--dump-config'],
                                     cwd=root, env=env, capture_output=True, text=True, timeout=20)
            self.assertEqual(result.returncode, 0, result.stderr)
            rows = {block.splitlines()[0].strip(): block for block in
                    re.split(r'(?m)^- id: ', result.stdout)[1:]}
            self.assertTrue(rows, result.stdout)
            for name in mcp_ids + ['agent-instructions', 'skill-filesystem', 'plan-mode',
                                   'session-title-llm', 'tool-bash', 'tool-fs', 'ptc-runtime']:
                self.assertRegex(rows[name], r'(?m)^  disabled: true$')
            self.assertRegex(rows['tools'], r'(?m)^    mode: native$')
            self.assertIn('consultationPolicy', rows['headless-runner'])
            self.assertIn(str(root / 'consultation-policy.mjs'), rows['consultation-policy'])

    def test_native_registry_factory_assembly_and_tool_rejection(self):
        try:
            executable = bridge.binary()
        except ValueError:
            self.skipTest('DSH is not installed; run the native fixture where DSH is deployed')
        root = Path(executable).resolve().parent.parent
        if not (root / 'node_modules/@deepseek-ai/dsh-tools').is_dir():
            self.skipTest('native DSH package sources are unavailable')
        node = shutil.which('node', path=bridge.execution_env(executable)['PATH'])
        self.assertIsNotNone(node)
        result = subprocess.run([node, str(Path(__file__).with_name('test_consultation_policy.mjs')), str(root)],
                                cwd=Path(__file__).parent, capture_output=True, text=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        evidence = json.loads(result.stdout.splitlines()[-1])
        self.assertEqual(evidence, {'fixture': 'native_consultation', 'schema_count': 0,
                                    'presentation': 'native', 'tool_bodies': 0, 'network_requests': 0})


if __name__ == '__main__':
    unittest.main()
