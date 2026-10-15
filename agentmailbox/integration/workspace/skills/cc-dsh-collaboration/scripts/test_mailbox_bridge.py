"""Mailbox acceptance wiring: fixed ownership, evidence files, and resume rules.

Most tests use a contract-shaped host fixture; the real-service class runs the
installed Go service with the current real host implementation read-only.
"""
import hashlib
import inspect
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import bridge
import jobs
import offline

os.environ.setdefault('AGENT_MAILBOX_DISABLE', '1')

MODULE_ROOT = Path(__file__).resolve().parents[5]
REAL_BINARY = Path(os.environ.get('AGENT_MAILBOX_BIN', str(MODULE_ROOT / 'build/agent-mailbox')))
REAL_HOST_DIR = Path(os.environ.get('AGENT_MAILBOX_HOST_DIR', str(MODULE_ROOT / 'skill/scripts')))


def load_real_host():
    """Import the current shared host implementation read-only, by path.

    The bridge may already have imported a different copy under the same module
    name, so load this file explicitly instead of trusting ``sys.modules``.
    """
    path = REAL_HOST_DIR / 'host_runtime.py'
    if not path.is_file():
        return None
    if str(REAL_HOST_DIR) not in sys.path:
        sys.path.insert(0, str(REAL_HOST_DIR))
    import importlib.util
    spec = importlib.util.spec_from_file_location('real_host_runtime', path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception:
        return None
    return module


class FakeHostRuntime:
    """Contract-shaped host runtime: same file names and conflict rules as the real one."""

    def __init__(self, service=True, parent_binding=None):
        self.service = service
        self.parent_binding = parent_binding
        self.registered = []
        self.deactivated = []

    def resolve_mailbox_service(self, runtime_dir=None):
        if not self.service:
            return None, None
        return Path('/nonexistent/api.sock'), Path('/nonexistent/admin-token')

    def check_mailbox_service(self, socket_path, timeout=3):
        return self.service

    def binding_for_current(self):
        return str(self.parent_binding) if self.parent_binding else None

    def request_unix(self, socket_path, method, path, token=None, payload=None, timeout=12):
        if path == '/v1/me':
            return {'mailbox': {'mailbox_id': 'mbx_parent', 'scope': 'cc:parent-scope', 'coordinator': True}}
        raise AssertionError(path)

    def register_task_mailbox(self, service_socket, admin_token, registration_key, scope, team='',
                              label='', kind='hermes', coordinator=False, parent_id='', metadata=None,
                              task_dir=None):
        task_dir = Path(task_dir)
        task_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        safe = hashlib.sha256(registration_key.encode('utf-8')).hexdigest()[:16]
        reg_file = task_dir / f'reg-{safe}.json'
        request = {'registration_key': registration_key, 'label': label or kind, 'kind': kind,
                   'coordinator': bool(coordinator), 'scope': scope, 'team': team or '',
                   'parent_id': parent_id or '', 'metadata': metadata or {}}
        if reg_file.exists():
            existing = json.loads(reg_file.read_text(encoding='utf-8'))
            for key, value in request.items():
                if existing.get(key) != value:
                    raise ValueError('registration_conflict')
            token = existing['token']
        else:
            token = 'tok-' + safe
            reg_file.write_text(json.dumps(request | {'token': token}), encoding='utf-8')
        self.registered.append(request | {'token': token, 'task_dir': str(task_dir), 'reg_file': str(reg_file)})
        mailbox_id = 'mbx_' + safe
        return {'mailbox_id': mailbox_id, 'scope': scope, 'team': team or '', 'parent_id': parent_id or ''}, token

    def create_binding_file(self, binding_path, service_socket, token, mailbox_id, generation):
        Path(binding_path).write_text(json.dumps({
            'schema': 1, 'socket': str(service_socket), 'token': token,
            'mailbox_id': mailbox_id, 'generation': generation,
        }), encoding='utf-8')
        return Path(binding_path)

    def deactivate_mailbox(self, service_socket, token, generation):
        self.deactivated.append(generation)


def write_parent_binding(directory):
    target = Path(directory) / 'parent-binding.json'
    target.write_text(json.dumps({'schema': 1, 'socket': '/nonexistent/parent.sock', 'token': 'parent-token',
                                  'mailbox_id': 'mbx_parent', 'generation': 'parent-gen'}), encoding='utf-8')
    return target


class LayoutTests(unittest.TestCase):
    """Item 1: the shared host and plugin are found in the production install layout."""

    def test_production_layout_is_found(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            scripts = root / 'skills/cc-dsh-collaboration/scripts'
            scripts.mkdir(parents=True)
            host_dir = root / 'skills/cc-agent-ipc/scripts'
            host_dir.mkdir(parents=True)
            (host_dir / 'host_runtime.py').write_text('# host\n', encoding='utf-8')
            (host_dir / 'dsh-plugin.mjs').write_text('// plugin\n', encoding='utf-8')
            bridge_path = scripts / 'bridge.py'
            candidates = [path.resolve() for path in bridge.host_runtime_candidates(bridge_path)]
            self.assertIn((host_dir / 'host_runtime.py').resolve(), candidates)
            # The production shared host must be the first existing candidate, not a
            # same-directory file that could shadow it.
            existing = [path for path in candidates if path.is_file()]
            self.assertEqual(existing[0], (host_dir / 'host_runtime.py').resolve())
            self.assertEqual(bridge.resolve_dsh_plugin_path(bridge_path), (host_dir / 'dsh-plugin.mjs').resolve())

    def test_development_layout_is_found(self):
        candidates = [path.resolve() for path in bridge.host_runtime_candidates(bridge.__file__)]
        self.assertIn((Path(bridge.__file__).resolve().parents[5] / 'skill/scripts/host_runtime.py').resolve(), candidates)
        plugin = bridge.resolve_dsh_plugin_path()
        self.assertIsNotNone(plugin)
        self.assertEqual(plugin.name, 'dsh-plugin.mjs')

    def test_explicit_team_keeps_failing_without_the_host_module(self):
        with patch.object(bridge, 'host_runtime', None), \
                patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': ''}):
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_service_unavailable'):
                bridge.resolve_mailbox_plan(Path('/tmp/rt'), 'team-a', 'default', 'dsh')


class MailboxBridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test config')
        self.task = self.root / 'task.md'
        self.task.write_text('讨论 $() `echo nope` 中文')
        self.host = FakeHostRuntime(parent_binding=write_parent_binding(self.root))
        self.env = patch.dict(os.environ, {
            'AGENT_MAILBOX_DISABLE': '', 'AGENT_MAILBOX_BINDING': str(self.host.parent_binding),
        }, clear=False)
        self.env.start()
        self.patches = [
            patch.object(bridge, 'host_runtime', self.host),
            patch.object(bridge, 'resolve_dsh_modules', return_value=('/fake/dsh-tools.mjs', '/fake/dsh-llm.mjs')),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.env.stop()
        self.temp.cleanup()

    def child(self, code):
        target = self.root / 'fake-dsh'
        target.write_text('#!' + sys.executable + '\n' + code)
        target.chmod(0o700)
        return str(target)

    def session_child(self):
        return self.child('import sys,json,os\nsys.stdin.read()\n'
                          'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                          'print(json.dumps({"type":"final","text":json.dumps({'
                          '"argv":sys.argv[1:],"binding":os.environ.get("AGENT_MAILBOX_BINDING","")})}))\n')

    def run_child(self, executable, **kwargs):
        with redirect_stdout(io.StringIO()):
            return bridge.run(self.task, None, 3, self.runtime, executable, **kwargs)

    def resume_child(self, executable, dest, cwd):
        rows = [{'type': 'session', 'id': 'session-test', 'cwd': cwd}]
        with patch.object(bridge, 'session_rows', return_value=(Path('/unused'), rows)):
            with redirect_stdout(io.StringIO()):
                return bridge.run(self.task, dest, 3, self.runtime, executable)

    def test_auto_registers_with_confirmed_identity_and_writes_evidence(self):
        dest, status = self.run_child(self.session_child())
        self.assertEqual(status, 'completed')
        record = json.loads((dest / 'result.json').read_text())
        self.assertEqual(record['mailbox_status'], 'registered')
        self.assertEqual(record['mailbox_scope'], 'cc:parent-scope')
        self.assertEqual(record['parent_id'], 'mbx_parent')
        self.assertEqual(record['registration_key'], 'dsh-run:' + dest.name)
        self.assertEqual(record['mailbox_service_socket'], '/nonexistent/api.sock')
        self.assertEqual(record['mailbox_parent_binding_path'], str(Path(self.host.parent_binding).resolve()))
        self.assertEqual(record['mailbox_task_dir'], str(dest))
        self.assertEqual(len(self.host.registered), 1)
        binding = json.loads((dest / 'mailbox-binding.json').read_text())
        self.assertEqual(binding['mailbox_id'], record['mailbox_id'])
        self.assertEqual(bridge.digest((dest / 'mailbox-binding.json').read_bytes()), record['mailbox_binding_sha256'])
        self.assertEqual(bridge.digest((dest / 'mailbox.patch.yml').read_bytes()), record['mailbox_patch_sha256'])
        safe = hashlib.sha256(('dsh-run:' + dest.name).encode()).hexdigest()[:16]
        registration_file = dest / f'reg-{safe}.json'
        self.assertEqual(record['mailbox_registration_path'], str(registration_file))
        self.assertEqual(record['mailbox_registration_sha256'], bridge.digest(registration_file.read_bytes()))
        response = json.loads((dest / 'response.md').read_text())
        self.assertEqual(response['binding'], str(dest / 'mailbox-binding.json'))
        self.assertIn(str(dest / 'mailbox.patch.yml'), response['argv'])
        patch_text = (dest / 'mailbox.patch.yml').read_text()
        for key in ('bindingFile', 'rootIdentityFile', 'stateDir', 'toolsModule', 'llmModule'):
            self.assertIn(key, patch_text)
        identity = json.loads((dest / 'mailbox-root.json').read_text())
        self.assertEqual(identity['sessionId'], 'session-test')
        self.assertEqual(len(self.host.deactivated), 1)

    def test_resume_reuses_one_mailbox_across_two_continuations(self):
        r1, status = self.run_child(self.session_child())
        self.assertEqual(status, 'completed')
        prior1 = json.loads((r1 / 'result.json').read_text())
        r2, status = self.resume_child(self.session_child(), r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        prior2 = json.loads((r2 / 'result.json').read_text())
        r3, status = self.resume_child(self.session_child(), r2, prior2['cwd'])
        self.assertEqual(status, 'completed')
        prior3 = json.loads((r3 / 'result.json').read_text())
        self.assertEqual({prior1['mailbox_id'], prior2['mailbox_id'], prior3['mailbox_id']},
                         {prior1['mailbox_id']})
        generations = set()
        for dest, rec in ((r1, prior1), (r2, prior2), (r3, prior3)):
            generations.add(json.loads((dest / 'mailbox-binding.json').read_text())['generation'])
            self.assertEqual(rec['mailbox_task_dir'], prior1['mailbox_task_dir'])
            self.assertEqual(rec['mailbox_registration_path'], prior1['mailbox_registration_path'])
            self.assertEqual(rec['mailbox_registration_sha256'], prior1['mailbox_registration_sha256'])
            self.assertEqual(rec['mailbox_label'], prior1['mailbox_label'])
        self.assertEqual(len(generations), 3)
        self.assertEqual(len({entry['reg_file'] for entry in self.host.registered}), 1)
        self.assertEqual(len({entry['token'] for entry in self.host.registered}), 1)

    def test_resume_uses_recorded_evidence_not_the_current_environment(self):
        r1, _ = self.run_child(self.session_child())
        prior1 = json.loads((r1 / 'result.json').read_text())
        # The current chat moves: no binding and a different scope are provided.
        with patch.dict(os.environ, {'AGENT_MAILBOX_BINDING': '', 'AGENT_MAILBOX_SCOPE': 'cc:other-chat'}):
            r2, status = self.resume_child(self.session_child(), r1, prior1['cwd'], )
        self.assertEqual(status, 'completed')
        prior2 = json.loads((r2 / 'result.json').read_text())
        self.assertEqual(prior2['mailbox_id'], prior1['mailbox_id'])
        self.assertEqual(prior2['mailbox_scope'], 'cc:parent-scope')
        self.assertEqual(prior2['mailbox_service_socket'], prior1['mailbox_service_socket'])
        self.assertEqual(prior2['mailbox_parent_binding_path'], prior1['mailbox_parent_binding_path'])

    def test_resume_ignores_a_different_service_path_in_the_environment(self):
        r1, _ = self.run_child(self.session_child())
        prior1 = json.loads((r1 / 'result.json').read_text())
        with patch.dict(os.environ, {'AGENT_MAILBOX_SOCKET': '/nonexistent/other/api.sock',
                                     'AGENT_MAILBOX_ADMIN_TOKEN_PATH': '/nonexistent/other/admin-token'}):
            r2, status = self.resume_child(self.session_child(), r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        prior2 = json.loads((r2 / 'result.json').read_text())
        self.assertEqual(prior2['mailbox_service_socket'], prior1['mailbox_service_socket'])
        self.assertEqual(prior2['mailbox_id'], prior1['mailbox_id'])

    def test_resume_rejects_a_missing_registration_file(self):
        r1, _ = self.run_child(self.session_child())
        prior1 = json.loads((r1 / 'result.json').read_text())
        Path(prior1['mailbox_registration_path']).unlink()
        with self.assertRaisesRegex(ValueError, 'mailbox_registration_missing'):
            self.resume_child(self.session_child(), r1, prior1['cwd'])

    def test_resume_rejects_a_changed_registration_file(self):
        r1, _ = self.run_child(self.session_child())
        prior1 = json.loads((r1 / 'result.json').read_text())
        Path(prior1['mailbox_registration_path']).write_text('{"tampered": true}', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'mailbox_registration_changed'):
            self.resume_child(self.session_child(), r1, prior1['cwd'])

    def test_explicit_team_without_service_fails_before_launch(self):
        self.host.service = False
        with self.assertRaisesRegex(ValueError, 'agent_mailbox_service_unavailable'):
            self.run_child(self.session_child(), team='team-1')
        self.assertFalse((self.runtime / 'runs').exists())
        self.assertFalse(self.host.registered)

    def test_without_confirmed_identity_degrades_without_team(self):
        self.host.parent_binding = None
        with patch.dict(os.environ, {'AGENT_MAILBOX_BINDING': ''}):
            dest, status = self.run_child(self.session_child())
        self.assertEqual(status, 'completed')
        record = json.loads((dest / 'result.json').read_text())
        self.assertEqual(record['mailbox_status'], 'not_connected')
        self.assertNotIn('mailbox_id', record)
        self.assertFalse(self.host.registered)

    def test_explicit_team_without_identity_fails_before_launch(self):
        self.host.parent_binding = None
        with patch.dict(os.environ, {'AGENT_MAILBOX_BINDING': ''}):
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_identity_unavailable'):
                self.run_child(self.session_child(), team='team-1')
        self.assertFalse((self.runtime / 'runs').exists())

    def test_team_with_non_default_policy_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'team_tool_policy_conflict'):
            self.run_child(self.session_child(), team='team-1', tool_policy='none', profile='opencode-go')

    def test_host_disable_switch_only_disables(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': '1', 'AGENT_MAILBOX_BINDING': ''}):
            dest, status = self.run_child(self.session_child())
            self.assertEqual(status, 'completed')
            self.assertEqual(json.loads((dest / 'result.json').read_text())['mailbox_status'], 'disabled')
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_service_unavailable'):
                self.run_child(self.session_child(), team='team-1')
        self.assertFalse(self.host.registered)

    def test_resume_rejects_changed_plugin_bytes(self):
        dest, status = self.run_child(self.session_child())
        self.assertEqual(status, 'completed')
        record = json.loads((dest / 'result.json').read_text())
        changed = self.root / 'changed-plugin.mjs'
        changed.write_bytes(bridge.resolve_dsh_plugin_path().read_bytes() + b'\n// changed\n')
        with patch.object(bridge, 'resolve_dsh_plugin_path', return_value=changed):
            with self.assertRaisesRegex(ValueError, 'plugin_changed'):
                self.resume_child(self.session_child(), dest, record['cwd'])
        self.assertEqual(json.loads((dest / 'result.json').read_text())['mailbox_id'], record['mailbox_id'])

    def test_child_env_never_inherits_the_parent_binding(self):
        self.host.service = False
        child = self.child('import sys,json,os\nsys.stdin.read()\n'
                           'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                           'print(json.dumps({"type":"final","text":json.dumps({'
                           '"binding":os.environ.get("AGENT_MAILBOX_BINDING",""),'
                           '"scope":os.environ.get("AGENT_MAILBOX_SCOPE",""),'
                           '"parent":os.environ.get("AGENT_MAILBOX_PARENT_ID","")})}))\n')
        dest, status = self.run_child(child)
        self.assertEqual(status, 'completed')
        self.assertEqual(json.loads((dest / 'response.md').read_text()),
                         {'binding': '', 'scope': '', 'parent': ''})

    def test_resume_rejects_an_incomplete_mailbox_record(self):
        dest = self.runtime / 'runs/incomplete'
        dest.mkdir(parents=True)
        (dest / 'task.md').write_bytes(self.task.read_bytes())
        (self.runtime / 'work').mkdir(exist_ok=True)
        cwd = str((self.runtime / 'work').resolve())
        record = {'status': 'completed', 'cwd': cwd, 'session_id': 'session-legacy',
                  'source_before_seq': -1, 'task_sha256': bridge.digest(self.task.read_bytes()),
                  'response_sha256': bridge.digest(b'old'),
                  'config_sha256': bridge.digest((self.runtime / bridge.PATCH_NAME).read_bytes()),
                  'mailbox_id': 'mbx_old'}
        bridge.write_json(dest / 'result.json', record)
        with self.assertRaisesRegex(ValueError, 'mailbox_record_incompatible'):
            with redirect_stdout(io.StringIO()):
                bridge.run(self.task, dest, 3, self.runtime, self.session_child())

    def test_host_disable_switch_stops_a_resume(self):
        r1, _ = self.run_child(self.session_child())
        prior1 = json.loads((r1 / 'result.json').read_text())
        self.assertEqual(prior1['mailbox_status'], 'registered')
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': '1'}):
            r2, status = self.resume_child(self.session_child(), r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        record = json.loads((r2 / 'result.json').read_text())
        self.assertEqual(record['mailbox_status'], 'disabled')
        self.assertNotIn('mailbox_id', record)
        self.assertEqual(len(self.host.registered), 1)

    def test_legacy_resume_never_resolves_a_new_mailbox(self):
        dest = self.runtime / 'runs/legacy'
        dest.mkdir(parents=True)
        (dest / 'task.md').write_bytes(self.task.read_bytes())
        (dest / 'response.md').write_text('old')
        (self.runtime / 'work').mkdir(exist_ok=True)
        cwd = str((self.runtime / 'work').resolve())
        record = {'status': 'completed', 'cwd': cwd, 'session_id': 'session-legacy',
                  'source_before_seq': -1, 'task_sha256': bridge.digest(self.task.read_bytes()),
                  'response_sha256': bridge.digest(b'old'),
                  'config_sha256': bridge.digest((self.runtime / bridge.PATCH_NAME).read_bytes())}
        bridge.write_json(dest / 'result.json', record)
        with patch.object(bridge, 'resolve_dsh_modules', side_effect=AssertionError('plan was resolved')):
            with self.assertRaisesRegex(ValueError, 'source_unavailable'):
                with redirect_stdout(io.StringIO()):
                    bridge.run(self.task, dest, 3, self.runtime, self.session_child())
        self.assertFalse(self.host.registered)


class MailboxPlanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.host = FakeHostRuntime(parent_binding=write_parent_binding(self.root))
        self.patches = [
            patch.object(bridge, 'host_runtime', self.host),
            patch.object(bridge, 'resolve_dsh_modules', return_value=('/fake/tools.mjs', '/fake/llm.mjs')),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp.cleanup()

    def test_plan_requires_service_and_identity(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': '', 'AGENT_MAILBOX_BINDING': ''}):
            self.host.service = False
            self.assertEqual(bridge.resolve_mailbox_plan(Path('/tmp/rt'), None, 'default', 'dsh')['status'],
                             'not_connected')
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_service_unavailable'):
                bridge.resolve_mailbox_plan(Path('/tmp/rt'), 't', 'default', 'dsh')

    def test_plan_requires_identity_without_team(self):
        no_identity = FakeHostRuntime(parent_binding=None)
        with patch.object(bridge, 'host_runtime', no_identity), \
                patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': '', 'AGENT_MAILBOX_BINDING': ''}):
            self.assertEqual(bridge.resolve_mailbox_plan(Path('/tmp/rt'), '', 'default', 'dsh')['status'],
                             'not_connected')
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_identity_unavailable'):
                bridge.resolve_mailbox_plan(Path('/tmp/rt'), 't', 'default', 'dsh')

    def test_plan_pins_service_parent_plugin_and_registration_key(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': '', 'AGENT_MAILBOX_BINDING': ''}):
            plan = bridge.resolve_mailbox_plan(Path('/tmp/rt'), 'team-a', 'default', 'dsh',
                                               registration_key='dsh-job:j1')
        self.assertEqual(plan['status'], 'planned')
        self.assertEqual(plan['scope'], 'cc:parent-scope')
        self.assertEqual(plan['parent_id'], 'mbx_parent')
        self.assertEqual(plan['team'], 'team-a')
        self.assertEqual(plan['service_socket'], '/nonexistent/api.sock')
        self.assertEqual(plan['admin_token_path'], '/nonexistent/admin-token')
        self.assertEqual(plan['parent_binding_path'], str(Path(self.host.parent_binding).resolve()))
        self.assertEqual(plan['registration_key'], 'dsh-job:j1')
        self.assertEqual(plan['plugin_sha256'], bridge.digest(bridge.resolve_dsh_plugin_path().read_bytes()))

    def test_explicit_scope_conflicting_with_parent_is_refused(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': '', 'AGENT_MAILBOX_SCOPE': 'cc:other'}):
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_scope_mismatch'):
                bridge.resolve_mailbox_plan(Path('/tmp/rt'), 'team-a', 'default', 'dsh')

    def test_validate_mailbox_requires_the_run_evidence(self):
        plan = {'status': 'planned', 'scope': 's', 'parent_id': 'p', 'team': 't', 'plugin_sha256': 'x',
                'service_socket': '/s', 'parent_binding_path': '/b'}
        bridge.validate_mailbox({'mailbox_scope': 's', 'parent_id': 'p', 'team': 't',
                                 'mailbox_plugin_sha256': 'x', 'mailbox_service_socket': '/s',
                                 'mailbox_parent_binding_path': '/b'}, plan)
        bridge.validate_mailbox({'mailbox_status': 'not_connected'}, plan)
        with self.assertRaisesRegex(ValueError, 'mailbox_binding_mismatch'):
            bridge.validate_mailbox({}, plan)
        with self.assertRaisesRegex(ValueError, 'mailbox_binding_mismatch'):
            bridge.validate_mailbox({'mailbox_status': 'registered', 'mailbox_scope': 's', 'parent_id': 'p',
                                     'team': 't', 'mailbox_plugin_sha256': 'x', 'mailbox_service_socket': '/other',
                                     'mailbox_parent_binding_path': '/b'}, plan)


class JobMailboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test')
        self.task = self.runtime / 'input.md'
        self.task.write_text('任务')
        self.host = FakeHostRuntime(parent_binding=write_parent_binding(self.temp.name))
        self.patches = [
            patch.object(bridge, 'host_runtime', self.host),
            patch.object(bridge, 'resolve_dsh_modules', return_value=('/fake/tools.mjs', '/fake/llm.mjs')),
            patch.object(bridge, 'binary', return_value='/bin/true'),
            patch.object(jobs.subprocess, 'Popen', return_value=MagicMock(pid=4242)),
            patch.dict(os.environ, {'AGENT_MAILBOX_BINDING': ''}, clear=False),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp.cleanup()

    def test_submit_rejects_explicit_team_without_service(self):
        self.host.service = False
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': ''}):
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_service_unavailable'):
                jobs.submit(self.task, 'job-2', runtime=self.runtime, team='team-a')
        self.assertFalse((self.runtime / 'jobs').exists())

    def test_submit_persists_the_fixed_plan_and_reuses_the_id(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': ''}):
            jobs.submit(self.task, 'job-1', runtime=self.runtime, team='team-a')
            request = json.loads((self.runtime / 'jobs/job-1/request.json').read_text())
            self.assertEqual(request['mailbox']['status'], 'planned')
            self.assertEqual(request['mailbox']['team'], 'team-a')
            self.assertEqual(request['mailbox']['registration_key'], 'dsh-job:job-1')
            self.assertEqual(request['mailbox']['service_socket'], '/nonexistent/api.sock')
            self.assertEqual(request['team'], 'team-a')
            again = jobs.submit(self.task, 'job-1', runtime=self.runtime, team='team-a')
            self.assertTrue(again.get('reused'))
            self.assertEqual(again['job_id'], 'job-1')
        self.assertEqual(len(self.host.registered), 0)

    def test_legacy_job_request_without_mailbox_is_reused(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': ''}):
            jobs.submit(self.task, 'legacy-1', runtime=self.runtime, team='team-a')
            request_path = self.runtime / 'jobs/legacy-1/request.json'
            request = json.loads(request_path.read_text())
            request.pop('mailbox')
            bridge.write_json(request_path, request)
            again = jobs.submit(self.task, 'legacy-1', runtime=self.runtime, team='team-a')
        self.assertTrue(again.get('reused'))
        self.assertNotIn('mailbox', json.loads(request_path.read_text()))

    def test_reuse_does_not_require_the_live_service(self):
        with patch.dict(os.environ, {'AGENT_MAILBOX_DISABLE': ''}):
            jobs.submit(self.task, 'job-9', runtime=self.runtime, team='team-a')
            self.host.service = False
            again = jobs.submit(self.task, 'job-9', runtime=self.runtime, team='team-a')
        self.assertTrue(again.get('reused'))


class OfflineMailboxTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name) / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test')
        (self.runtime / 'work').mkdir()
        self.task = self.runtime / 'input.md'
        self.task.write_text('任务')

    def tearDown(self):
        self.temp.cleanup()

    def test_explicit_team_fails_before_preflight(self):
        calls = []
        with patch.object(bridge, 'resolve_mailbox_plan',
                          side_effect=ValueError('agent_mailbox_service_unavailable')), \
                patch.object(offline, 'preflight', side_effect=lambda *a, **k: calls.append('preflight')):
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_service_unavailable'):
                offline.start(self.task, 'k1', str(self.runtime / 'work'), 'title', 30,
                              runtime=self.runtime, team='team-a')
        self.assertEqual(calls, [])


class HostRuntimeContractTests(unittest.TestCase):
    def test_real_host_runtime_exposes_the_contract_signatures(self):
        try:
            import host_runtime
        except ImportError:
            self.skipTest('host_runtime is not installed beside this skill')
        for name, required in [
            ('binding_for_current', []),
            ('compute_scope', ['project', 'session_key', 'cwd', 'socket_path']),
            ('resolve_mailbox_service', []),
            ('check_mailbox_service', ['socket_path']),
            ('register_task_mailbox', ['service_socket', 'admin_token', 'registration_key', 'scope']),
            ('create_binding_file', ['binding_path', 'service_socket', 'token', 'mailbox_id', 'generation']),
            ('activate_mailbox', ['service_socket', 'token', 'generation']),
            ('heartbeat_mailbox', ['service_socket', 'token', 'generation', 'native_session_id']),
            ('deactivate_mailbox', ['service_socket', 'token', 'generation']),
        ]:
            function = getattr(host_runtime, name, None)
            self.assertTrue(callable(function), name)
            parameters = list(inspect.signature(function).parameters)
            for item in required:
                self.assertIn(item, parameters, f'{name}({item})')


def _skip_reason():
    if not REAL_BINARY.is_file():
        return 'agent-mailbox service binary is not built'
    if load_real_host() is None:
        return 'current shared host_runtime is not available'
    return None


class RealServiceTests(unittest.TestCase):
    """Items 2/4 against the installed Go service and the current real host."""

    @classmethod
    def setUpClass(cls):
        cls.skip_reason = _skip_reason()

    def setUp(self):
        if self.skip_reason:
            self.skipTest(self.skip_reason)
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.service_runtime = self.root / 'service'
        self.service_runtime.mkdir()
        self.runtime = self.root / 'dsh-runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test config')
        self.service = subprocess.Popen(
            [str(REAL_BINARY), 'serve', '--runtime', str(self.service_runtime)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
        self.socket = self.service_runtime / 'api.sock'
        self.token_path = self.service_runtime / 'admin-token'
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.socket.exists() and self.token_path.is_file():
                break
            time.sleep(0.05)
        else:
            self.fail('agent-mailbox service did not start')
        self.host = load_real_host()
        self.parent_binding = self.root / 'atlas-binding.json'
        self._register_parent()
        self.env = patch.dict(os.environ, {
            'AGENT_MAILBOX_DISABLE': '',
            'AGENT_MAILBOX_SOCKET': str(self.socket),
            'AGENT_MAILBOX_ADMIN_TOKEN_PATH': str(self.token_path),
            'AGENT_MAILBOX_BINDING': str(self.parent_binding),
            'AGENT_MAILBOX_SCOPE': '', 'AGENT_MAILBOX_PARENT_ID': '',
        }, clear=False)
        self.env.start()
        self.dsh = self.root / 'fake-dsh'
        self.dsh.write_text('#!' + sys.executable + '\nimport sys,json\nsys.stdin.read()\n'
                            'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                            'print(json.dumps({"type":"final","text":"ok"}))\n')
        self.dsh.chmod(0o700)
        self.patches = [
            patch.object(bridge, 'host_runtime', self.host),
            patch.object(bridge, 'resolve_dsh_modules', return_value=('/fake/tools.mjs', '/fake/llm.mjs')),
            patch.object(bridge, 'binary', return_value=str(self.dsh)),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(getattr(self, 'patches', [])):
            item.stop()
        if hasattr(self, 'env'):
            self.env.stop()
        if hasattr(self, 'service'):
            self.service.terminate()
            try:
                self.service.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.service.kill()
                self.service.wait(timeout=5)
        if hasattr(self, 'temp'):
            self.temp.cleanup()

    def _admin(self):
        return self.token_path.read_text(encoding='utf-8').strip()

    def _register_parent(self):
        result = self.host.request_unix(
            self.socket, 'POST', '/v1/register', token=self._admin(),
            payload={'registration_key': 'atlas-real', 'token': 'atlas-token', 'label': 'atlas', 'kind': 'atlas',
                     'coordinator': True, 'scope': 'cc:real-scope', 'team': '', 'parent_id': '', 'metadata': {}})
        parent = result['mailbox']
        self.parent_binding.write_text(json.dumps({
            'schema': 1, 'socket': str(self.socket), 'token': 'atlas-token',
            'mailbox_id': parent['mailbox_id'], 'generation': 'atlas-gen', 'scope': 'cc:real-scope',
        }), encoding='utf-8')

    def _run(self, resume, cwd):
        rows = [{'type': 'session', 'id': 'session-test', 'cwd': cwd}]
        with patch.object(bridge, 'session_rows', return_value=(Path('/unused'), rows)):
            with redirect_stdout(io.StringIO()):
                return bridge.run(self.root / 'task.md', resume, 5, self.runtime, str(self.dsh))

    def _binding(self, dest):
        return json.loads((dest / 'mailbox-binding.json').read_text(encoding='utf-8'))

    def _me(self, token):
        return self.host.request_unix(self.socket, 'GET', '/v1/me', token=token)['mailbox']

    def _activate(self, token, generation):
        self.host.request_unix(self.socket, 'POST', '/v1/activate', token=token,
                               payload={'generation': generation, 'native_session_id': 'session-test',
                                        'capabilities': ['steer']})

    def test_continuous_resume_keeps_one_mailbox_and_new_generations(self):
        (self.root / 'task.md').write_text('真实服务连续续谈', encoding='utf-8')
        r1, status = self._run(None, None)
        self.assertEqual(status, 'completed')
        prior1 = json.loads((r1 / 'result.json').read_text())
        self.assertEqual(prior1['mailbox_status'], 'registered')
        r2, status = self._run(r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        prior2 = json.loads((r2 / 'result.json').read_text())
        r3, status = self._run(r2, prior2['cwd'])
        self.assertEqual(status, 'completed')
        prior3 = json.loads((r3 / 'result.json').read_text())
        self.assertEqual(prior1['mailbox_id'], prior2['mailbox_id'])
        self.assertEqual(prior2['mailbox_id'], prior3['mailbox_id'])
        generations = {json.loads((dest / 'mailbox-binding.json').read_text())['generation']
                       for dest in (r1, r2, r3)}
        self.assertEqual(len(generations), 3)
        self.assertEqual(prior1['mailbox_task_dir'], prior2['mailbox_task_dir'])
        self.assertEqual(prior2['mailbox_task_dir'], prior3['mailbox_task_dir'])
        self.assertEqual(prior1['mailbox_registration_sha256'], prior3['mailbox_registration_sha256'])

    def test_missing_registration_file_is_rejected(self):
        (self.root / 'task.md').write_text('真实服务登记缺失', encoding='utf-8')
        r1, status = self._run(None, None)
        self.assertEqual(status, 'completed')
        prior1 = json.loads((r1 / 'result.json').read_text())
        Path(prior1['mailbox_registration_path']).unlink()
        with self.assertRaisesRegex(ValueError, 'mailbox_registration_missing'):
            self._run(r1, prior1['cwd'])

    def test_resume_ends_the_recorded_generation_when_the_binding_is_gone(self):
        (self.root / 'task.md').write_text('绑定丢失恢复', encoding='utf-8')
        r1, status = self._run(None, None)
        self.assertEqual(status, 'completed')
        prior1 = json.loads((r1 / 'result.json').read_text())
        binding = self._binding(r1)
        self._activate(binding['token'], binding['generation'])
        self.assertTrue(self._me(binding['token'])['active'])
        (r1 / 'mailbox-binding.json').unlink()
        r2, status = self._run(r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        self.assertFalse(self._me(binding['token'])['active'])
        prior2 = json.loads((r2 / 'result.json').read_text())
        self.assertEqual(prior2['mailbox_id'], prior1['mailbox_id'])
        self.assertNotEqual(prior2['mailbox_generation'], prior1['mailbox_generation'])

    def test_resume_ignores_a_corrupt_binding_file(self):
        (self.root / 'task.md').write_text('损坏绑定', encoding='utf-8')
        r1, status = self._run(None, None)
        prior1 = json.loads((r1 / 'result.json').read_text())
        binding = self._binding(r1)
        self._activate(binding['token'], binding['generation'])
        (r1 / 'mailbox-binding.json').write_text('{"garbage": true}', encoding='utf-8')
        r2, status = self._run(r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        self.assertFalse(self._me(binding['token'])['active'])

    def test_resume_does_not_end_a_foreign_active_generation(self):
        (self.root / 'task.md').write_text('其他代次', encoding='utf-8')
        r1, status = self._run(None, None)
        prior1 = json.loads((r1 / 'result.json').read_text())
        binding = self._binding(r1)
        self._activate(binding['token'], binding['generation'])
        self.host.request_unix(self.socket, 'POST', '/v1/deactivate', token=binding['token'],
                               payload={'generation': binding['generation']})
        self._activate(binding['token'], 'gen-foreign')
        (r1 / 'mailbox-binding.json').unlink()
        with self.assertRaisesRegex(ValueError, 'agent_mailbox_generation_foreign'):
            self._run(r1, prior1['cwd'])
        self.assertTrue(self._me(binding['token'])['active'])
        self.assertEqual(self._me(binding['token'])['generation'], 'gen-foreign')

    def test_resume_fails_when_the_recorded_generation_cannot_be_ended(self):
        (self.root / 'task.md').write_text('结束代次失败', encoding='utf-8')
        r1, status = self._run(None, None)
        prior1 = json.loads((r1 / 'result.json').read_text())
        binding = self._binding(r1)
        self._activate(binding['token'], binding['generation'])
        (r1 / 'mailbox-binding.json').unlink()
        with patch.object(self.host, 'deactivate_mailbox', side_effect=OSError('transport down')):
            with self.assertRaisesRegex(ValueError, 'agent_mailbox_generation_unrecovered'):
                self._run(r1, prior1['cwd'])
        self.assertTrue(self._me(binding['token'])['active'])
        self.assertEqual(self._me(binding['token'])['generation'], binding['generation'])

    def test_resume_proceeds_when_the_recorded_generation_already_ended(self):
        (self.root / 'task.md').write_text('已结束代次', encoding='utf-8')
        r1, status = self._run(None, None)
        prior1 = json.loads((r1 / 'result.json').read_text())
        binding = self._binding(r1)
        self._activate(binding['token'], binding['generation'])
        self.host.request_unix(self.socket, 'POST', '/v1/deactivate', token=binding['token'],
                               payload={'generation': binding['generation']})
        (r1 / 'mailbox-binding.json').unlink()
        r2, status = self._run(r1, prior1['cwd'])
        self.assertEqual(status, 'completed')
        self.assertFalse(self._me(binding['token'])['active'])

    def test_claim_replay_and_receipt_after_generation_end(self):
        """The recovery contract the plugin depends on, proved on the real service."""
        child = self.host.request_unix(
            self.socket, 'POST', '/v1/register', token=self._admin(),
            payload={'registration_key': 'child-real', 'token': 'child-token', 'label': 'hermes', 'kind': 'hermes',
                     'coordinator': False, 'scope': 'cc:real-scope', 'team': 'team-real',
                     'parent_id': json.loads(self.parent_binding.read_text())['mailbox_id'], 'metadata': {}})['mailbox']
        self.host.request_unix(self.socket, 'POST', '/v1/activate', token='child-token',
                               payload={'generation': 'gen-1', 'native_session_id': 'session-x', 'capabilities': ['steer']})
        sent = self.host.request_unix(
            self.socket, 'POST', '/v1/send', token='atlas-token',
            payload={'client_message_id': 'c1', 'to': child['mailbox_id'], 'body': 'hello child',
                     'task_id': 'task-1', 'reply_to': ''})['message']
        claimed = self.host.request_unix(self.socket, 'POST', '/v1/claim', token='child-token',
                                         payload={'generation': 'gen-1', 'request_id': 'req-1'})
        self.assertEqual(claimed['message']['message_id'], sent['message_id'])
        self.host.request_unix(self.socket, 'POST', '/v1/deactivate', token='child-token',
                               payload={'generation': 'gen-1'})
        # Replaying the original request with its original generation returns history.
        replayed = self.host.request_unix(self.socket, 'POST', '/v1/claim', token='child-token',
                                          payload={'generation': 'gen-1', 'request_id': 'req-1'})
        self.assertEqual(replayed['attempt_id'], claimed['attempt_id'])
        self.assertEqual(replayed['message']['message_id'], sent['message_id'])
        # A different generation cannot reuse the request id.
        with self.assertRaises(Exception) as conflict:
            self.host.request_unix(self.socket, 'POST', '/v1/claim', token='child-token',
                                   payload={'generation': 'gen-2', 'request_id': 'req-1'})
        self.assertEqual(getattr(conflict.exception, 'code', None), 'request_id_conflict')
        # The ended generation may still write the receipt for its own attempt.
        applied = self.host.request_unix(
            self.socket, 'POST', '/v1/receipt', token='child-token',
            payload={'generation': 'gen-1', 'message_id': sent['message_id'],
                     'attempt_id': claimed['attempt_id'], 'state': 'accepted', 'evidence': 'dsh_inbox:native-1'})
        self.assertEqual(applied['message']['state'], 'accepted')
        self.assertEqual(applied['message']['receipts'][0]['evidence'], 'dsh_inbox:native-1')


if __name__ == '__main__':
    unittest.main()
