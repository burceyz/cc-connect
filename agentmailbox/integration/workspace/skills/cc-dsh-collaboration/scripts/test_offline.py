"""Evidence gates and offline handoff behavior, without a live model or webhook."""
from copy import deepcopy
import errno
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

import bridge
import offline
import shell_probe

# These legacy suites never touch a live mailbox service.
os.environ.setdefault('AGENT_MAILBOX_DISABLE', '1')


def evidence(cwd):
    record = {'status': 'completed', 'returncode': 0, 'session_id': 'session-test'}
    rows = [
        {'type': 'session', 'id': 'session-test', 'cwd': str(cwd)},
        {'type': 'sandbox/mode', 'data': {'mode': 'workspace-write'}},
        {'type': 'permission/preset', 'data': {'preset': 'workspace-write'}},
        {'type': 'request/header', 'data': {'header': {'config': {
            'provider': 'opencode-go', 'model': 'deepseek-v4.1-flash', 'reasoningEffort': 'high'}}}},
        {'type': 'tool/call', 'data': {'name': 'bash', 'callId': 'call-1',
                                     'arguments': json.dumps({'command': 'exact command'})}},
        {'type': 'tool/result', 'data': {'message': {'toolCallId': 'call-1',
            'content': [{'type': 'text', 'text': 'DSH_PREFLIGHT_OK::nonce'}], 'isError': False}}},
    ]
    return rows, record


class ProbeEvidenceTests(unittest.TestCase):
    def test_exact_native_execution_passes(self):
        rows, record = evidence('/repo')
        result = offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')
        self.assertEqual(result['mode'], 'workspace-write')
        self.assertEqual(result['reasoning_effort'], 'high')

    def test_model_final_alone_cannot_pass(self):
        rows, record = evidence('/repo')
        rows = rows[:4] + [{'type': 'assistant/message', 'data': {'text': 'DSH_PREFLIGHT_OK::nonce'}}]
        with self.assertRaisesRegex(ValueError, 'unexpected_tools'):
            offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')

    def test_native_timeout_ms_keeps_command_and_permission_checks(self):
        rows, record = evidence('/repo')
        args = {'command': 'exact command', 'description': 'Run probe once', 'timeoutMs': 120000}
        rows[4]['data']['arguments'] = json.dumps(args)
        offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')
        for change in [{'command': 'echo forged'}, {'workdir': '/other'}]:
            with self.subTest(change=change):
                rows[4]['data']['arguments'] = json.dumps(args | change)
                with self.assertRaisesRegex(ValueError, 'probe_command_mismatch'):
                    offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')
        for change in [{'sandbox_permissions': 'require_escalated'}, {'unknownOption': True}, {'justification': 'User authorized'}]:
            with self.subTest(change=change):
                rows[4]['data']['arguments'] = json.dumps(args | change)
                with self.assertRaisesRegex(ValueError, 'probe_arguments_invalid'):
                    offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')

    def test_exact_command_with_illegal_justification_and_is_error_distinguished(self):
        # SYNC-01 regression: exact command + illegal justification causes argument rejection;
        # if tool execution failed natively (isError=True), probe_shell_failed is raised.
        rows, record = evidence('/repo')
        args_with_justification = {
            'command': 'exact command',
            'description': 'Run authorized offline shell probe script',
            'justification': 'User explicitly authorized this single offline probe command'
        }
        rows[4]['data']['arguments'] = json.dumps(args_with_justification)
        with self.assertRaisesRegex(ValueError, 'probe_arguments_invalid'):
            offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')

        # When arguments are clean but execution returned DSH invalid escalation error:
        rows[4]['data']['arguments'] = json.dumps({'command': 'exact command', 'description': 'Run probe once'})
        rows[5]['data']['message']['isError'] = True
        rows[5]['data']['message']['content'] = [{'type': 'text', 'text': 'Error: invalid escalation: justification is only valid together with sandbox_permissions'}]
        with self.assertRaisesRegex(ValueError, 'probe_shell_failed'):
            offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')

    def test_explicit_shell_workdir_must_equal_verified_cwd(self):
        rows, record = evidence('/repo')
        rows[4]['data']['arguments'] = json.dumps({'command': 'exact command', 'workdir': '/repo'})
        offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')
        for workdir in ['/other', '.', None, '/repo/../repo']:
            rows[4]['data']['arguments'] = json.dumps({'command': 'exact command', 'workdir': workdir})
            with self.assertRaisesRegex(ValueError, 'probe_command_mismatch'):
                offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')

    def test_bad_session_permissions_model_command_or_result_rejected(self):
        original, record = evidence('/repo')
        changes = [
            (0, lambda r: r.update(cwd='/other')),
            (1, lambda r: r['data'].update(mode='danger-full-access')),
            (3, lambda r: r['data']['header']['config'].update(reasoningEffort='max')),
            (3, lambda r: r['data']['header']['config'].update(reasoningEffort='low')),
            (4, lambda r: r['data'].update(arguments=json.dumps({'command': 'echo forged'}))),
            (4, lambda r: r['data'].update(arguments=json.dumps(
                {'command': 'exact command', 'sandbox_permissions': 'require_escalated'}))),
            (5, lambda r: r['data']['message'].update(isError=True)),
            (5, lambda r: r['data']['message'].update(content=[{'type': 'text', 'text': 'no marker'}])),
        ]
        for index, change in changes:
            with self.subTest(index=index):
                rows = deepcopy(original)
                change(rows[index])
                with self.assertRaises(ValueError):
                    offline.verify_probe(rows, record, Path('/repo'), 'exact command', 'nonce')

    def test_other_tool_or_additional_mode_change_rejected(self):
        for extra in [{'type': 'tool/call', 'data': {'name': 'write'}},
                      {'type': 'sandbox/mode', 'data': {'mode': 'danger-full-access'}}]:
            rows, record = evidence('/repo')
            with self.assertRaises(ValueError):
                offline.verify_probe(rows + [extra], record, Path('/repo'), 'exact command', 'nonce')


class OfflineStartTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='.offline-test-')
        self.root = Path(self.temp.name).resolve()
        self.workspace_patch = patch.object(bridge, 'WORKSPACE', self.root)
        self.workspace_patch.start()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('fixture')
        self.target = self.root / 'repo'
        self.target.mkdir()
        self.task = self.root / 'task.md'
        self.task.write_text('repair')
        self.key = 'offline-test-' + uuid.uuid4().hex
        self.signals = Path('/private/tmp') / ('dsh-' + self.key)
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()
        os.environ.pop('DSH_PERMISSION_MODE', None)

    def tearDown(self):
        self.workspace_patch.stop()
        self.env.stop()
        if self.signals.exists():
            self.signals.rmdir()
        self.temp.cleanup()
        self.temp.cleanup()

    def start(self):
        return offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime)

    def test_failed_preflight_never_submits(self):
        with patch.object(offline, 'preflight', side_effect=ValueError('probe_shell_failed')), \
                patch.object(offline.jobs, 'submit') as submit, patch.object(offline, 'emit_once'):
            with self.assertRaisesRegex(ValueError, 'probe_shell_failed'):
                self.start()
            submit.assert_not_called()
        state = json.loads((self.runtime / 'offline' / self.key / 'state.json').read_text())
        self.assertEqual(state['status'], 'not_submitted')

    def test_conversation_target_is_fixed_before_preflight(self):
        from test_ipc import binding
        with patch.object(offline.ipc, 'capture', return_value=binding()) as capture, \
                patch.object(offline, 'preflight', return_value={}), \
                patch.object(offline.jobs, 'submit', return_value={'status': 'accepted'}) as submit, \
                patch.object(offline.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime,
                          delivery='conversation')
            capture.assert_called_once()
            self.assertEqual(submit.call_args.kwargs['callback_target'], binding())
            self.assertEqual(submit.call_args.kwargs['delivery'], 'conversation')

    def test_unavailable_conversation_receiver_does_not_run_preflight(self):
        with patch.object(offline.ipc, 'capture', side_effect=ValueError('ipc_receiver_unavailable')), \
                patch.object(offline, 'preflight') as preflight:
            with self.assertRaisesRegex(ValueError, 'receiver_unavailable'):
                offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime,
                              delivery='conversation')
            preflight.assert_not_called()

    def test_pass_submits_same_target_and_starts_monitor(self):
        with patch.object(offline, 'preflight', return_value={'mode': 'workspace-write'}), \
                patch.object(offline.jobs, 'submit', return_value={'status': 'accepted'}) as submit, \
                patch.object(offline.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            self.assertEqual(self.start()['status'], 'accepted')
            self.assertEqual(submit.call_args.kwargs['cwd'], self.target)
            self.assertEqual(os.environ['DSH_PERMISSION_MODE'], 'workspace-write')
            popen.assert_called_once()

    def test_changed_patch_after_probe_blocks_submission(self):
        def change(*args):
            (self.runtime / bridge.PATCH_NAME).write_text('changed')
            return {'mode': 'workspace-write'}
        with patch.object(offline, 'preflight', side_effect=change), \
                patch.object(offline.jobs, 'submit') as submit, patch.object(offline, 'emit_once'):
            with self.assertRaisesRegex(ValueError, 'config_changed'):
                self.start()
            submit.assert_not_called()

    def test_existing_job_reuses_without_model_preflight(self):
        dest = self.runtime / 'jobs' / self.key
        dest.mkdir(parents=True)
        bridge.write_json(dest / 'request.json', {})
        with patch.object(offline, 'preflight') as check, patch.object(
                offline.jobs, 'submit', return_value={'status': 'running', 'reused': True}):
            self.assertTrue(self.start()['reused'])
            check.assert_not_called()

    def test_unconfined_environment_rejected(self):
        os.environ['DSH_PERMISSION_MODE'] = 'danger-full-access'
        with patch.object(offline, 'preflight') as check:
            with self.assertRaisesRegex(ValueError, 'unexpected_permission_environment'):
                self.start()
            check.assert_not_called()

    def test_failed_monitor_does_not_claim_job_was_not_started(self):
        with patch.object(offline, 'preflight', return_value={}), patch.object(
                offline.jobs, 'submit', return_value={'status': 'accepted'}), patch.object(
                offline.subprocess, 'Popen', side_effect=OSError()):
            result = self.start()
        self.assertEqual(result, {'status': 'accepted', 'milestone_monitor': 'launch_failed'})

    def test_notification_attempt_is_not_repeated(self):
        with patch.object(offline.notify, 'load_config', return_value={'fixture': True}), \
                patch.object(offline.notify, 'send', return_value={'status': 'unknown'}) as send:
            offline.emit_once(self.root, 'one', '任务进度', self.runtime)
            offline.emit_once(self.root, 'one', '任务进度', self.runtime)
            send.assert_called_once()

    def test_team_passed_to_jobs_submit(self):
        with patch.object(offline, 'preflight', return_value={'mode': 'workspace-write'}), \
                patch.object(offline.bridge, 'resolve_mailbox_plan', return_value={'status': 'planned'}), \
                patch.object(offline.jobs, 'submit', return_value={'status': 'accepted'}) as submit, \
                patch.object(offline.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            res = offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime, team='team-mars')
            self.assertEqual(res['status'], 'accepted')
            self.assertEqual(submit.call_args.kwargs['team'], 'team-mars')
            saved_req = json.loads((self.runtime / 'offline' / self.key / 'request.json').read_text())
            self.assertEqual(saved_req.get('team'), 'team-mars')

    def test_team_conflict_rejected(self):
        with patch.object(offline, 'preflight', return_value={'mode': 'workspace-write'}), \
                patch.object(offline.bridge, 'resolve_mailbox_plan', return_value={'status': 'planned'}), \
                patch.object(offline.jobs, 'submit', return_value={'status': 'accepted'}), \
                patch.object(offline.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime, team='team-mars')
            with self.assertRaisesRegex(ValueError, 'offline_request_conflict'):
                offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime, team='team-venus')

    def test_team_inherited_on_resume(self):
        dest = self.runtime / 'jobs' / self.key
        dest.mkdir(parents=True)
        bridge.write_json(dest / 'request.json', {'team': 'team-mars'})
        with patch.object(offline, 'preflight') as check, \
                patch.object(offline.bridge, 'resolve_mailbox_plan', return_value={'status': 'planned'}), \
                patch.object(offline.jobs, 'submit', return_value={'status': 'accepted'}) as submit, \
                patch.object(offline.subprocess, 'Popen') as popen:
            popen.return_value.pid = 12345
            offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime, team='team-mars')
            check.assert_not_called()
            # Second call without team
            offline.start(self.task, self.key, self.target, '测试任务', 60, self.runtime, team=None)
            self.assertEqual(submit.call_args.kwargs['team'], 'team-mars')


class ShellProbeTests(unittest.TestCase):
    def test_real_readwrite_and_unittest_with_simulated_outside_denial(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            target = root / 'repo'
            target.mkdir()
            outside = root / 'outside'
            actual_open = Path.open
            def guarded_open(path, *args, **kwargs):
                if path == outside:
                    raise PermissionError(errno.EACCES, 'fixture denied')
                return actual_open(path, *args, **kwargs)
            prior = Path.cwd()
            try:
                os.chdir(target)
                with patch.object(Path, 'open', guarded_open):
                    shell_probe.probe(target, outside, 'unit-test')
                self.assertEqual(list(target.iterdir()), [])
            finally:
                os.chdir(prior)

    def test_outside_write_success_fails_closed(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder).resolve()
            target = root / 'repo'
            target.mkdir()
            prior = Path.cwd()
            try:
                os.chdir(target)
                with self.assertRaisesRegex(ValueError, 'outside_write_was_allowed'):
                    shell_probe.probe(target, root / 'outside', 'unit-test')
            finally:
                os.chdir(prior)


if __name__ == '__main__':
    unittest.main()
