import fcntl
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch
import urllib.error

import bridge
import jobs
import notify


class NotificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.runtime = Path(self.temp.name)
        self.dest = self.runtime / 'jobs/test-one'
        self.dest.mkdir(parents=True)
        self.config = {'enabled': True,
                       'webhook': 'https://oapi.dingtalk.com/robot/send?access_token=' + 'a' * 64}
        self.path = self.runtime / notify.CONFIG
        self.path.write_text(json.dumps(self.config))
        self.path.chmod(0o600)
        self.record = {'job_id': 'test-one', 'status': 'completed', 'title': '通知测试',
                       'ended': '2026-09-30T12:00:00+00:00', 'notification_requested': True,
                       'task': 'private task must not appear', 'response': 'private answer'}
        bridge.write_json(self.dest / 'receipt.json', self.record)
        bridge.write_json(self.dest / 'request.json', {})

    def tearDown(self):
        self.temp.cleanup()

    def test_template_uses_only_summary_fields_and_beijing_time(self):
        for status in ['completed', 'timeout', 'interrupted', 'incomplete', 'launch_failed']:
            text = notify.message(self.record | {'status': status})
            self.assertIn('任务进度', text)
            self.assertIn('Hermes（DSH）', text)
            self.assertIn('Atlas（Codex）', text)
            self.assertIn('查 DSH 结果 test-one', text)
            self.assertIn('20:00:00 +08:00', text)
            self.assertNotIn('private', text)
            self.assertNotIn('复核为准', text)
            self.assertNotIn('已推送', text)
            self.assertNotIn('已完成', text if status != 'completed' else '')
            if status == 'completed':
                self.assertIn('本轮执行已结束', text)
                self.assertIn('是否完成需求', text)
                self.assertNotIn('任务已完成', text)
        for title in ['a\nb', 'x' * 81, 'https://example.com', 'access_token=foo']:
            with self.assertRaises(ValueError):
                notify.validate_title(title)

    def test_delivery_template_does_not_claim_ipc_delivery_from_dingtalk(self):
        normal = notify.message(self.record)
        continued = notify.message(self.record | {'delivery': 'conversation'})
        self.assertIn('通知结束；无需Atlas自动收尾', normal)
        self.assertIn('会话接续；是否送达原会话，以IPC回执为准', continued)
        self.assertNotIn('已唤起', continued)

    def test_config_permissions_endpoint_and_disable(self):
        self.assertEqual(notify.load_config(self.runtime), self.config)
        self.path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'private_config_permissions'):
            notify.load_config(self.runtime)
        self.path.chmod(0o600)
        for webhook in ['http://oapi.dingtalk.com/robot/send?access_token=x',
                        'https://evil.example/robot/send?access_token=x',
                        self.config['webhook'] + '&next=evil']:
            self.path.write_text(json.dumps(self.config | {'webhook': webhook}))
            with self.assertRaises(ValueError):
                notify.load_config(self.runtime)
        self.path.write_text('{"enabled":false}')
        self.assertIsNone(notify.load_config(self.runtime))

    def test_notifies_once_and_queries_never_send(self):
        with patch.object(notify, 'send', return_value={'status': 'sent', 'errcode': 0}) as sender:
            result = notify.finish(self.dest, self.record, self.runtime)
            self.assertEqual(result['status'], 'sent')
            notify.finish(self.dest, self.record, self.runtime)
            self.assertEqual(jobs.status('test-one', self.runtime)['notification']['status'], 'sent')
            sender.assert_called_once()
        self.assertNotIn(self.config['webhook'], (self.dest / 'notification.json').read_text())

    def test_notification_errors_do_not_change_task_or_retry(self):
        for outcome in ['failed', 'unknown']:
            with self.subTest(outcome=outcome):
                (self.dest / 'notification.json').unlink(missing_ok=True)
                with patch.object(notify, 'send', return_value={'status': outcome}) as sender:
                    notify.finish(self.dest, self.record, self.runtime)
                    notify.finish(self.dest, self.record, self.runtime)
                    sender.assert_called_once()
                self.assertEqual(jobs.status('test-one', self.runtime)['status'], 'completed')

    def test_interrupted_attempt_is_unknown_without_automatic_retry(self):
        bridge.write_json(self.dest / 'notification.json', {'status': 'sending'})
        with patch.object(notify, 'send') as sender:
            self.assertEqual(notify.read(self.dest)['status'], 'unknown')
            self.assertEqual(notify.once(self.dest, self.record, self.runtime)['status'], 'unknown')
            sender.assert_not_called()

    def test_concurrent_sender_cannot_send(self):
        with (self.dest / 'notification.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(notify, 'send') as sender:
                self.assertEqual(notify.once(self.dest, self.record, self.runtime)['status'], 'sending')
                sender.assert_not_called()

    def test_old_active_disabled_and_bad_config_jobs_do_not_send(self):
        with patch.object(notify, 'send') as sender:
            self.assertIsNone(notify.once(self.dest, self.record | {'notification_requested': False}, self.runtime))
            self.assertIsNone(notify.once(self.dest, self.record | {'status': 'running'}, self.runtime))
            self.path.write_text('{"enabled":false}')
            self.assertEqual(notify.once(self.dest, self.record, self.runtime)['status'], 'skipped')
            (self.dest / 'notification.json').unlink()
            self.path.write_text('not json')
            self.assertEqual(notify.once(self.dest, self.record, self.runtime)['status'], 'failed')
            sender.assert_not_called()

    def test_no_send_when_attempt_cannot_be_persisted(self):
        with patch.object(bridge, 'write_json', side_effect=OSError('disk full')):
            with patch.object(notify, 'send') as sender:
                self.assertEqual(notify.finish(self.dest, self.record, self.runtime)['status'], 'unknown')
                sender.assert_not_called()
        self.assertEqual(jobs.status('test-one', self.runtime)['status'], 'completed')

    def test_transport_accepts_only_business_success_and_never_returns_body(self):
        response = Mock(status=200)
        response.__enter__ = Mock(return_value=response)
        response.__exit__ = Mock(return_value=False)
        opener = Mock()
        opener.open.return_value = response
        with patch.object(notify.urllib.request, 'build_opener', return_value=opener):
            for raw, expected in [(b'{"errcode":0}', 'sent'),
                                  (b'{"errcode":310000,"errmsg":"token secret"}', 'failed'),
                                  (b'{"errcode":false}', 'unknown'),
                                  (b'bad json secret', 'unknown'),
                                  (b'x' * 8193, 'unknown')]:
                response.read.return_value = raw
                outcome = notify.post(self.config, '任务进度 测试')
                self.assertEqual(outcome['status'], expected)
                self.assertNotIn('secret', json.dumps(outcome))
            request = opener.open.call_args.args[0]
            self.assertEqual(json.loads(request.data)['text']['content'], '任务进度 测试')
            opener.open.side_effect = urllib.error.URLError(self.config['webhook'])
            outcome = notify.post(self.config, '任务进度 测试')
            self.assertEqual(outcome['status'], 'unknown')
            self.assertNotIn(self.config['webhook'], json.dumps(outcome))

    def test_redirects_rejected_and_network_deadline_enforced(self):
        self.assertIsNone(notify.NoRedirect().redirect_request(None, None, 302, '', {}, 'https://evil.example'))
        with patch.object(notify.subprocess, 'run', side_effect=subprocess.TimeoutExpired('sender', 15)) as proc:
            outcome = notify.send(self.config, '任务进度 测试')
            self.assertEqual(outcome['status'], 'unknown')
            self.assertEqual(proc.call_args.kwargs['timeout'], 15)
            self.assertNotIn(self.config['webhook'], str(proc.call_args.args))

    def test_worker_saves_terminal_receipt_before_notification(self):
        (self.runtime / bridge.PATCH_NAME).write_text('test')
        task = b'test task'
        (self.dest / 'task.md').write_bytes(task)
        bridge.write_json(self.dest / 'request.json', {
            'task_sha256': bridge.digest(task), 'config_sha256': bridge.digest(b'test'),
            'timeout': 1, 'executable': '/nonexistent-test-dsh'})
        def check_saved(config, content):
            saved = json.loads((self.dest / 'receipt.json').read_text())
            self.assertEqual(saved['status'], 'timeout')
            self.assertIn('ended', saved)
            return {'status': 'sent', 'errcode': 0}
        bridge.write_json(self.dest / 'receipt.json', self.record | {'status': 'accepted'})
        fd = os.open(self.dest / 'worker.lock', os.O_CREAT | os.O_WRONLY, 0o600)
        with patch.object(bridge, 'run', return_value=(None, 'timeout')):
            with patch.object(jobs.signal, 'signal'), patch.object(notify, 'send', side_effect=check_saved) as sender:
                jobs.worker('test-one', self.runtime, fd)
                sender.assert_called_once()
        self.assertEqual(jobs.status('test-one', self.runtime)['notification']['status'], 'sent')

    def test_launch_failed_is_notified_without_retrying_model(self):
        (self.runtime / bridge.PATCH_NAME).write_text('test')
        task = self.runtime / 'task.md'
        task.write_text('test')
        with patch.object(jobs.subprocess, 'Popen', side_effect=OSError('test')) as launch:
            with patch.object(notify, 'send', return_value={'status': 'sent', 'errcode': 0}) as sender:
                result = jobs.submit(task, 'launch-test', runtime=self.runtime, executable='/test')
                self.assertEqual(result['status'], 'launch_failed')
                self.assertEqual(result['notification']['status'], 'sent')
                jobs.submit(task, 'launch-test', runtime=self.runtime, executable='/test')
                sender.assert_called_once()
                launch.assert_called_once()


if __name__ == '__main__':
    unittest.main()
