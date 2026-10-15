import json
from pathlib import Path
import tempfile
import unittest

import native


def event(kind, **value):
    return json.dumps({'event': kind, kind: value}, ensure_ascii=False)


class StreamTests(unittest.TestCase):
    def start(self, expected=None):
        stream = native.Stream(expected)
        stream.feed(event('init', conversation_id='session-1', model=native.MODEL))
        self.step(stream, 0, 'user_input', 'DONE')
        return stream

    def step(self, stream, index, kind, state, text='', sid='session-1'):
        stream.feed(event('step_update', conversation_id=sid, step_index=index,
                          step_type=kind, state=state, text_delta=text))

    def finish(self, stream, sid='session-1'):
        stream.feed(event('result', conversation_id=sid, status='SUCCESS', response='aggregate'))

    def test_report_contains_only_last_complete_reply(self):
        stream = self.start()
        self.step(stream, 1, 'agent_response', 'DONE', '正在查证。')
        self.step(stream, 2, 'tool', 'DONE')
        self.step(stream, 3, 'agent_response', 'ACTIVE', '结论：\n')
        self.step(stream, 3, 'agent_response', 'DONE', '完整正文。')
        self.step(stream, 4, 'finish', 'DONE')
        self.finish(stream)
        self.assertEqual(stream.report(), '结论：\n完整正文。')

    def test_resume_must_keep_exact_identity(self):
        stream = self.start('other-session')
        self.assertEqual(stream.error, 'invalid_native_stream')

    def test_missing_result_empty_reply_and_later_tool_are_incomplete(self):
        for mode in ('missing', 'empty', 'later_tool', 'unfinished'):
            with self.subTest(mode=mode):
                stream = self.start()
                self.step(stream, 1, 'agent_response', 'ACTIVE' if mode == 'unfinished' else 'DONE',
                          '' if mode == 'empty' else '报告')
                if mode == 'later_tool':
                    self.step(stream, 2, 'tool', 'DONE')
                if mode != 'missing':
                    self.finish(stream)
                self.assertIsNone(stream.report())

    def test_duplicate_terminal_and_foreign_step_fail(self):
        stream = self.start()
        self.step(stream, 1, 'agent_response', 'DONE', '报告')
        self.finish(stream)
        self.finish(stream)
        self.assertEqual(stream.error, 'invalid_native_stream')
        stream = self.start()
        self.step(stream, 1, 'agent_response', 'DONE', '串错', sid='other')
        self.assertEqual(stream.error, 'invalid_native_stream')

    def test_long_chinese_text_preserves_exact_bytes(self):
        text = '中文资料🙂\n' * 50000
        stream = self.start()
        self.step(stream, 1, 'agent_response', 'DONE', text)
        self.finish(stream)
        self.assertEqual(stream.report().encode(), text.encode())

    def test_native_failure_never_becomes_complete(self):
        stream = self.start()
        self.step(stream, 1, 'agent_response', 'DONE', '部分结果')
        stream.feed(event('result', conversation_id='session-1', status='ERROR'))
        self.assertIsNone(stream.report())
        self.assertEqual(''.join(stream.partial), '部分结果')

    def test_recovered_native_error_keeps_its_diagnostic_but_is_not_success(self):
        stream = self.start()
        self.step(stream, 1, 'error_message', 'DONE')
        self.step(stream, 2, 'agent_response', 'DONE', '可见答复')
        stream.feed(event('result', conversation_id='session-1', status='ERROR',
            error='Malformed function call: SECRET request body',
            usage={'input_tokens': 10, 'output_tokens': 2, 'private': 'SECRET'}))
        diagnostic = stream.diagnostic()
        self.assertEqual(diagnostic['error']['code'], 'malformed_function_call')
        self.assertEqual(diagnostic['error_step_indices'], [1])
        self.assertEqual(diagnostic['usage'], {'input_tokens': 10, 'output_tokens': 2})
        self.assertNotIn('SECRET', json.dumps(diagnostic))
        self.assertIsNone(stream.report())
        # An ERROR terminal does not make trailing malformed events acceptable.
        stream.feed(event('result', conversation_id='session-1', status='SUCCESS'))
        self.assertEqual(stream.error, 'invalid_native_stream')

    def test_historical_terminal_error_is_separate_from_current_error_steps(self):
        stream = self.start()
        self.step(stream, 1, 'agent_response', 'DONE', '本轮答复')
        stream.feed(event('result', conversation_id='session-1', status='ERROR',
                          error='agent executor error: Error 400, Message: SECRET'))
        self.assertEqual(stream.diagnostic()['error_step_indices'], [])
        self.assertEqual(stream.diagnostic()['error']['http_status'], 400)
        self.assertIsNone(stream.report())
        quota = native.error_summary('RESOURCE_EXHAUSTED (code 429): Individual quota reached. SECRET')
        self.assertEqual(quota['code'], 'quota_exceeded')
        self.assertEqual(quota['http_status'], 429)
        self.assertNotIn('SECRET', json.dumps(quota))


class ArchiveTests(unittest.TestCase):
    def test_archive_refuses_symlinks_without_copying_target(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'source').mkdir()
            (root / 'outside').write_text('do not copy')
            (root / 'source/link').symlink_to(root / 'outside')
            with self.assertRaisesRegex(ValueError, 'external_link'):
                native.copy_tree(root / 'source', root / 'archive')
            self.assertFalse((root / 'archive').exists())

    def test_archive_preserves_native_relative_log_link(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'source').mkdir()
            (root / 'source/log.1').write_bytes(b'log')
            (root / 'source/cli.log').symlink_to('log.1')
            native.copy_tree(root / 'source', root / 'archive')
            self.assertTrue((root / 'archive/cli.log').is_symlink())
            self.assertEqual((root / 'archive/cli.log').read_bytes(), b'log')

    def test_archive_preserves_regular_files(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'source/data').mkdir(parents=True)
            (root / 'source/data/report').write_bytes('资料\n'.encode())
            native.copy_tree(root / 'source', root / 'archive')
            self.assertEqual((root / 'archive/data/report').read_bytes(), '资料\n'.encode())

    def test_archive_preserves_native_helper_user_execute_bit(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'source').mkdir()
            helper = root / 'source/helper'
            helper.write_bytes(b'helper')
            helper.chmod(0o755)
            native.copy_tree(root / 'source', root / 'archive')
            self.assertEqual((root / 'archive/helper').stat().st_mode & 0o777, 0o700)


if __name__ == '__main__':
    unittest.main()
