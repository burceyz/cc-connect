"""Failed replies remain failed; export must match the immutable native source."""
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import dorothy
import native


class VisibleExportTests(unittest.TestCase):
    def setUp(self):
        parent = dorothy.WORKSPACE / '.local/test-visible-export'
        parent.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(dir=parent)
        self.addCleanup(self.temp.cleanup)
        self.runtime = Path(self.temp.name)
        self.dest = self.runtime / 'jobs/failed'
        self.archive = self.dest / 'native'
        self.path = self.archive / 'antigravity-cli/brain/session-1/.system_generated/logs/transcript_full.jsonl'
        self.path.parent.mkdir(parents=True)
        self.task = '核对当前原文。'
        (self.dest / 'task.md').write_text(self.task)
        (self.dest / 'request.json').write_text(json.dumps({'task_sha256': dorothy.digest(self.task.encode())}))
        self.rows = [
            {'step_index': 0, 'type': 'USER_INPUT', 'status': 'DONE', 'content': '另一轮任务'},
            {'step_index': 1, 'type': 'PLANNER_RESPONSE', 'status': 'DONE', 'content': '另一轮答复'},
            {'step_index': 2, 'type': 'USER_INPUT', 'status': 'DONE', 'content': self.task},
            {'step_index': 3, 'type': 'ERROR_MESSAGE', 'status': 'DONE',
             'content': 'Malformed function call SECRET_ERROR'},
            {'step_index': 4, 'type': 'GENERIC', 'status': 'DONE', 'content': 'SECRET_TOOL_OUTPUT'},
            {'step_index': 5, 'type': 'PLANNER_RESPONSE', 'status': 'DONE',
             'content': '完整可见答复🙂', 'thinking': 'HIDDEN_THINKING'},
        ]
        self.state = {'native_status': 'ERROR', 'protocol': {'input_accepted': True,
                      'reply_done': True, 'reply_index': 5, 'last_step_index': 5, 'last_step_type': 'agent_response'},
                      'conversation_id': 'session-1'}
        self.save_archive()
        (self.dest / 'partial.md').write_text('完整可见答复🙂\n')
        self.value = {'status': 'native_run_failed', 'returncode': 0,
                      'cleanup': {'worker': 'absent', 'native_group': 'absent'}}
        self.addCleanup(patch.stopall)
        patch.object(dorothy, 'status', return_value=self.value).start()

    def save_archive(self):
        self.path.write_text('\n'.join(json.dumps(row) for row in self.rows) + '\n')
        self.state['native_sha256'] = dorothy.tree_digest(self.archive)
        (self.dest / 'state.json').write_text(json.dumps(self.state))

    def test_failed_turn_can_export_verified_visible_text_without_becoming_a_result(self):
        before = (self.dest / 'state.json').read_bytes()
        value = dorothy.export_visible('failed', self.runtime)
        self.assertEqual(value['status'], 'native_run_failed')
        self.assertFalse(value['native_success'])
        self.assertEqual(value['visible_reply'], '完整可见答复🙂\n')
        self.assertEqual(len(value['visible_messages']), 2)
        self.assertEqual(value['native_error_steps'][0]['error']['code'], 'malformed_function_call')
        self.assertNotIn('SECRET', json.dumps(value))
        self.assertNotIn('HIDDEN', json.dumps(value))
        self.assertEqual((self.dest / 'state.json').read_bytes(), before)
        self.assertFalse((self.dest / 'report.md').exists())
        with self.assertRaisesRegex(ValueError, 'result_not_complete'):
            dorothy.result('failed', self.runtime)

    def test_changed_archive_or_partial_and_foreign_task_are_rejected(self):
        original = self.path.read_bytes()
        self.path.write_bytes(original + b'\n')
        with self.assertRaisesRegex(ValueError, 'native_archive_changed'):
            dorothy.export_visible('failed', self.runtime)
        self.path.write_bytes(original)
        (self.dest / 'partial.md').write_text('冒充的答复')
        with self.assertRaisesRegex(ValueError, 'visible_text_mismatch'):
            dorothy.export_visible('failed', self.runtime)
        self.rows[2]['content'] = '其他任务'
        self.save_archive()
        with self.assertRaisesRegex(ValueError, 'native_task_mismatch'):
            dorothy.export_visible('failed', self.runtime)

    def test_native_wrapper_matches_exact_task_and_later_tool_is_not_a_final_reply(self):
        self.rows[2]['content'] = ('<USER_REQUEST>\n' + self.task +
            '\n</USER_REQUEST>\n<ADDITIONAL_METADATA>\nThe current local time is: '
            '2026-10-15T16:00:00+08:00.\n</ADDITIONAL_METADATA>')
        self.save_archive()
        self.assertEqual(dorothy.export_visible('failed', self.runtime)['verification'], 'native_archive')
        self.rows.append({'step_index': 6, 'type': 'GENERIC', 'status': 'DONE', 'content': '工具'})
        self.save_archive()
        with self.assertRaisesRegex(ValueError, 'visible_reply_unverified'):
            dorothy.export_visible('failed', self.runtime)

    def test_active_execution_cannot_be_exported(self):
        self.value['cleanup']['native_group'] = 'running'
        with self.assertRaisesRegex(ValueError, 'native_execution_not_closed'):
            dorothy.export_visible('failed', self.runtime)


if __name__ == '__main__':
    unittest.main()
