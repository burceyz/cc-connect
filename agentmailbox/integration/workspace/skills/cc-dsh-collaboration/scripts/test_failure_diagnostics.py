"""Failure causes must come from the exact native turn, never error prose."""
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

import bridge

os.environ.setdefault('AGENT_MAILBOX_DISABLE', '1')


class NativeFailureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test config')
        self.task = self.root / 'task.md'
        self.task.write_text('请核对固定材料。')

    def run_failure(self, reason, malformed=False):
        child = self.root / 'fake-dsh'
        child.write_text('#!' + sys.executable + '\n' + '''
import json, os, pathlib, subprocess, sys
task = sys.stdin.read()
sid = 'session-diagnostic'
rows = [
    {'type': 'session', 'id': sid, 'cwd': os.getcwd()},
    {'seq': 0, 'type': 'turn/start', 'data': {'turn': 1}},
    {'seq': 1, 'type': 'user/message', 'data': {
        'source': {'kind': 'user'}, 'content': [{'type': 'text', 'text': task}]}},
    {'seq': 2, 'type': 'request/context', 'data': {'contextWindow': 262144}},
    {'seq': 3, 'type': 'assistant/message', 'data': {'message': {
        'content': [{'type': 'reasoning', 'text': 'HIDDEN_REASONING'}]},
        'usage': {'inputTokens': 247649, 'outputTokens': 1,
        'cacheReadTokens': 238464, 'other': 'SECRET_USAGE'}}},
    {'seq': 4, 'type': 'turn/end', 'data': {'turn': 1, 'reason': __END_REASON__}},
]
source = pathlib.Path(os.environ['DSH_HOME']) / 'sessions/test' / sid / 'session.v4.jsonl.zstd'
source.parent.mkdir(parents=True)
source.write_bytes(subprocess.run(['zstd', '-c'], input='\\n'.join(json.dumps(r) for r in rows).encode(),
                                 stdout=subprocess.PIPE, check=True).stdout)
print(json.dumps({'type': 'session', 'sessionId': sid}))
if __MALFORMED__:
    print('malformed stream line')
    print(json.dumps({'type': 'error', 'message': 'SECRET provider failure'}))
print(json.dumps({'type': 'final', 'text': ''}))
sys.exit(1)
'''.replace('__END_REASON__', repr(reason)).replace('__MALFORMED__', repr(malformed)))
        child.chmod(0o700)
        with redirect_stdout(io.StringIO()):
            return bridge.run(self.task, None, 5, self.runtime, str(child))

    def test_empty_final_is_a_valid_event_without_a_response(self):
        projection = bridge.Projection()
        projection.feed(json.dumps({'type': 'session', 'sessionId': 'session-one'}))
        projection.feed(json.dumps({'type': 'final', 'text': ''}))
        self.assertEqual(projection.status(1), 'empty_response')
        projection.feed(json.dumps({'type': 'final', 'text': 'second'}))
        self.assertEqual(projection.status(0), 'invalid_event')

    def test_native_causes_are_distinct_and_do_not_publish_empty_reports(self):
        cases = [
            ({'kind': 'max-tokens'}, 'max_tokens'),
            ({'kind': 'error', 'error': {'code': 'CONTEXT_WINDOW_EXCEEDED',
              'message': 'SECRET error prose'}}, 'context_window_exceeded'),
            ({'kind': 'error', 'error': {'code': 'SERVER',
              'message': '524 status code (no body) SECRET'}}, 'provider_error'),
        ]
        for reason, expected in cases:
            with self.subTest(expected=expected):
                dest, status = self.run_failure(reason)
                self.assertEqual(status, expected)
                record = json.loads((dest / 'result.json').read_text())
                self.assertEqual(record['stream_status'], 'empty_response')
                self.assertEqual(record['native_outcome']['context_window'], 262144)
                self.assertEqual(record['native_outcome']['usage']['outputTokens'], 1)
                if expected == 'provider_error':
                    self.assertEqual(record['native_outcome']['http_status'], 524)
                self.assertNotIn('SECRET', json.dumps(record))
                self.assertNotIn('HIDDEN', json.dumps(record))
                self.assertFalse((dest / 'response.md').exists())

    def test_native_evidence_does_not_hide_a_malformed_stream(self):
        dest, status = self.run_failure({'kind': 'max-tokens'}, malformed=True)
        self.assertEqual(status, 'invalid_event')
        record = json.loads((dest / 'result.json').read_text())
        self.assertEqual(record['native_outcome']['status'], 'max_tokens')

    def test_old_turn_diagnosis_ignores_later_success_and_preserves_originals(self):
        dest, _ = self.run_failure({'kind': 'max-tokens'})
        record_path = dest / 'result.json'
        record = json.loads(record_path.read_text())
        record['status'] = 'invalid_event'  # A receipt written by the older bridge.
        bridge.write_json(record_path, record)
        before = record_path.read_bytes()
        first = bridge.diagnose_run(dest, self.runtime)
        source, rows = bridge.session_rows(record['session_id'], self.runtime, record['instance_id'])
        rows.extend([
            {'seq': 5, 'type': 'turn/start', 'data': {'turn': 2}},
            {'seq': 6, 'type': 'user/message', 'data': {'source': {'kind': 'user'},
                'content': [{'type': 'text', 'text': self.task.read_text()}]}},
            {'seq': 7, 'type': 'turn/end', 'data': {'turn': 2, 'reason': {'kind': 'completed'}}},
        ])
        source.write_bytes(subprocess.run(['zstd', '-c'],
            input='\n'.join(json.dumps(row) for row in rows).encode(), stdout=subprocess.PIPE,
            check=True).stdout)
        source_before = source.read_bytes()
        second = bridge.diagnose_run(dest, self.runtime)
        self.assertEqual(first, second)
        self.assertEqual(second['recorded_status'], 'invalid_event')
        self.assertEqual(second['native_outcome']['turn'], 1)
        self.assertEqual(record_path.read_bytes(), before)
        self.assertEqual(source.read_bytes(), source_before)

    def test_foreign_identity_task_or_unfinished_turn_is_not_attributed(self):
        dest, _ = self.run_failure({'kind': 'max-tokens'})
        record = json.loads((dest / 'result.json').read_text())
        _, original = bridge.session_rows(record['session_id'], self.runtime, record['instance_id'])
        for mutation, code in (
                (lambda rows: rows[0].update(id='session-other'), 'source_identity_mismatch'),
                (lambda rows: rows[2]['data']['content'][0].update(text='other'), 'task_not_in_source'),
                (lambda rows: rows.pop(), 'native_turn_incomplete')):
            rows = json.loads(json.dumps(original))
            mutation(rows)
            with self.assertRaisesRegex(ValueError, code):
                bridge.native_run_diagnostic(record, rows, self.task.read_bytes())

    def test_invalid_native_records_fail_without_exposing_their_contents(self):
        dest, _ = self.run_failure({'kind': 'error', 'error': {'code': 'SERVER'}})
        record = json.loads((dest / 'result.json').read_text())
        _, original = bridge.session_rows(record['session_id'], self.runtime, record['instance_id'])
        mutations = [
            lambda rows: rows.__setitem__(0, []),
            lambda rows: rows[0].update(cwd=None),
            lambda rows: rows.__setitem__(1, []),
            lambda rows: rows[1].update(data=None),
            lambda rows: rows[2].update(seq=0),
            lambda rows: rows[2].update(seq=2),
            lambda rows: rows[2].update(seq=True),
            lambda rows: rows[2]['data'].update(source=None),
            lambda rows: rows[-1]['data'].update(reason=[]),
            lambda rows: rows[-1]['data']['reason'].update(kind=[]),
            lambda rows: rows[-1]['data']['reason'].update(error=None),
            lambda rows: rows[-1]['data']['reason']['error'].update(code=[]),
            lambda rows: rows[4]['data'].update(usage=None),
            lambda rows: rows[4]['data']['message'].update(content=[None]),
        ]
        for mutation in mutations:
            rows = json.loads(json.dumps(original))
            mutation(rows)
            with self.assertRaises(ValueError):
                bridge.native_run_diagnostic(record, rows, self.task.read_bytes())

    def test_resumed_turn_inherits_capacity_but_has_its_own_usage(self):
        dest, _ = self.run_failure({'kind': 'max-tokens'})
        record = json.loads((dest / 'result.json').read_text())
        _, rows = bridge.session_rows(record['session_id'], self.runtime, record['instance_id'])
        record['source_before_seq'] = 4
        rows.extend([
            {'seq': 5, 'type': 'turn/start', 'data': {'turn': 2}},
            {'seq': 6, 'type': 'user/message', 'data': {'source': {'kind': 'user'},
                'content': [{'type': 'text', 'text': self.task.read_text()}]}},
            {'seq': 7, 'type': 'assistant/message', 'data': {
                'message': {'content': [{'type': 'text', 'text': '答复'}]},
                'usage': {'inputTokens': 500, 'outputTokens': 2}}},
            {'seq': 8, 'type': 'turn/end', 'data': {'turn': 2, 'reason': {'kind': 'max-tokens'}}},
        ])
        details = bridge.native_run_diagnostic(record, rows, self.task.read_bytes())
        self.assertEqual(details['turn'], 2)
        self.assertEqual(details['context_window'], 262144)
        self.assertEqual(details['usage'], {'inputTokens': 500, 'outputTokens': 2})
        self.assertEqual(details['visible_text_bytes'], len('答复'.encode()))
        rows[5]['seq'] = 3  # The saved cursor no longer names an event in this log.
        with self.assertRaisesRegex(ValueError, 'native_sequence_invalid'):
            bridge.native_run_diagnostic(record, rows, self.task.read_bytes())


if __name__ == '__main__':
    unittest.main()
