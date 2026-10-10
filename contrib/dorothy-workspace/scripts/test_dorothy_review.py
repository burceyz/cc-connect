import copy
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

import bridge
import dorothy_review as review
import jobs


class DorothyReviewTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.temp.name).resolve()
        (self.workspace / 'AGENTS.md').write_text('本工作区：Dorothy 为桃乐丝，Hermes 负责联合复核。\n')
        (self.workspace / 'agent').mkdir()
        (self.workspace / 'agent/dorothy-collaboration.md').write_text('固定模型与 High，每项任务实际交叉复核。\n')
        self.root = self.workspace / 'reviews'
        self.runtime = self.workspace / 'runtime'
        self.runtime.mkdir()
        (self.runtime / 'runs').mkdir()
        (self.runtime / 'jobs').mkdir()
        self.gate = review.Gate(self.root, self.runtime)
        self.native_rows = {}
        self.origin = {'project': 'dorothy', 'session_key': 'chat:one', 'cwd': str(self.workspace)}
        self.binding = {'protocol': 'cc-connect-session-ipc-v1', 'socket': str(self.workspace / 'ipc.sock'),
                        'target': self.origin | {'session_id': 'cc-one', 'agent_session_id': 'agy-one'}}
        self.patches = [patch.object(bridge, 'WORKSPACE', self.workspace),
                        patch.object(review.ipc, 'capture', return_value=self.binding),
                        patch.object(jobs, 'pid_probe', return_value='absent'),
                        patch.object(jobs, 'lock_probe', return_value='released'),
                        patch.object(bridge, 'session_rows', side_effect=self.session_rows),
                        patch.object(review, 'native_child_rows', side_effect=self.session_rows),
                        patch.object(jobs, 'submit', side_effect=self.fake_submit)]
        for item in self.patches:
            item.start()
        self.submissions = []
        self.begin_payload = {'protocol': review.PROTOCOL, 'op': 'begin', 'task_id': 'task-one',
                              'origin': self.origin, 'request_text': '解释 2 + 2。',
                              'request_sha256': review.sha('解释 2 + 2。'.encode()), 'continuation': False}
        result = self.gate.handle(self.begin_payload)
        self.assertEqual(result['status'], 'pending_review')
        self.dest = Path(result['task_dir'])
        self.manifest = {'schema': review.MANIFEST_SCHEMA, 'task_id': 'task-one', 'mode': 'answer_only',
                         'acceptance': ['算式正确且答复中文'], 'independent_judgment': '2 + 2 = 4。',
                         'artifacts': [], 'test_evidence': []}
        self.write_manifest()
        self.candidate = '2 + 2 = 4。\n'

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp.cleanup()

    def write_manifest(self):
        (self.dest / 'manifest.json').write_bytes(review.json_bytes(self.manifest))

    def check(self, **changes):
        payload = {'protocol': review.PROTOCOL, 'op': 'check', 'task_id': self.manifest['task_id'],
                   'origin': self.origin, 'conversation_id': 'agy-one', 'candidate_text': self.candidate,
                   'candidate_sha256': review.sha(self.candidate.encode()),
                   'manifest_path': str(self.dest / 'manifest.json')} | changes
        return self.gate.handle(payload)

    def bind(self, **changes):
        return self.gate.handle({'protocol': review.PROTOCOL, 'op': 'bind', 'task_id': self.manifest['task_id'],
                                 'origin': self.origin, 'conversation_id': 'agy-one'} | changes)

    def refresh(self, **changes):
        return self.bind(op='refresh', **changes)

    def current(self):
        task = review.read_json(self.dest / 'task.json')
        directory = self.dest / 'revisions' / task['active_revision']
        return directory, review.read_json(directory / 'revision.json')

    def good_report(self, revision):
        return {key: revision[key] for key in ('task_id', 'revision', 'candidate_sha256', 'manifest_sha256')} | {
            'schema': review.REVIEW_SCHEMA, 'verdict': 'pass', 'scope': '核对原题及候选算式',
            'independent_initial': '主审独立确认四个单位的计数。',
            'reviewers': [{'agent_id': 'sub-a', 'perspective': '需求', 'status': 'completed',
                           'independent_initial': '候选满足原题。', 'checked_materials': ['request.txt'], 'findings': []},
                          {'agent_id': 'sub-b', 'perspective': '反例', 'status': 'completed',
                           'independent_initial': '整数加法可复核。', 'checked_materials': ['candidate.txt'], 'findings': []}],
            'findings': [], 'followup': [], 'limitations': ['仅验证所提供文本，没有运行 Shell。']}

    def fake_submit(self, task_path, key, **kwargs):
        self.submissions.append((key, kwargs))
        revision = review.read_json(task_path.parent / 'revision.json')
        job = self.runtime / 'jobs' / key
        run = self.runtime / 'runs' / key
        job.mkdir()
        run.mkdir()
        raw_task = task_path.read_bytes()
        raw_response = review.json_bytes(self.good_report(revision))
        common = {'profile': 'opencode-go', 'reasoning_effort': 'high', 'cwd': kwargs['cwd'],
                  'config_sha256': 'a' * 64}
        request = common | {'task_sha256': review.sha(raw_task), 'delivery': 'conversation',
                            'callback_target': kwargs['callback_target']}
        receipt = common | {'job_id': key, 'status': 'completed', 'run_dir': str(run),
                            'worker_pid': 99111, 'delivery': 'conversation',
                            'callback': kwargs['callback_target']['protocol'],
                            'callback_target': kwargs['callback_target']}
        session_id = 'session-' + key
        record = common | {'status': 'completed', 'returncode': 0, 'child_pid': 99112, 'source_before_seq': -1,
                           'session_id': session_id, 'task_sha256': review.sha(raw_task),
                           'response_sha256': review.sha(raw_response)}
        for directory in (job, run):
            (directory / 'task.md').write_bytes(raw_task)
        (run / 'response.md').write_bytes(raw_response)
        for path, value in ((job / 'request.json', request), (job / 'receipt.json', receipt), (run / 'result.json', record)):
            path.write_bytes(review.json_bytes(value))
        config = {'provider': 'opencode-go', 'model': 'deepseek-v4.1-flash', 'reasoningEffort': 'high'}
        rows = [{'type': 'session', 'id': session_id, 'cwd': kwargs['cwd']},
                {'type': 'user/message', 'seq': 0, 'data': {'source': {'kind': 'user'},
                 'content': [{'type': 'text', 'text': raw_task.decode()}]}},
                {'type': 'request/header', 'seq': 1, 'data': {'header': {'config': copy.deepcopy(config)}}}]
        for index, label in enumerate(('sub-a', 'sub-b')):
            child_id = str(uuid.uuid5(uuid.NAMESPACE_URL, session_id + ':' + label))
            call_id = 'call-' + label
            prompt = '只读审查 ' + kwargs['cwd']
            seq = 2 + index * 3
            rows.extend([
                {'type': 'tool/call', 'seq': seq, 'data': {'name': 'subagent', 'callId': call_id,
                 'arguments': json.dumps({'description': label, 'prompt': prompt, 'run_in_background': False})}},
                {'type': 'subagent/catalog', 'seq': seq + 1,
                 'data': {'childId': child_id, 'mode': 'one-shot', 'label': label}},
                {'type': 'tool/result', 'seq': seq + 2, 'data': {'message': {'toolCallId': call_id,
                 'isError': False, 'content': [{'type': 'text', 'text': '子审证据正文。'}]}}}
            ])
            self.native_rows[child_id] = [
                {'type': 'session', 'id': child_id, 'parentSession': session_id, 'cwd': kwargs['cwd']},
                {'type': 'request/header', 'seq': 0, 'data': {'header': {'config': copy.deepcopy(config)}}},
                {'type': 'assistant/message', 'seq': 1, 'data': {'message': {
                 'content': [{'type': 'text', 'text': '子审证据正文。'}]}}}
            ]
        rows.append({'type': 'assistant/message', 'seq': 8, 'data': {'message': {
                     'content': [{'type': 'text', 'text': raw_response.decode()}]}}})
        self.native_rows[session_id] = rows
        return receipt

    def session_rows(self, session_id, runtime, instance_id=None):
        source = self.runtime / (session_id + '.jsonl')
        rows = self.native_rows[session_id]
        source.write_text('\n'.join(json.dumps(row) for row in rows))
        return source, copy.deepcopy(rows)

    def change_report(self, update):
        directory, revision = self.current()
        run = self.runtime / 'runs' / revision['job_id']
        report = json.loads((run / 'response.md').read_bytes())
        update(report)
        data = review.json_bytes(report)
        (run / 'response.md').write_bytes(data)
        record = review.read_json(run / 'result.json')
        record['response_sha256'] = review.sha(data)
        (run / 'result.json').write_bytes(review.json_bytes(record))
        self.native_rows[record['session_id']][-1]['data']['message']['content'][0]['text'] = data.decode()

    def respond(self):
        directory, revision = self.current()
        report = json.loads((directory / 'hermes-response.txt').read_bytes())
        response = {key: revision[key] for key in ('task_id', 'revision', 'candidate_sha256',
                                                  'manifest_sha256', 'hermes_response_sha256')}
        response.update(schema=review.RESPONSE_SCHEMA, review_read=True,
                        assessment='已读取两路审查并依据原式独立核对，结论有相应证据。',
                        responses=[{'finding_id': f.get('id') or f['finding_id'], 'resolution': 'verified',
                                    'assessment': '已依据所列材料核验。', 'evidence': ['candidate.txt']}
                                   for f in report['findings'] + report['followup']], limitations=[])
        (self.dest / 'response.json').write_bytes(review.json_bytes(response))

    def initial_review(self):
        self.assertEqual(self.check()['reasons'], ['hermes_review_pending'])
        return self.check()

    def test_valid_no_findings_requires_actual_response_and_releases_exact_candidate(self):
        pending = self.initial_review()
        self.assertEqual(pending['reasons'], ['dorothy_response_missing'])
        self.assertFalse(pending['allow_publish'])
        self.respond()
        result = self.check()
        self.assertTrue(result['allow_publish'])
        self.assertEqual(result['status'], 'verified')
        self.assertEqual(result['candidate_sha256'], review.sha(self.candidate.encode()))
        self.assertEqual(len(result['evidence_refs']), 5)
        key, kwargs = self.submissions[0]
        self.assertEqual((kwargs['profile'], kwargs['reasoning_effort'], kwargs['delivery']),
                         ('opencode-go', 'high', 'conversation'))
        self.assertEqual(kwargs['callback_target'], self.binding)

    def test_duplicate_checks_never_submit_twice(self):
        for _ in range(4):
            self.assertFalse(self.check()['allow_publish'])
        self.assertEqual(len(self.submissions), 1)

    def test_callback_reconciles_late_receipt_before_reading_review(self):
        self.check()
        directory, revision = self.current()
        revision['submit_state'] = 'unknown'
        (directory / 'revision.json').write_bytes(review.json_bytes(revision))
        callback = self.begin_payload | {'task_id': 'callback-proposal', 'continuation': True,
                                         'event_id': revision['event_id'],
                                         'conversation_id': 'agy-one',
                                         'request_text': 'Hermes 已结束。'}
        result = self.gate.handle(callback)
        self.assertEqual(result['task_id'], 'task-one')
        self.assertEqual(self.current()[1]['submit_state'], 'unknown')
        self.assertFalse((directory / 'hermes-response.txt').exists())
        result = self.refresh()
        self.assertEqual(self.current()[1]['submit_state'], 'submitted')
        self.assertTrue((directory / 'hermes-response.txt').exists())
        self.assertNotIn('submission_unconfirmed', result['instructions'])
        self.assertEqual(len(self.submissions), 1)

    def test_changed_candidate_reconciles_late_receipt_without_deadlock(self):
        self.check()
        directory, revision = self.current()
        revision['submit_state'] = 'unknown'
        (directory / 'revision.json').write_bytes(review.json_bytes(revision))
        self.candidate = '四个单位相加得到 4。\n'
        result = self.check()
        self.assertEqual(result['reasons'], ['hermes_review_pending'])
        self.assertNotEqual(self.current()[1]['revision'], revision['revision'])
        self.assertEqual(len(self.submissions), 2)

    def test_finalize_publishes_frozen_candidate_after_explicit_author_response(self):
        self.initial_review()
        self.respond()
        response_path = self.dest / 'response.json'
        response = review.read_json(response_path) | {'preserve_candidate': True}
        response_path.write_bytes(review.json_bytes(response))
        directory, revision = self.current()
        result = self.gate.handle({'protocol': review.PROTOCOL, 'op': 'finalize',
                                  'task_id': 'task-one', 'origin': self.origin,
                                  'conversation_id': 'agy-one', 'revision': revision['revision']})
        self.assertTrue(result['allow_publish'])
        self.assertEqual(result['candidate_path'], str(directory / 'materials/candidate.txt'))
        self.assertEqual(Path(result['candidate_path']).read_text(), self.candidate)
        self.assertEqual(len(self.submissions), 1)

    def test_finalize_cannot_assume_author_accepted_candidate(self):
        self.initial_review()
        self.respond()
        _, revision = self.current()
        payload = {'protocol': review.PROTOCOL, 'op': 'finalize', 'task_id': 'task-one',
                   'origin': self.origin, 'conversation_id': 'agy-one', 'revision': revision['revision']}
        self.assertEqual(self.gate.handle(payload)['reasons'], ['candidate_decision_missing'])
        self.assertFalse(self.gate.handle(payload | {'revision': '0' * 64})['allow_publish'])

    def test_finalize_distinguishes_missing_response_and_explicit_revision(self):
        self.initial_review()
        _, revision = self.current()
        payload = {'protocol': review.PROTOCOL, 'op': 'finalize', 'task_id': 'task-one',
                   'origin': self.origin, 'conversation_id': 'agy-one', 'revision': revision['revision']}
        result = self.gate.handle(payload)
        self.assertEqual(result['reasons'], ['dorothy_response_missing'])
        self.assertIn('response.json', result['instructions'])
        self.respond()
        response_path = self.dest / 'response.json'
        response_path.write_bytes(review.json_bytes(review.read_json(response_path) | {'preserve_candidate': False}))
        result = self.gate.handle(payload)
        self.assertEqual(result['reasons'], ['candidate_revision_required'])
        self.assertFalse(result['allow_publish'])
        self.assertEqual(self.check()['reasons'], ['candidate_revision_required'])
        self.assertEqual(len(self.submissions), 1)

    def test_finalize_requires_valid_current_response_and_manifest(self):
        self.initial_review()
        _, revision = self.current()
        payload = {'protocol': review.PROTOCOL, 'op': 'finalize', 'task_id': 'task-one',
                   'origin': self.origin, 'conversation_id': 'agy-one', 'revision': revision['revision']}
        response_path = self.dest / 'response.json'
        response_path.write_text('{unfinished')
        self.assertEqual(self.gate.handle(payload)['reasons'], ['dorothy_response_invalid_json'])
        self.respond()
        response_path.write_bytes(review.json_bytes(review.read_json(response_path) | {'preserve_candidate': True}))
        manifest_path = self.dest / 'manifest.json'
        manifest_path.unlink()
        self.assertEqual(self.gate.handle(payload)['reasons'], ['manifest_missing'])
        self.write_manifest()
        manifest_path.write_bytes(manifest_path.read_bytes() + b'\n')
        self.assertEqual(self.gate.handle(payload)['reasons'], ['candidate_materials_changed'])

    def test_finalize_does_not_discard_a_newer_unreviewed_draft(self):
        self.check()
        _, revision = self.current()
        newer = '更清晰的新答案：四。\n'
        with patch.object(jobs, 'status', return_value={'status': 'running'}):
            self.assertEqual(self.check(candidate_text=newer, candidate_sha256=review.sha(newer.encode()))['reasons'],
                             ['previous_hermes_review_pending'])
        self.refresh()
        self.respond()
        response_path = self.dest / 'response.json'
        response_path.write_bytes(review.json_bytes(review.read_json(response_path) | {'preserve_candidate': True}))
        result = self.gate.handle({'protocol': review.PROTOCOL, 'op': 'finalize', 'task_id': 'task-one',
                                  'origin': self.origin, 'conversation_id': 'agy-one', 'revision': revision['revision']})
        self.assertEqual(result['reasons'], ['candidate_revision_required'])
        self.assertFalse(result['allow_publish'])
        self.assertEqual(len(self.submissions), 1)

    def test_finalize_rechecks_native_hermes_participation(self):
        self.initial_review()
        self.respond()
        response_path = self.dest / 'response.json'
        response_path.write_bytes(review.json_bytes(review.read_json(response_path) | {'preserve_candidate': True}))
        _, revision = self.current()
        record = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[record['session_id']]
        self.native_rows[record['session_id']] = [row for row in rows if row.get('type') != 'tool/result']
        result = self.gate.handle({'protocol': review.PROTOCOL, 'op': 'finalize', 'task_id': 'task-one',
                                  'origin': self.origin, 'conversation_id': 'agy-one', 'revision': revision['revision']})
        self.assertEqual(result['reasons'], ['native_subagent_result_missing'])
        self.assertFalse(result['allow_publish'])

    def test_answer_only_can_include_frozen_source_references(self):
        source = self.workspace / 'rule.txt'
        source.write_text('原始依据。')
        self.manifest['references'] = [{'path': str(source)}]
        self.write_manifest()
        self.initial_review()
        directory, revision = self.current()
        self.assertEqual(revision['files'][0]['source'], str(source))
        self.assertEqual((directory / 'materials' / revision['files'][0]['snapshot']).read_text(), '原始依据。')
        self.assertTrue((directory / 'materials/host-evidence.json').is_file())
        source.write_text('修改后的依据。')
        self.assertEqual(self.check()['reasons'], ['source_changed_after_review'])

    def test_references_cannot_escape_workspace(self):
        self.manifest['references'] = [{'path': '/etc/passwd'}]
        self.write_manifest()
        self.assertEqual(self.check()['reasons'], ['manifest_source_outside_scope'])
        self.assertFalse(self.submissions)

    def test_host_runtime_and_authorized_shared_rules_are_frozen_as_limited_evidence(self):
        evidence = {'configured_model': 'gemini-3.8-flash-high', 'effort': 'high', 'mode': 'acceptEdits'}
        self.assertEqual(self.bind(runtime_evidence=evidence)['status'], 'pending_review')
        self.assertEqual(self.bind(runtime_evidence={'observed_model': 'gemini-3.8-flash-high'})['status'], 'pending_review')
        self.initial_review()
        directory, revision = self.current()
        material = directory / 'materials'
        host = review.read_json(material / 'host-evidence.json')
        self.assertEqual(host['runtime_evidence'], evidence | {'observed_model': 'gemini-3.8-flash-high'})
        self.assertIn('不证明性能', host['runtime_evidence_scope'])
        self.assertEqual((material / 'workspace-AGENTS.md').read_bytes(), (self.workspace / 'AGENTS.md').read_bytes())
        self.assertEqual((material / 'dorothy-collaboration.md').read_bytes(),
                         (self.workspace / 'agent/dorothy-collaboration.md').read_bytes())
        self.assertEqual({item['snapshot'] for item in host['shared_rules']},
                         {'workspace-AGENTS.md', 'dorothy-collaboration.md'})
        self.assertTrue(all(item in revision['files'] for item in host['shared_rules']))
        self.assertIn('workspace-AGENTS.md', (directory / 'review-task.md').read_text())
        (self.workspace / 'AGENTS.md').write_text('changed rule')
        self.assertEqual(self.check()['reasons'], ['source_changed_after_review'])

    def test_runtime_evidence_rejects_secrets_fields_and_invalid_values(self):
        for evidence in ({'api_key': 'should-not-be-read'}, {'model': 'wrong field'}, [],
                         {'mode': ''}, {'effort': '   '}, {'configured_model': 'm' * 129},
                         {'observed_model': 'model\nsecret'}, {'effort': 1}):
            with self.subTest(evidence=evidence):
                self.assertEqual(self.bind(runtime_evidence=evidence)['reasons'], ['runtime_evidence_invalid'])
        self.assertNotIn('runtime_evidence', review.read_json(self.dest / 'task.json'))
        self.assertFalse(self.submissions)

    def test_bound_runtime_evidence_cannot_silently_change(self):
        self.bind(runtime_evidence={'configured_model': 'gemini-3.8-flash-high'})
        result = self.bind(runtime_evidence={'configured_model': 'another-model'})
        self.assertEqual(result['reasons'], ['runtime_evidence_changed'])
        self.assertEqual(review.read_json(self.dest / 'task.json')['runtime_evidence'],
                         {'configured_model': 'gemini-3.8-flash-high'})

    def test_shared_rules_cannot_be_symlinked_out_of_scope(self):
        rule = self.workspace / 'AGENTS.md'
        rule.unlink()
        rule.symlink_to('/etc/passwd')
        self.assertEqual(self.check()['reasons'], ['shared_rule_source_outside_scope'])
        self.assertFalse(self.submissions)

    def test_refresh_preserves_real_review_even_if_source_has_since_changed(self):
        self.check()
        directory, _ = self.current()
        (self.workspace / 'AGENTS.md').write_text('newer rule')
        result = self.refresh()
        self.assertEqual(result['reasons'], ['source_changed_after_review'])
        self.assertEqual(result['status'], 'pending_review')
        self.assertTrue((directory / 'hermes-response.txt').exists())
        self.assertFalse(result['allow_publish'])

    def test_explicit_continue_restores_pinned_task_across_gate_instances(self):
        self.initial_review()
        fresh = review.Gate(self.root, self.runtime)
        payload = self.begin_payload | {'task_id': 'new-proposal', 'resume': True,
                                        'conversation_id': 'agy-one', 'request_text': '继续',
                                        'request_sha256': review.sha('继续'.encode())}
        result = fresh.handle(payload)
        self.assertEqual(result['task_id'], 'task-one')
        self.assertEqual(result['revision'], self.current()[1]['revision'])
        wrong = fresh.handle(payload | {'conversation_id': 'another-conversation'})
        self.assertEqual(wrong['reasons'], ['no_pending_review'])

    def test_bind_before_final_candidate_restores_exact_pending_identity(self):
        (self.dest / 'manifest.json').unlink()
        result = self.bind()
        self.assertEqual(result['status'], 'pending_review')
        self.assertFalse(result['allow_publish'])
        task = review.read_json(self.dest / 'task.json')
        self.assertEqual(task['binding'], self.binding)
        self.assertIsNone(task['active_revision'])
        resumed = review.Gate(self.root, self.runtime).handle(self.begin_payload | {
            'task_id': 'resume-proposal', 'resume': True, 'conversation_id': 'agy-one'})
        self.assertEqual(resumed['task_id'], 'task-one')
        self.assertEqual(resumed['manifest_path'], str(self.dest / 'manifest.json'))
        self.assertFalse(self.submissions)

    def test_resume_index_is_verified_only_after_complete_joint_review(self):
        self.initial_review()
        index = self.gate.conversation_path(self.origin, 'agy-one')
        self.assertEqual(review.read_json(index)['status'], 'pending_review')
        self.respond()
        response_path = self.dest / 'response.json'
        response = review.read_json(response_path)
        response_path.write_bytes(review.json_bytes(response | {'review_read': False}))
        self.assertEqual(self.check()['reasons'], ['dorothy_review_not_read'])
        self.assertEqual(review.read_json(index)['status'], 'pending_review')
        response_path.write_bytes(review.json_bytes(response | {'preserve_candidate': True}))
        self.assertTrue(self.check()['allow_publish'])
        self.assertEqual(review.read_json(index)['status'], 'verified')
        self.bind()
        self.assertEqual(review.read_json(index)['status'], 'verified')
        result = self.gate.handle(self.begin_payload | {'task_id': 'resume-proposal', 'resume': True,
                                                       'conversation_id': 'agy-one'})
        self.assertEqual(result['reasons'], ['no_pending_review'])
        self.candidate = '四。\n'
        self.assertEqual(self.check()['reasons'], ['hermes_review_pending'])
        self.assertEqual(review.read_json(index)['status'], 'pending_review')

    def test_old_success_does_not_complete_or_replace_new_user_task_index(self):
        self.initial_review()
        self.respond()
        newest = 'task-new-pending'
        self.gate.handle(self.begin_payload | {'task_id': newest, 'conversation_id': 'agy-one'})
        self.bind(task_id=newest)
        self.assertTrue(self.check()['allow_publish'])
        index = self.gate.conversation_path(self.origin, 'agy-one')
        self.assertEqual(review.read_json(index), {'task_id': newest, 'status': 'pending_review'})
        resumed = self.gate.handle(self.begin_payload | {'task_id': 'resume-proposal', 'resume': True,
                                                        'conversation_id': 'agy-one'})
        self.assertEqual(resumed['task_id'], newest)

    def test_missing_candidate_and_manifest_checks_still_pin_real_origin(self):
        (self.dest / 'manifest.json').unlink()
        self.assertEqual(self.check(candidate_text='')['reasons'], ['candidate_missing'])
        self.assertEqual(self.check()['reasons'], ['manifest_missing'])
        task = review.read_json(self.dest / 'task.json')
        self.assertEqual(task['binding'], self.binding)
        index = self.gate.conversation_path(self.origin, 'agy-one')
        self.assertEqual(review.read_json(index)['task_id'], 'task-one')
        self.assertFalse(self.submissions)

    def test_existing_binding_rebuilds_only_its_missing_index(self):
        self.bind()
        index = self.gate.conversation_path(self.origin, 'agy-one')
        index.unlink()
        task_path = self.dest / 'task.json'
        legacy = review.read_json(task_path)
        legacy.pop('creation_order')
        task_path.write_bytes(review.json_bytes(legacy))
        self.assertEqual(self.bind()['status'], 'pending_review')
        self.assertEqual(review.read_json(index)['task_id'], 'task-one')
        wrong = self.bind(conversation_id='another-conversation')
        self.assertEqual(wrong['reasons'], ['captured_conversation_mismatch'])
        self.assertFalse(self.gate.conversation_path(self.origin, 'another-conversation').exists())

    def test_old_callback_and_delayed_first_bind_cannot_replace_new_task_index(self):
        self.initial_review()
        _, revision = self.current()
        for old_state in ('bound', 'unbound', 'legacy'):
            with self.subTest(old_state=old_state):
                newest = 'task-new-' + old_state
                self.gate.handle(self.begin_payload | {'task_id': newest, 'conversation_id': 'agy-one'})
                self.assertEqual(self.bind(task_id=newest)['status'], 'pending_review')
                task_path = self.dest / 'task.json'
                old = review.read_json(task_path)
                if old_state == 'unbound':
                    old['binding'] = None
                if old_state == 'legacy':
                    old.pop('creation_order')
                task_path.write_bytes(review.json_bytes(old))
                restored = self.gate.handle(self.begin_payload | {'task_id': 'callback-proposal',
                    'continuation': True, 'event_id': revision['event_id'], 'conversation_id': 'agy-one'})
                self.assertEqual(restored['task_id'], 'task-one')
                self.assertEqual(self.refresh()['status'], 'pending_review')
                resumed = self.gate.handle(self.begin_payload | {'task_id': 'resume-proposal',
                    'resume': True, 'conversation_id': 'agy-one'})
                self.assertEqual(resumed['task_id'], newest)

    def test_begin_reads_only_local_state_and_refresh_returns_actual_review(self):
        self.check()
        directory, revision = self.current()
        with patch.object(self.gate, 'collect_review', side_effect=AssertionError('must not decode during begin')) as collect:
            result = self.gate.handle(self.begin_payload | {'task_id': 'callback-proposal',
                'continuation': True, 'event_id': revision['event_id'], 'conversation_id': 'agy-one'})
        collect.assert_not_called()
        self.assertEqual(result['revision'], revision['revision'])
        self.assertEqual(result['candidate_sha256'], revision['candidate_sha256'])
        self.assertFalse((directory / 'hermes-response.txt').exists())
        result = self.refresh()
        self.assertEqual(result['status'], 'pending_review')
        self.assertEqual(result['reasons'], ['hermes_review_ready'])
        self.assertFalse(result['allow_publish'])
        self.assertIn(str(directory / 'hermes-response.txt'), result['instructions'])
        self.assertTrue((directory / 'native-evidence.json').exists())

    def test_refresh_changes_required_keeps_model_repair_available(self):
        self.check()
        self.change_report(lambda report: report.update(verdict='changes_required'))
        result = self.refresh()
        self.assertEqual(result['status'], 'pending_review')
        self.assertEqual(result['reasons'], ['hermes_changes_or_unresolved'])
        self.assertIn('preserve_candidate=false', result['instructions'])
        self.assertFalse(result['allow_publish'])
        self.assertEqual(self.check()['status'], 'blocked')
        self.assertEqual(review.read_json(self.gate.conversation_path(self.origin, 'agy-one'))['status'], 'pending_review')

    def test_refresh_does_not_hold_other_tasks_or_global_identity_lock(self):
        self.check()
        original = self.gate.collect_review
        observed = []
        def collect(*args):
            other = review.Gate(self.root, self.runtime)
            observed.append(other.handle(self.begin_payload | {'task_id': 'task-two'}))
            observed.append(other.handle({'protocol': review.PROTOCOL, 'op': 'bind', 'task_id': 'task-two',
                                          'origin': self.origin, 'conversation_id': 'agy-one'}))
            return original(*args)
        with patch.object(self.gate, 'collect_review', side_effect=collect):
            self.assertEqual(self.refresh()['reasons'], ['hermes_review_ready'])
        self.assertTrue(all(item['reasons'] == [] for item in observed))

    def test_refresh_timeout_releases_task_lock_and_keeps_report_pending(self):
        self.check()
        with patch.object(review, 'REFRESH_TIMEOUT', 0.01), patch.object(
                self.gate, 'collect_review', side_effect=lambda *args: time.sleep(1)):
            result = self.refresh()
        self.assertEqual(result['reasons'], ['review_refresh_timeout'])
        self.assertEqual(result['status'], 'pending_review')
        self.assertFalse(result['allow_publish'])
        self.assertEqual(self.refresh()['reasons'], ['hermes_review_ready'])
        self.assertEqual(len(self.submissions), 1)

    def test_failed_launch_can_be_replaced_without_discarding_old_receipt(self):
        self.check()
        directory, revision = self.current()
        job_dir = self.runtime / 'jobs' / revision['job_id']
        run_dir = self.runtime / 'runs' / revision['job_id']
        receipt = review.read_json(job_dir / 'receipt.json') | {'status': 'process_failed'}
        record = review.read_json(run_dir / 'result.json') | {
            'status': 'process_failed', 'returncode': 127, 'session_id': None}
        (job_dir / 'receipt.json').write_bytes(review.json_bytes(receipt))
        (run_dir / 'result.json').write_bytes(review.json_bytes(record))
        (run_dir / 'response.md').unlink()
        self.assertEqual(self.check()['reasons'], ['hermes_launch_failed'])
        self.manifest['retry_review'] = {'job_id': revision['job_id'], 'reason': '启动环境已修复'}
        self.write_manifest()
        result = self.check()
        self.assertEqual(result['reasons'], ['hermes_review_pending'])
        new_dir, new_revision = self.current()
        self.assertNotEqual(new_revision['revision'], revision['revision'])
        self.assertTrue((new_dir / 'materials/previous-review-failure.json').is_file())
        self.assertEqual(review.read_json(job_dir / 'receipt.json')['status'], 'process_failed')
        self.assertEqual(len(self.submissions), 2)

    def test_native_review_does_not_hold_global_begin_lock(self):
        self.initial_review()
        original = self.gate.collect_review
        observed = []
        def collect(*args):
            other = review.Gate(self.root, self.runtime)
            observed.append(other.handle(self.begin_payload | {'task_id': 'task-two'}))
            return original(*args)
        with patch.object(self.gate, 'collect_review', side_effect=collect):
            self.check()
        self.assertTrue(observed)
        self.assertTrue(all(item.get('task_dir') for item in observed))
        self.assertTrue(all(item['reasons'] == [] for item in observed))

    def test_manifest_is_explicit_and_missing_manifest_does_not_dispatch(self):
        (self.dest / 'manifest.json').unlink()
        self.assertEqual(self.check()['reasons'], ['manifest_missing'])
        self.assertEqual((self.dest / 'drafts' / (review.sha(self.candidate.encode()) + '.txt')).read_text(), self.candidate)
        self.assertEqual(self.submissions, [])
        self.manifest.pop('mode')
        self.write_manifest()
        self.assertEqual(self.check()['reasons'], ['explicit_manifest_mode_required'])

    def test_utf8_bytes_and_newline_are_not_normalized(self):
        result = self.check(candidate_sha256=review.sha(self.candidate.strip().encode()))
        self.assertEqual(result['reasons'], ['candidate_hash_mismatch'])
        self.assertFalse(result['allow_publish'])
        self.assertEqual(self.submissions, [])

    def test_wrong_task_and_origin_never_use_current_task(self):
        self.assertFalse(self.check(task_id='other-task')['allow_publish'])
        self.assertEqual(self.check(origin=self.origin | {'session_key': 'elsewhere'})['reasons'], ['task_origin_mismatch'])
        self.assertEqual(self.submissions, [])

    def test_capture_identity_is_pinned_and_rechecked(self):
        self.initial_review()
        self.respond()
        foreign = copy.deepcopy(self.binding)
        foreign['target']['session_id'] = 'new-cc-session'
        with patch.object(review.ipc, 'capture', return_value=foreign):
            self.assertEqual(self.check()['reasons'], ['captured_binding_changed'])

    def test_new_user_request_is_distinct_but_same_begin_is_idempotent(self):
        again = self.gate.handle(self.begin_payload)
        self.assertEqual(again['task_dir'], str(self.dest))
        other = self.gate.handle(self.begin_payload | {'task_id': 'task-two'})
        self.assertNotEqual(other['task_dir'], str(self.dest))
        self.assertFalse(other['allow_publish'])

    def test_replayed_begin_must_hash_its_actual_request(self):
        result = self.gate.handle(self.begin_payload | {'request_text': 'another request'})
        self.assertEqual(result['reasons'], ['request_hash_mismatch'])

    def test_original_request_cannot_change_after_begin(self):
        path = self.dest / 'request.txt'
        path.chmod(0o600)
        path.write_text('another request')
        self.assertEqual(self.check()['reasons'], ['original_request_modified'])

    def test_unknown_ipc_is_registered_once_and_known_callback_restores_original(self):
        incoming = self.begin_payload | {'task_id': 'cooperation-one', 'continuation': True, 'event_id': 'c' * 64}
        first = self.gate.handle(incoming)
        second = self.gate.handle(incoming | {'task_id': 'another-random-uuid'})
        self.assertEqual(first['task_id'], second['task_id'])
        self.initial_review()
        _, revision = self.current()
        resumed = self.gate.handle(incoming | {'task_id': 'newest-task', 'event_id': revision['event_id']})
        self.assertEqual(resumed['task_id'], 'task-one')
        self.assertIn('preserve_candidate=true', resumed['instructions'])

    def test_deterministic_callback_matches_existing_ipc_algorithm(self):
        self.check()
        _, revision = self.current()
        job = self.runtime / 'jobs' / revision['job_id']
        payload = review.ipc.build_input(job, review.read_json(job / 'request.json'), review.read_json(job / 'receipt.json'))
        self.assertEqual(payload['event_id'], revision['event_id'])

    def test_stale_callback_does_not_restore_old_version(self):
        self.initial_review()
        _, old = self.current()
        self.candidate = '2 + 2 的结果是 4。\n'
        self.check()
        result = self.gate.handle(self.begin_payload | {'task_id': 'new-uuid', 'continuation': True, 'event_id': old['event_id']})
        self.assertEqual(result['task_id'], 'task-one')
        self.assertEqual(result['reasons'], ['stale_callback_revision'])
        self.assertFalse(result['allow_publish'])

    def test_wrong_review_task_or_candidate_is_rejected(self):
        for field in ('task_id', 'candidate_sha256', 'manifest_sha256', 'revision'):
            with self.subTest(field=field):
                task_id = 'mismatch-' + field
                self.dest = Path(self.gate.handle(self.begin_payload | {'task_id': task_id})['task_dir'])
                self.manifest['task_id'] = task_id
                self.write_manifest()
                self.check()
                self.change_report(lambda report: report.update({field: 'wrong'}))
                result = self.check()
                self.assertFalse(result['allow_publish'])
                self.assertEqual(result['reasons'], ['hermes_report_binding_mismatch'])

    def test_fake_completed_with_failed_process_is_rejected(self):
        self.check()
        _, revision = self.current()
        path = self.runtime / 'runs' / revision['job_id'] / 'result.json'
        value = review.read_json(path)
        value['returncode'] = 1
        path.write_bytes(review.json_bytes(value))
        self.assertEqual(self.check()['reasons'], ['review_exit_not_successful'])

    def test_fake_completed_with_tampered_response_is_rejected(self):
        self.check()
        _, revision = self.current()
        (self.runtime / 'runs' / revision['job_id'] / 'response.md').write_text('{"verdict":"pass"}')
        self.assertEqual(self.check()['reasons'], ['artifact_modified'])

    def test_worker_group_and_locks_must_be_released(self):
        self.check()
        with patch.object(jobs, 'pid_probe', return_value='exists_unverified'):
            self.assertEqual(self.check()['reasons'], ['hermes_cleanup_pending'])
        with patch.object(jobs, 'lock_probe', return_value='held'):
            self.assertEqual(self.check()['reasons'], ['hermes_cleanup_pending'])

    def test_completed_without_two_real_participation_declarations_is_rejected(self):
        self.check()
        self.change_report(lambda report: report.update(reviewers=report['reviewers'][:1]))
        self.assertEqual(self.check()['reasons'], ['hermes_actual_subreviews_missing'])

    def test_invalid_review_is_preserved_and_new_revision_requires_explicit_followup(self):
        self.check()
        self.change_report(lambda report: report.update(schema='unsupported'))
        self.assertEqual(self.check()['reasons'], ['hermes_report_schema_mismatch'])
        old_dir, _ = self.current()
        self.assertTrue((old_dir / 'hermes-response.txt').is_file())
        self.manifest['acceptance'].append('补齐审查契约')
        self.write_manifest()
        self.assertEqual(self.check()['reasons'], ['hermes_review_pending'])
        self.assertEqual(self.check()['reasons'], ['prior_findings_not_verified'])

    def test_report_cannot_invent_successful_subagent_execution(self):
        self.check()
        _, revision = self.current()
        run = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[run['session_id']]
        self.native_rows[run['session_id']] = [row for row in rows if row.get('type') not in ('tool/call', 'tool/result', 'subagent/catalog')]
        self.assertEqual(self.check()['reasons'], ['native_two_subreviews_missing'])

    def test_native_failed_or_background_child_does_not_count_as_review(self):
        self.check()
        _, revision = self.current()
        run = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[run['session_id']]
        result = next(row for row in rows if row.get('type') == 'tool/result')
        call = next(row for row in rows if row.get('type') == 'tool/call')
        result['data']['message']['isError'] = True
        self.assertEqual(self.check()['reasons'], ['native_subagent_failed'])
        result['data']['message']['isError'] = False
        arguments = json.loads(call['data']['arguments'])
        arguments['run_in_background'] = True
        call['data']['arguments'] = json.dumps(arguments)
        self.assertEqual(self.check()['reasons'], ['native_subagent_not_synchronous'])

    def test_parent_and_child_native_models_are_checked(self):
        self.check()
        _, revision = self.current()
        run = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[run['session_id']]
        config = next(row['data']['header']['config'] for row in rows if row.get('type') == 'request/header')
        config['reasoningEffort'] = 'max'
        self.assertEqual(self.check()['reasons'], ['native_review_model_mismatch'])
        config['reasoningEffort'] = 'high'
        child_id = next(row['data']['childId'] for row in rows if row.get('type') == 'subagent/catalog')
        self.native_rows[child_id][1]['data']['header']['config']['provider'] = 'happy2'
        self.assertEqual(self.check()['reasons'], ['native_review_model_mismatch'])

    def test_native_child_must_belong_to_parent_and_match_returned_text(self):
        self.check()
        _, revision = self.current()
        run = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[run['session_id']]
        child_id = next(row['data']['childId'] for row in rows if row.get('type') == 'subagent/catalog')
        child = self.native_rows[child_id]
        child[0]['parentSession'] = 'session-foreign'
        self.assertEqual(self.check()['reasons'], ['native_child_session_mismatch'])
        child[0]['parentSession'] = run['session_id']
        child[-1]['data']['message']['content'][0]['text'] = '与父会话返回不同的正文'
        self.assertEqual(self.check()['reasons'], ['native_child_result_mismatch'])

    def test_native_task_and_final_window_cannot_be_forged_by_receipts(self):
        self.check()
        _, revision = self.current()
        run = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[run['session_id']]
        original = rows[-1]['data']['message']['content'][0]['text']
        rows[-1]['data']['message']['content'][0]['text'] = '实际原生正文不同'
        self.assertEqual(self.check()['reasons'], ['native_final_not_in_source'])
        rows[-1]['data']['message']['content'][0]['text'] = original
        rows[1]['data']['content'][0]['text'] = '其他任务'
        self.assertEqual(self.check()['reasons'], ['native_task_not_in_source'])

    def test_native_source_window_excludes_later_user_turn(self):
        self.check()
        _, revision = self.current()
        run = review.read_json(self.runtime / 'runs' / revision['job_id'] / 'result.json')
        rows = self.native_rows[run['session_id']]
        rows.extend([
            {'type': 'user/message', 'seq': 100, 'data': {'source': {'kind': 'user'},
             'content': [{'type': 'text', 'text': '另一个任务'}]}},
            {'type': 'assistant/message', 'seq': 101, 'data': {'message': {
             'content': [{'type': 'text', 'text': '另一个答案'}]}}}
        ])
        self.assertEqual(self.check()['reasons'], ['dorothy_response_missing'])
        native = review.read_json(self.current()[0] / 'native-evidence.json')
        self.assertLess(native['last_seq'], 100)

    def test_native_source_cursor_rejects_prior_task(self):
        self.check()
        _, revision = self.current()
        path = self.runtime / 'runs' / revision['job_id'] / 'result.json'
        run = review.read_json(path)
        run['source_before_seq'] = 100
        path.write_bytes(review.json_bytes(run))
        self.assertEqual(self.check()['reasons'], ['native_task_not_in_source'])

    def test_workspace_lease_is_context_not_old_process_evidence(self):
        self.initial_review()
        self.respond()
        def lock_probe(path):
            return 'held' if path.parent.name == 'workspaces' else 'released'
        with patch.object(jobs, 'lock_probe', side_effect=lock_probe):
            self.assertTrue(self.check()['allow_publish'])

    def test_changed_settings_do_not_pass(self):
        self.check()
        _, revision = self.current()
        for directory, name in ((self.runtime / 'jobs' / revision['job_id'], 'request.json'),
                                (self.runtime / 'jobs' / revision['job_id'], 'receipt.json'),
                                (self.runtime / 'runs' / revision['job_id'], 'result.json')):
            value = review.read_json(directory / name)
            value['reasoning_effort'] = 'max'
            (directory / name).write_bytes(review.json_bytes(value))
        self.assertEqual(self.check()['reasons'], ['review_model_settings_mismatch'])

    def test_responding_to_old_report_hash_does_not_release(self):
        self.initial_review()
        self.respond()
        path = self.dest / 'response.json'
        response = review.read_json(path)
        response['hermes_response_sha256'] = 'b' * 64
        path.write_bytes(review.json_bytes(response))
        self.assertEqual(self.check()['reasons'], ['dorothy_response_binding_mismatch'])

    def test_changes_required_cannot_be_overridden_by_author(self):
        self.check()
        self.change_report(lambda report: report.update(verdict='changes_required'))
        self.assertEqual(self.check()['reasons'], ['hermes_changes_or_unresolved'])
        self.respond()
        self.assertFalse(self.check()['allow_publish'])

    def test_changed_manifest_and_candidate_need_new_review(self):
        self.initial_review()
        self.respond()
        self.assertTrue(self.check()['allow_publish'])
        old = self.current()[1]
        self.manifest['acceptance'].append('增加表达审查')
        self.write_manifest()
        result = self.check()
        self.assertFalse(result['allow_publish'])
        self.assertNotEqual(result['revision'], old['revision'])
        self.assertEqual(len(self.submissions), 2)
        self.assertEqual(self.check()['reasons'], ['dorothy_response_binding_mismatch'])

    def test_source_and_snapshot_changes_are_detected(self):
        source = self.workspace / 'result.txt'
        source.write_text('four')
        self.manifest.update(mode='artifacts', artifacts=[{'path': str(source), 'sha256': review.sha(b'four')}])
        self.write_manifest()
        self.initial_review()
        self.respond()
        self.assertTrue(self.check()['allow_publish'])
        source.write_text('five')
        self.assertEqual(self.check()['reasons'], ['artifact_hash_mismatch'])
        source.write_text('four')
        directory, revision = self.current()
        snapshot = directory / 'materials' / revision['files'][0]['snapshot']
        snapshot.chmod(0o600)
        snapshot.write_text('five')
        self.assertEqual(self.check()['reasons'], ['frozen_material_modified'])

    def test_prior_key_findings_require_actual_followup(self):
        finding = {'id': 'F1', 'kind': 'defect', 'judgment': 'confirmed', 'disposition': 'needs_change',
                   'critical': True, 'claim': '需要更明确的证明', 'evidence': ['candidate.txt']}
        self.check()
        self.change_report(lambda report: report.update(verdict='changes_required', findings=[finding]))
        self.check()
        self.manifest['acceptance'].append('补充证明依据')
        self.write_manifest()
        self.check()
        self.assertEqual(self.check()['reasons'], ['prior_findings_not_verified'])

    def test_revision_change_collects_unread_previous_findings(self):
        finding = {'id': 'F1', 'kind': 'defect', 'judgment': 'confirmed', 'disposition': 'needs_change',
                   'critical': True, 'claim': '上一轮尚未取件的关键发现', 'evidence': ['candidate.txt']}
        self.check()
        old_dir, old = self.current()
        self.change_report(lambda report: report.update(verdict='changes_required', findings=[finding]))
        self.assertFalse((old_dir / 'hermes-response.txt').exists())
        self.candidate = '结果为四。\n'
        self.assertEqual(self.check()['reasons'], ['hermes_review_pending'])
        new_dir, new = self.current()
        self.assertEqual(new['prior_findings'][0]['finding_id'], old['revision'] + '/F1')
        self.assertTrue((new_dir / 'materials/previous-review.json').is_file())
        self.assertEqual(self.check()['reasons'], ['prior_findings_not_verified'])

    def test_revision_change_waits_for_previous_pending_review(self):
        self.check()
        _, old = self.current()
        self.candidate = '结果为四。\n'
        with patch.object(jobs, 'status', return_value={'status': 'running'}):
            self.assertEqual(self.check()['reasons'], ['previous_hermes_review_pending'])
        self.assertEqual(self.current()[1]['revision'], old['revision'])
        self.assertEqual(len(self.submissions), 1)
        self.assertTrue((self.dest / 'drafts' / (review.sha(self.candidate.encode()) + '.txt')).is_file())

    def test_busy_keeps_revision_without_dispatch(self):
        with patch.object(review, 'capacity_reason', return_value='capacity_busy'):
            result = self.check()
        self.assertEqual(result['reasons'], ['capacity_busy'])
        self.assertFalse(result['allow_publish'])
        self.assertEqual(self.submissions, [])
        self.check()
        self.assertEqual(len(self.submissions), 1)

    def test_uncertain_submission_is_never_retried(self):
        with patch.object(jobs, 'submit', side_effect=OSError('do not disclose me')) as submit:
            self.assertEqual(self.check()['reasons'], ['submission_unconfirmed'])
            self.assertEqual(self.check()['reasons'], ['submission_unconfirmed'])
            self.assertEqual(submit.call_count, 1)

    def test_late_receipt_reconciles_without_resubmitting(self):
        with patch.object(jobs, 'submit', side_effect=OSError('receipt response lost')):
            self.assertEqual(self.check()['reasons'], ['submission_unconfirmed'])
        directory, revision = self.current()
        self.fake_submit(directory / 'review-task.md', revision['job_id'],
                         cwd=str(directory / 'materials'), profile='opencode-go',
                         reasoning_effort='high', delivery='conversation',
                         callback_target=self.binding)
        self.assertEqual(self.check()['reasons'], ['dorothy_response_missing'])
        self.respond()
        self.assertTrue(self.check()['allow_publish'])

    def test_protocol_and_continuation_type_fail_closed(self):
        self.assertEqual(self.gate.handle(self.begin_payload | {'protocol': 'wrong'})['reasons'], ['protocol_mismatch'])
        self.assertEqual(self.gate.handle(self.begin_payload | {'continuation': 'true'})['reasons'], ['invalid_continuation'])

    def test_cli_emits_one_json_and_requires_identity_before_missing_manifest_without_model(self):
        # An unbound CLI check fails before capture/submit; no live IPC is contacted.
        source_workspace = Path(bridge.__file__).resolve().parents[3]
        command = [sys.executable, '-B', str(Path(review.__file__).resolve()),
                   '--review-root', str(self.workspace / 'cli-review'), '--runtime', str(self.runtime)]
        origin = self.origin | {'cwd': str(source_workspace)}
        begin = self.begin_payload | {'origin': origin, 'task_id': 'cli-task'}
        process = subprocess.run(command, input=json.dumps(begin), text=True, capture_output=True, timeout=5)
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertEqual(len(process.stdout.splitlines()), 1)
        result = json.loads(process.stdout)
        self.assertEqual(result['status'], 'pending_review')
        payload = {'protocol': review.PROTOCOL, 'op': 'check', 'task_id': result['task_id'], 'origin': origin,
                   'candidate_text': self.candidate, 'candidate_sha256': review.sha(self.candidate.encode()),
                   'conversation_id': '', 'manifest_path': result['manifest_path']}
        checked = subprocess.run(command, input=json.dumps(payload), text=True, capture_output=True, timeout=5)
        self.assertEqual(json.loads(checked.stdout)['reasons'], ['conversation_id_missing'])
        self.assertEqual(list((self.runtime / 'jobs').glob('*/receipt.json')), [])


class NativeChildSourceTests(unittest.TestCase):
    def test_non_uuid_child_cannot_select_arbitrary_source(self):
        for child_id in ('../outside', 'session-foreign', '', None):
            with self.subTest(child_id=child_id), self.assertRaisesRegex(ValueError, 'native_child_id_invalid'):
                review.native_child_rows(child_id, Path('/unused'))

    @unittest.skipUnless(shutil.which('zstd'), 'zstd is required for native session fixtures')
    def test_uuid_child_source_roundtrip_and_symlink_rejection(self):
        with tempfile.TemporaryDirectory() as temp:
            runtime = Path(temp).resolve()
            child_id = 'bc4513df-bb48-4ac3-994e-fb422e732978'
            directory = runtime / 'home/sessions/workspace' / child_id
            directory.mkdir(parents=True)
            source = directory / 'session.v4.jsonl.zstd'
            rows = [{'type': 'session', 'id': child_id, 'parentSession': 'session-parent', 'cwd': temp}]
            compressed = subprocess.run(['zstd', '-q', '-c'], input=(json.dumps(rows[0]) + '\n').encode(),
                                        capture_output=True, check=True, timeout=5).stdout
            source.write_bytes(compressed)
            selected, actual = review.native_child_rows(child_id, runtime)
            self.assertEqual(selected, source)
            self.assertEqual(actual, rows)
            moved = runtime / 'outside.zstd'
            source.rename(moved)
            source.symlink_to(moved)
            with self.assertRaisesRegex(ValueError, 'native_child_source_outside_runtime'):
                review.native_child_rows(child_id, runtime)


if __name__ == '__main__':
    unittest.main()
