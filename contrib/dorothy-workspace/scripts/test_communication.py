"""Real peer receipts in temporary directories; transport, watchers and jobs are mocked."""
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import bridge
import communication as communication
import ipc
import jobs
import peer_ipc


class CommunicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.source_dir = self.root / 'dorothy'
        self.source_dir.mkdir(mode=0o700)
        self.runtime = self.root / 'peer'
        self.runtime.mkdir(mode=0o700)
        self.work = self.root / 'work'
        self.work.mkdir(mode=0o700)
        self.dsh = self.root / 'dsh'
        self.dsh.mkdir(mode=0o700)
        self.source = self.binding('dorothy', self.source_dir)
        self.atlas = self.binding('atlas', self.root)
        self.other = self.binding('other', self.root)
        self.current = self.source
        self.receipts = {}
        self.event_posts = []
        self.lose_response = False
        self.patches = [patch.object(bridge, 'WORKSPACE', self.root),
                        patch.object(ipc, 'capture', side_effect=lambda: deepcopy(self.current)),
                        patch.object(ipc, 'call', side_effect=self.transport),
                        patch.object(peer_ipc, 'launch_watch', return_value={'status': 'watching'})]
        for item in self.patches:
            item.start()
        peer_ipc.register('atlas', self.runtime, binding=self.atlas)
        peer_ipc.register('dorothy', self.runtime, binding=self.source)
        peer_ipc.register('other', self.runtime, binding=self.other)
        ipc.capture.reset_mock()
        ipc.call.reset_mock()
        self.policy = {'scripts_dir': Path(peer_ipc.__file__).resolve().parent,
                       'peer_runtime': self.runtime, 'dsh_runtime': self.dsh, 'work_dir': self.work,
                       'source_cwd': self.source_dir, 'source_prefix': 'dorothy',
                       'allowed_contacts': {'atlas': 'atlas'}, 'enable_consultation': False,
                       'materials': {}, 'consultation_timeout': 300}
        self.job_records = {}
        self.jobs = SimpleNamespace(submit=Mock(side_effect=self.submit_job),
                                    status=Mock(side_effect=lambda key, runtime: deepcopy(self.job_records[key])),
                                    result=Mock(return_value={'response': '只读咨询结论。'}),
                                    job_path=lambda runtime, key: runtime / 'jobs' / key)
        self.capacity = Mock(return_value=None)
        self.backend = communication.Communication(self.policy, SimpleNamespace(
            bridge=bridge, ipc=ipc, peer=peer_ipc, jobs=self.jobs, capacity_reason=self.capacity))

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.temp.cleanup()

    def binding(self, name, cwd):
        return {'protocol': ipc.PROTOCOL, 'socket': str(self.root / 'api.sock'),
                'target': {'project': name, 'session_key': 'chat:' + name, 'session_id': 'cc-' + name,
                           'agent_session_id': 'native-' + name, 'cwd': str(cwd)}}

    def transport(self, socket, method, route, body=None, **kwargs):
        if route == '/ipc/target':
            return {'protocol': ipc.PROTOCOL, 'supports_live_input': True, 'target': body}
        if route == '/ipc/event' and method == 'POST':
            self.event_posts.append(deepcopy(body))
            receipt = {'input': deepcopy(body), 'status': 'delivered', 'method': 'queue'}
            self.receipts[body['event_id']] = receipt
            if self.lose_response:
                raise ipc.DeliveryUnknown()
            return deepcopy(receipt)
        if route.startswith('/ipc/event?event_id=') and method == 'GET':
            return deepcopy(self.receipts[route.split('=', 1)[1]])
        if route == '/ipc/ack':
            receipt = self.receipts[body['event_id']]
            self.assertEqual(body['target'], receipt['input']['target'])
            receipt.update(status='delivered', acknowledged_at='2026-10-11T12:00:00Z')
            return deepcopy(receipt)
        raise AssertionError('unexpected transport route')

    def submit_job(self, task, job_id, **kwargs):
        directory = self.dsh / 'jobs' / job_id
        directory.mkdir(mode=0o700, parents=True)
        request = {key: kwargs[key] for key in ('profile', 'reasoning_effort', 'delivery', 'callback_target', 'cwd', 'tool_policy')}
        request['task_sha256'] = communication.digest(task.read_bytes())
        ipc.write_state(directory / 'request.json', request)
        record = {'job_id': job_id, 'status': 'accepted'}
        ipc.write_state(directory / 'receipt.json', record)
        self.job_records[job_id] = record
        return record

    def request(self, name, arguments=None, scope='turn-one'):
        return {'protocol': communication.PROTOCOL, 'method': 'tools/call',
                'params': {'name': name, 'arguments': arguments or {}},
                'context': {'operation_scope': scope}}

    def call(self, name, arguments=None, scope='turn-one', error=False):
        result = communication.execute(self.request(name, arguments, scope), self.backend)
        self.assertEqual(result['isError'], error, result)
        return json.loads(result['content'][0]['text'])

    def incoming(self, sender='atlas', text='请核对这段资料。'):
        self.current = self.atlas if sender == 'atlas' else self.other
        path = self.root / 'incoming.txt'
        communication.immutable(path, text.encode())
        result = peer_ipc.send(sender, 'dorothy', 'incoming-' + sender, path, self.runtime)
        self.current = self.source
        return result['event_id']

    def completed(self, job_id):
        self.job_records[job_id].update(status='completed', cleanup={
            'state': 'released', 'dsh_returncode': 0, 'worker_process': 'absent',
            'dsh_process': 'absent', 'dsh_process_group': 'absent',
            'worker_lock': 'released', 'shared_bridge_lock': 'released'})

    def test_tools_list_and_contacts_never_capture_or_launch(self):
        request = self.request('unused') | {'method': 'tools/list', 'params': {}}
        result = communication.execute(request, self.backend)
        self.assertEqual({item['name'] for item in result['tools']}, {
            'list_contacts', 'send_message', 'reply_message', 'read_message', 'message_status', 'ack_message'})
        ipc.call.assert_not_called()
        self.assertEqual(self.call('list_contacts'), {'contacts': [
            {'alias': 'atlas', 'purpose': '任务主题未核实', 'status': 'available'}]})
        ipc.capture.assert_not_called()
        self.jobs.submit.assert_not_called()
        peer_ipc.launch_watch.assert_not_called()

    def test_native_mcp_metadata_does_not_select_identity_or_paths(self):
        metadata = {'progressToken': 1,
                    'antigravity.google/conversation_id': 'untrusted-native-id',
                    'antigravity.google/artifacts_dir': '/untrusted/path',
                    'operation_scope': 'untrusted-scope'}
        request = self.request('list_contacts')
        request['params']['_meta'] = metadata
        result = communication.execute(request, self.backend)
        self.assertFalse(result['isError'], result)
        ipc.capture.assert_not_called()
        request = self.request('send_message', {'contact': 'atlas', 'text': 'native metadata test'})
        request['params']['_meta'] = metadata
        result = communication.execute(request, self.backend)
        self.assertFalse(result['isError'], result)
        self.assertEqual(len(self.event_posts), 1)
        self.assertEqual(self.event_posts[0]['target'], self.atlas['target'])
        self.assertEqual(self.current, self.source)
        request['params']['context'] = {'operation_scope': 'forged'}
        self.assertTrue(communication.execute(request, self.backend)['isError'])
        request['params'].pop('context')
        request['params']['_meta'] = 'invalid metadata'
        self.assertTrue(communication.execute(request, self.backend)['isError'])
        self.assertEqual(len(self.event_posts), 1)

    def test_contacts_show_two_verified_atlas_labels_and_only_actual_busy(self):
        self.policy.update(allowed_contacts={'atlas-current': 'atlas', 'atlas-review': 'other'},
                           contact_labels={'atlas-current': 'Dorothy接入迁移', 'atlas-review': '回调修复复核'})
        before = {path.name: path.read_bytes() for path in (self.runtime / 'contacts').glob('*.json')}

        def probe(socket, method, route, body, **kwargs):
            self.assertEqual((method, route), ('POST', '/ipc/target'))
            self.assertEqual(kwargs, {'identity_lookup': True, 'timeout': 2})
            value = {'protocol': ipc.PROTOCOL, 'target': deepcopy(body), 'supports_live_input': True,
                     'socket': '/private/never-returned', 'agent_session_id': 'never-returned'}
            if body == self.atlas['target']:
                value['busy'] = True
            else:
                value['busy'] = 'false'
            return value

        with patch.object(ipc, 'call', side_effect=probe), patch.object(peer_ipc, 'register') as register, \
                patch.object(peer_ipc, 'send') as send:
            result = self.call('list_contacts')
        self.assertEqual(result, {'contacts': [
            {'alias': 'atlas-current', 'purpose': 'Dorothy接入迁移', 'status': 'available', 'busy': True},
            {'alias': 'atlas-review', 'purpose': '回调修复复核', 'status': 'available'}]})
        register.assert_not_called()
        send.assert_not_called()
        ipc.capture.assert_not_called()
        self.jobs.submit.assert_not_called()
        peer_ipc.launch_watch.assert_not_called()
        self.assertEqual(self.event_posts, [])
        self.assertNotIn('never-returned', json.dumps(result))
        self.assertEqual(before, {path.name: path.read_bytes() for path in (self.runtime / 'contacts').glob('*.json')})

    def test_contact_checks_distinguish_changes_offline_and_unknown_without_rebinding(self):
        contacts = ('changed', 'offline', 'uncertain', 'unsupported', 'wrong')
        for contact in contacts:
            fixed = deepcopy(self.atlas)
            fixed['target']['project'] = contact
            ipc.write_state(self.runtime / 'contacts' / (contact + '.json'),
                            {'protocol': peer_ipc.VERSION, 'name': contact, 'binding': fixed})
        # This import snapshot is not a registered contact and must not escape as metadata.
        ipc.write_state(self.runtime / 'contacts' / 'invalid.json', self.atlas)
        self.policy['allowed_contacts'] = {name: name for name in (*contacts, 'missing', 'invalid')}
        before = {path.name: path.read_bytes() for path in (self.runtime / 'contacts').glob('*.json')}

        def probe(socket, method, route, body, **kwargs):
            self.assertEqual((method, route), ('POST', '/ipc/target'))
            self.assertEqual(kwargs, {'identity_lookup': True, 'timeout': 2})
            kind = body['project']
            if kind == 'changed':
                raise ipc.ReceiverRejected('ipc_target_changed')
            if kind == 'offline':
                raise ipc.ReceiverOffline()
            if kind == 'uncertain':
                raise ipc.DeliveryUnknown()
            if kind == 'unsupported':
                raise ipc.ReceiverRejected('ipc_project_unsupported')
            return {'protocol': ipc.PROTOCOL, 'target': body | {'agent_session_id': 'replacement'},
                    'supports_live_input': True, 'busy': False}

        with patch.object(ipc, 'call', side_effect=probe) as call:
            result = self.call('list_contacts')
        self.assertEqual({item['alias']: item['status'] for item in result['contacts']}, {
            'changed': 'changed', 'offline': 'offline', 'uncertain': 'unknown', 'unsupported': 'unknown',
            'wrong': 'changed', 'missing': 'unregistered', 'invalid': 'unknown'})
        self.assertEqual(call.call_count, 5)
        self.assertTrue(all(set(item) == {'alias', 'purpose', 'status'} for item in result['contacts']))
        self.assertNotIn(str(self.root), json.dumps(result))
        self.assertNotIn('replacement', json.dumps(result))
        self.assertEqual(before, {path.name: path.read_bytes() for path in (self.runtime / 'contacts').glob('*.json')})
        self.assertEqual(self.event_posts, [])

    def test_contact_checks_use_bounded_parallelism_and_keep_alias_order(self):
        aliases = ['atlas-' + str(index) for index in range(8)]
        self.policy['allowed_contacts'] = {alias: 'atlas' for alias in reversed(aliases)}
        barrier, lock = threading.Barrier(4), threading.Lock()
        active, peak = 0, 0

        def probe(socket, method, route, body, **kwargs):
            nonlocal active, peak
            self.assertEqual(kwargs['timeout'], 2)
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                barrier.wait(timeout=3)
                return {'protocol': ipc.PROTOCOL, 'target': body, 'supports_live_input': True, 'busy': False}
            finally:
                with lock:
                    active -= 1

        with patch.object(ipc, 'call', side_effect=probe) as call:
            result = self.call('list_contacts')
        self.assertEqual(peak, 4)
        self.assertEqual(call.call_count, len(aliases))
        self.assertEqual([item['alias'] for item in result['contacts']], aliases)
        self.assertTrue(all(item['status'] == 'available' and item['busy'] is False for item in result['contacts']))

    def test_empty_allowlists_do_not_publish_invalid_empty_enums(self):
        self.policy.update(allowed_contacts={}, enable_consultation=True, materials={})
        published = {item['name']: item for item in self.backend.tools()}
        contact = published['send_message']['inputSchema']['properties']['contact']
        material = published['consult_hermes']['inputSchema']['properties']['material_ids']['items']
        self.assertEqual(contact, {'type': 'string'})
        self.assertEqual(material, {'type': 'string'})
        self.assertEqual(self.call('list_contacts'), {'contacts': []})
        self.assertEqual(self.call('send_message', {'contact': 'atlas', 'text': '正文'}, error=True)['code'],
                         'contact_not_allowed')
        self.assertEqual(self.call('consult_hermes', {'question': '问题', 'material_ids': ['unknown']}, error=True)['code'],
                         'material_id_invalid')
        self.assertEqual(self.call('consult_hermes', {'question': '问题'})['status'], 'accepted')

    def test_send_preserves_text_and_uses_current_source_without_paths_in_result(self):
        body = '第一行。\n第二行。\n'
        result = self.call('send_message', {'contact': 'atlas', 'text': body})
        directory, record, payload = peer_ipc.load_message(result['event_id'], self.runtime)
        self.assertEqual(record['sender'], self.source)
        self.assertEqual(record['recipient'], self.atlas)
        self.assertTrue(record['sender_name'].startswith('dorothy-'))
        self.assertEqual((directory / 'body.md').read_text(), body)
        self.assertEqual(payload['target'], self.atlas['target'])
        self.assertNotIn('body_path', result)
        self.assertNotIn('socket', json.dumps(result))
        self.jobs.submit.assert_not_called()

    def test_same_operation_reuses_event_and_never_posts_twice(self):
        arguments = {'contact': 'atlas', 'text': '核对。'}
        first = self.call('send_message', arguments)
        second = self.call('send_message', arguments)
        self.assertEqual(first['event_id'], second['event_id'])
        self.assertEqual(len(self.event_posts), 1)
        changed = self.call('send_message', arguments, scope='turn-two')
        self.assertNotEqual(first['event_id'], changed['event_id'])
        self.assertEqual(len(self.event_posts), 2)

    def test_lost_response_retry_only_queries_original_event(self):
        self.lose_response = True
        arguments = {'contact': 'atlas', 'text': '原正文。'}
        first = self.call('send_message', arguments)
        self.assertEqual(first['status'], 'unknown')
        self.lose_response = False
        second = self.call('send_message', arguments)
        self.assertEqual(second['event_id'], first['event_id'])
        self.assertEqual(second['status'], 'delivered')
        self.assertEqual(len(self.event_posts), 1)

    def test_repeated_operation_does_not_follow_a_rebound_contact(self):
        arguments = {'contact': 'atlas', 'text': '发给原会话。'}
        first = self.call('send_message', arguments)
        moved = deepcopy(self.atlas)
        moved['target']['session_id'] = 'new-cc'
        moved['target']['agent_session_id'] = 'new-native'
        peer_ipc.register('atlas', self.runtime, binding=moved, replace=True)
        second = self.call('send_message', arguments)
        self.assertEqual(second['event_id'], first['event_id'])
        self.assertEqual(self.event_posts[0]['target'], self.atlas['target'])
        self.assertEqual(len(self.event_posts), 1)

    def test_interrupted_pre_send_creation_recovers_only_identical_input(self):
        original = ipc.write_state

        def fail_input(path, value):
            if path.name == 'input.json':
                raise OSError('interrupted before any POST')
            return original(path, value)

        arguments = {'contact': 'atlas', 'text': '固定正文。'}
        with patch.object(ipc, 'write_state', side_effect=fail_input):
            self.call('send_message', arguments, error=True)
        self.assertEqual(self.event_posts, [])
        result = self.call('send_message', arguments)
        self.assertEqual(result['status'], 'delivered')
        self.assertEqual(len(self.event_posts), 1)
        self.assertEqual(len(list((self.runtime / 'outbox').iterdir())), 1)

    def test_untrusted_fields_and_tools_fail_before_source_lookup(self):
        for key in ('sender', 'socket', 'path', 'runtime', 'command', 'target'):
            with self.subTest(key=key):
                result = self.call('send_message', {'contact': 'atlas', 'text': '正文', key: '/private/secret'}, error=True)
                self.assertEqual(result['code'], 'invalid_fields')
        self.call('register', {}, error=True)
        self.call('read_message', {'event_id': '../secret'}, error=True)
        self.call('consultation_result', {'job_id': '../job'}, error=True)
        ipc.capture.assert_not_called()
        self.assertEqual(self.event_posts, [])

    def test_host_context_cannot_supply_a_different_identity(self):
        request = self.request('send_message', {'contact': 'atlas', 'text': '正文'})
        request['context']['origin'] = self.atlas
        result = communication.execute(request, self.backend)
        self.assertTrue(result['isError'])
        ipc.capture.assert_not_called()

    def test_contact_allowlist_and_utf8_size_are_checked(self):
        self.assertEqual(self.call('send_message', {'contact': 'other', 'text': '正文'}, error=True)['code'], 'contact_not_allowed')
        self.assertEqual(self.call('send_message', {'contact': 'atlas', 'text': '中' * communication.MAX_TEXT}, error=True)['code'], 'text_too_large')
        self.assertEqual(self.event_posts, [])

    def test_read_and_ack_require_current_recipient_and_verify_body(self):
        event = self.incoming()
        read = self.call('read_message', {'event_id': event})
        self.assertEqual(read['text'], '请核对这段资料。')
        self.assertNotIn(str(self.root), json.dumps(read))
        self.assertEqual(self.call('ack_message', {'event_id': event})['status'], 'acknowledged')
        self.current = self.other
        self.policy.pop('source_cwd')
        for name in ('read_message', 'message_status', 'ack_message', 'reply_message'):
            arguments = {'event_id': event} | ({'text': '回复'} if name == 'reply_message' else {})
            self.assertEqual(self.call(name, arguments, error=True)['code'], 'message_not_owned')

    def test_outgoing_event_cannot_be_acknowledged_or_replied_to_as_recipient(self):
        event = self.call('send_message', {'contact': 'atlas', 'text': '正文'})['event_id']
        self.assertEqual(self.call('ack_message', {'event_id': event}, error=True)['code'], 'message_not_received')
        self.assertEqual(self.call('reply_message', {'event_id': event, 'text': '回复'}, error=True)['code'], 'message_not_received')

    def test_reply_uses_original_sender_after_contact_rebinding(self):
        event = self.incoming()
        moved = deepcopy(self.atlas)
        moved['target']['agent_session_id'] = 'moved-native'
        peer_ipc.register('atlas', self.runtime, binding=moved, replace=True)
        reply = self.call('reply_message', {'event_id': event, 'text': '已核对。'})
        _, record, payload = peer_ipc.load_message(reply['event_id'], self.runtime)
        self.assertEqual(record['reply_to'], event)
        self.assertEqual(record['sender_name'], 'dorothy')
        self.assertEqual(payload['target'], self.atlas['target'])

    def test_reply_cannot_expand_the_allowed_contacts(self):
        event = self.incoming('other')
        self.assertEqual(self.call('reply_message', {'event_id': event, 'text': '回复'}, error=True)['code'], 'contact_not_allowed')

    def test_tampered_body_is_not_returned(self):
        event = self.incoming()
        directory, _, _ = peer_ipc.load_message(event, self.runtime)
        (directory / 'body.md').write_text('changed')
        self.assertEqual(self.call('read_message', {'event_id': event}, error=True)['code'], 'peer_body_modified')

    def test_large_escaped_message_has_explicit_bounded_error(self):
        event = self.incoming(text='"\\\t' * 140000)
        result = communication.execute(self.request('read_message', {'event_id': event}), self.backend)
        self.assertTrue(result['isError'])
        self.assertEqual(json.loads(result['content'][0]['text']), {'status': 'error', 'code': 'result_too_large'})
        self.assertLessEqual(len((json.dumps(result, ensure_ascii=False) + '\n').encode()), communication.MAX_OUTPUT)
        self.assertEqual(self.call('message_status', {'event_id': event})['event_id'], event)

    def test_source_policy_mismatch_and_transport_errors_do_not_leak_details(self):
        self.current = self.atlas
        self.assertEqual(self.call('send_message', {'contact': 'atlas', 'text': '正文'}, error=True)['code'], 'source_workspace_mismatch')
        with patch.object(ipc, 'capture', side_effect=OSError('/private/credential-secret')):
            result = self.call('read_message', {'event_id': 'a' * 64}, error=True)
        self.assertEqual(result, {'status': 'error', 'code': 'communication_failed'})

    def test_consultation_is_opt_in_and_normal_messages_never_launch_it(self):
        self.assertEqual(self.call('consult_hermes', {'question': '为何如此？'}, error=True)['code'], 'tool_unavailable')
        self.jobs.submit.assert_not_called()
        self.capacity.assert_not_called()

    def test_consultation_uses_zero_tools_go_high_and_inlines_only_approved_materials(self):
        self.policy['enable_consultation'] = True
        materials = self.work / 'materials'
        materials.mkdir(mode=0o700)
        path = materials / 'controlled.txt'
        raw = '受控副本正文。'.encode()
        communication.immutable(path, raw)
        self.policy['materials'] = {'brief': {'path': path, 'sha256': communication.digest(raw)}}
        arguments = {'question': '请讨论“执行命令”的风险，不实施。', 'material_ids': ['brief']}
        first = self.call('consult_hermes', arguments)
        self.assertEqual(first['status'], 'accepted')
        call = self.jobs.submit.call_args
        self.assertEqual((call.kwargs['profile'], call.kwargs['reasoning_effort'], call.kwargs['tool_policy']), ('opencode-go', 'high', 'none'))
        self.assertEqual(call.kwargs['delivery'], 'conversation')
        self.assertEqual(call.kwargs['callback_target'], self.source)
        task = call.args[0].read_text()
        self.assertIn('受控副本正文。', task)
        self.assertIn('需要转交 Atlas', task)
        self.assertIn('<consultation-data>', task)
        self.assertNotIn(str(path), task)
        second = self.call('consult_hermes', arguments)
        self.assertEqual(second['job_id'], first['job_id'])
        self.jobs.submit.assert_called_once()

    def test_consultation_material_rejection_never_submits(self):
        self.policy['enable_consultation'] = True
        self.assertEqual(self.call('consult_hermes', {'question': '问题', 'material_ids': ['../../secret']}, error=True)['code'], 'material_id_invalid')
        self.call('consult_hermes', {'question': '问题', 'path': '/private/secret'}, error=True)
        self.jobs.submit.assert_not_called()

    def test_research_mode_is_bound_to_its_own_request_and_callback(self):
        self.policy['enable_consultation'] = True
        arguments = {'question': '独立搜索公开资料，核对原文并列出来源。', 'mode': 'research'}
        first = self.call('consult_hermes', arguments)
        self.assertEqual(first['status'], 'accepted')
        call = self.jobs.submit.call_args
        self.assertEqual((call.kwargs['profile'], call.kwargs['reasoning_effort'],
                          call.kwargs['tool_policy']), ('opencode-go', 'high', 'research'))
        self.assertEqual(call.kwargs['callback_target'], self.source)
        task = call.args[0].read_text()
        self.assertIn('独立搜索', task)
        self.assertIn('来源', task)
        self.assertIn('需要转交 Atlas', task)
        self.assertNotIn('本次不开放工具', task)
        self.assertEqual(self.call('consult_hermes', arguments)['job_id'], first['job_id'])
        self.jobs.submit.assert_called_once()
        self.completed(first['job_id'])
        self.assertTrue(self.call('consultation_result', {'job_id': first['job_id']})['processes_released'])
        path = self.dsh / 'jobs' / first['job_id'] / 'request.json'
        request = json.loads(path.read_text())
        request['tool_policy'] = 'none'
        ipc.write_state(path, request)
        self.assertEqual(self.call('consultation_result', {'job_id': first['job_id']}, error=True)['code'],
                         'consultation_request_mismatch')

    def test_explicit_text_mode_preserves_existing_id_and_research_is_distinct(self):
        self.policy['enable_consultation'] = True
        original = self.call('consult_hermes', {'question': '问题'})
        explicit = self.call('consult_hermes', {'question': '问题', 'mode': 'text'})
        self.assertEqual(explicit['job_id'], original['job_id'])
        self.jobs.submit.assert_called_once()
        research = self.call('consult_hermes', {'question': '问题', 'mode': 'research'})
        self.assertNotEqual(research['job_id'], original['job_id'])

    def test_consultation_mode_never_accepts_arbitrary_policy_or_execution_parameters(self):
        self.policy['enable_consultation'] = True
        for mode in ['default', 'shell', '/tmp/policy', None, [], True]:
            with self.subTest(mode=mode):
                self.assertEqual(self.call('consult_hermes', {'question': '问题', 'mode': mode}, error=True)['code'],
                                 'consultation_mode_invalid')
        for key in ['cwd', 'tool_policy', 'profile', 'command', 'repository']:
            with self.subTest(key=key):
                self.call('consult_hermes', {'question': '问题', 'mode': 'research', key: '/private/data'}, error=True)
        self.jobs.submit.assert_not_called()

    def test_busy_consultation_never_queues_or_retries_its_operation(self):
        self.policy['enable_consultation'] = True
        self.capacity.return_value = 'capacity_busy'
        first = self.call('consult_hermes', {'question': '问题'})
        self.assertEqual(first['status'], 'busy')
        self.capacity.return_value = None
        second = self.call('consult_hermes', {'question': '问题'})
        self.assertEqual(second, first)
        self.jobs.submit.assert_not_called()
        self.capacity.assert_called_once()

    def test_unknown_submission_is_not_resubmitted(self):
        self.policy['enable_consultation'] = True
        self.jobs.submit.side_effect = OSError('private backend failure')
        first = self.call('consult_hermes', {'question': '问题'})
        second = self.call('consult_hermes', {'question': '问题'})
        self.assertEqual(first, second)
        self.assertEqual(first['status'], 'unknown')
        self.jobs.submit.assert_called_once()

    def test_consultation_queries_require_ownership_and_original_request(self):
        self.policy['enable_consultation'] = True
        job = self.call('consult_hermes', {'question': '问题'})['job_id']
        self.current = self.other
        self.policy.pop('source_cwd')
        self.assertEqual(self.call('consultation_result', {'job_id': job}, error=True)['code'], 'consultation_not_owned')
        self.jobs.status.assert_not_called()
        self.current = self.source
        path = self.dsh / 'jobs' / job / 'request.json'
        request = json.loads(path.read_text())
        request['tool_policy'] = 'default'
        ipc.write_state(path, request)
        self.assertEqual(self.call('consultation_result', {'job_id': job}, error=True)['code'], 'consultation_request_mismatch')

    def test_completed_consultation_waits_for_verified_process_release(self):
        self.policy['enable_consultation'] = True
        job = self.call('consult_hermes', {'question': '问题'})['job_id']
        self.completed(job)
        self.job_records[job]['cleanup']['worker_process'] = 'exists_unverified'
        self.assertEqual(self.call('consultation_result', {'job_id': job})['status'], 'finishing')
        self.jobs.result.assert_not_called()
        self.job_records[job]['cleanup']['worker_process'] = 'absent'
        self.assertEqual(self.call('consultation_result', {'job_id': job})['text'], '只读咨询结论。')

    def test_large_consultation_answer_has_explicit_bounded_error(self):
        self.policy['enable_consultation'] = True
        job = self.call('consult_hermes', {'question': '问题'})['job_id']
        self.completed(job)
        self.jobs.result.return_value = {'response': '"\\\t' * 140000}
        self.assertEqual(self.call('consultation_result', {'job_id': job}, error=True)['code'], 'result_too_large')

    def real_completed_job(self, mode='text'):
        """Native job/receipt/run shape; no worker, transport or model is started."""
        self.policy['enable_consultation'] = True
        job_id = self.call('consult_hermes', {'question': '问题', 'mode': mode})['job_id']
        directory = self.dsh / 'jobs' / job_id
        request = json.loads((directory / 'request.json').read_text())
        policy = {'tool_policy': communication.CONSULTATION_MODES[mode], 'tool_policy_version': 1,
                  'model_config_sha256': communication.digest(b'fixture model'),
                  'tool_policy_plugin_sha256': communication.digest(b'fixture plugin'),
                  'tool_policy_overlay_sha256': communication.digest(b'fixture overlay')}
        policy['config_sha256'] = communication.digest(communication.encoded(policy))
        request.update(policy, instance_id='fixture-instance')
        instance = self.dsh / 'instances' / request['instance_id']
        (instance / 'home').mkdir(mode=0o700, parents=True)
        (instance / 'work').mkdir(mode=0o700)
        communication.immutable(instance / 'bridge.lock', b'')
        communication.immutable(directory / 'worker.lock', b'')
        run_dir = self.dsh / 'runs' / 'fixture-run'
        run_dir.mkdir(mode=0o700, parents=True)
        response = '实际 jobs.result 校验过的咨询结论。'.encode()
        communication.immutable(run_dir / 'response.md', response)
        task = self.work / 'consultations' / job_id / 'task.md'
        communication.immutable(run_dir / 'task.md', task.read_bytes())
        run = {**policy, 'status': 'completed', 'instance_id': request['instance_id'],
               'cwd': request['cwd'], 'profile': request['profile'], 'reasoning_effort': request['reasoning_effort'],
               'task_sha256': request['task_sha256'], 'response_sha256': communication.digest(response),
               'session_id': 'fixture-native-session', 'child_pid': 2147483646, 'returncode': 0}
        run['tool_policy_attestation'] = {
            'policy': 'none', 'version': 1, 'schema_count': 0, 'presentation': 'native', 'guard': 'deny_all',
            'plugin_sha256': policy['tool_policy_plugin_sha256'], 'overlay_sha256': policy['tool_policy_overlay_sha256'],
            'config_sha256': policy['config_sha256'], 'session_id': run['session_id']}
        if mode == 'research':
            run['tool_policy_attestation'].update(policy='research', schema_count=2,
                tools=['mcp__duckduckgo__search', 'web_fetch'], guard='allow_research_only')
        record = {**request, 'job_id': job_id, 'status': 'completed', 'callback': ipc.PROTOCOL,
                  'run_dir': str(run_dir), 'worker_pid': 2147483647}
        ipc.write_state(run_dir / 'result.json', run)
        ipc.write_state(directory / 'request.json', request)
        ipc.write_state(directory / 'receipt.json', record)
        self.backend.jobs = jobs
        return job_id, directory, request, record, run_dir, run

    @unittest.skipUnless(callable(getattr(bridge, 'tool_policy_record', None)), 'requires companion zero-tool integration')
    def test_consultation_callback_id_reads_real_verified_job_result(self):
        job_id, directory, request, record, _, _ = self.real_completed_job()
        callback = ipc.build_input(directory, request, record)
        self.assertEqual(callback['target'], self.source['target'])
        self.assertIn('consultation_result', callback['prompt'])
        self.assertNotIn(str(self.root), callback['prompt'])
        callback_job = next(line.split('：', 1)[1] for line in callback['prompt'].splitlines()
                            if line.startswith('任务编号：'))
        self.assertEqual(callback_job, job_id)
        with patch.object(jobs, 'pid_probe', return_value='absent'):
            result = self.call('consultation_result', {'job_id': callback_job})
        self.assertEqual(result, {'job_id': job_id, 'status': 'completed',
                                 'text': '实际 jobs.result 校验过的咨询结论。', 'processes_released': True})
        self.jobs.result.assert_not_called()

    def test_research_callback_uses_owned_tool_result_instead_of_host_file_paths(self):
        job_id, directory, request, record, _, _ = self.real_completed_job('research')
        callback = ipc.build_input(directory, request, record)
        self.assertEqual(callback['target'], self.source['target'])
        self.assertIn('consultation_result', callback['prompt'])
        self.assertNotIn(str(self.root), callback['prompt'])
        with patch.object(jobs, 'pid_probe', return_value='absent'):
            result = self.call('consultation_result', {'job_id': job_id})
        self.assertTrue(result['processes_released'])
        self.assertEqual(result['text'], '实际 jobs.result 校验过的咨询结论。')

    @unittest.skipUnless(callable(getattr(bridge, 'tool_policy_record', None)), 'requires companion zero-tool integration')
    def test_real_result_rejects_unproven_policy_and_modified_answer(self):
        job_id, _, _, _, run_dir, run = self.real_completed_job()
        run['tool_policy_attestation']['schema_count'] = 1
        ipc.write_state(run_dir / 'result.json', run)
        with patch.object(jobs, 'pid_probe', return_value='absent'):
            self.assertEqual(self.call('consultation_result', {'job_id': job_id}, error=True)['code'],
                             'tool_policy_unverified')
            run['tool_policy_attestation']['schema_count'] = 0
            ipc.write_state(run_dir / 'result.json', run)
            (run_dir / 'response.md').write_text('modified answer')
            self.assertEqual(self.call('consultation_result', {'job_id': job_id}, error=True)['code'],
                             'artifact_modified')


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.policy_path = self.root / 'policy.json'
        self.policy = {'scripts_dir': str(Path(peer_ipc.__file__).resolve().parent),
                       'peer_runtime': str(self.root / 'peer'), 'dsh_runtime': str(self.root / 'dsh'),
                       'work_dir': str(self.root / 'work'), 'allowed_contacts': ['atlas']}
        self.write()

    def tearDown(self):
        self.temp.cleanup()

    def write(self):
        ipc.write_state(self.policy_path, self.policy)

    def test_policy_is_private_and_has_only_allowlisted_fields(self):
        loaded = communication.load_policy(self.policy_path)
        self.assertEqual(loaded['allowed_contacts'], {'atlas': 'atlas'})
        self.assertFalse(loaded['enable_consultation'])
        self.assertEqual(loaded['contact_labels'], {})
        self.policy_path.chmod(0o644)
        with self.assertRaisesRegex(ValueError, 'record_not_private'):
            communication.load_policy(self.policy_path)
        self.write()
        self.policy['arbitrary_command'] = 'true'
        self.write()
        with self.assertRaisesRegex(ValueError, 'invalid_fields'):
            communication.load_policy(self.policy_path)

    def test_contact_labels_are_optional_bounded_and_scoped_to_allowed_aliases(self):
        self.policy['contact_labels'] = {'atlas': '本次接入迁移'}
        self.write()
        self.assertEqual(communication.load_policy(self.policy_path)['contact_labels'], self.policy['contact_labels'])
        for labels in ([], {'other': '未知成员'}, {'atlas': ''}, {'atlas': ' '}, {'atlas': ' 前导空格'},
                       {'atlas': '第一行\n第二行'}, {'atlas': 'a\x7fb'}, {'atlas': '中' * 86}, {'atlas': 5}):
            with self.subTest(labels=labels):
                self.policy['contact_labels'] = labels
                self.write()
                with self.assertRaisesRegex(ValueError, 'policy_contact_labels_invalid'):
                    communication.load_policy(self.policy_path)

    def test_policy_rejects_external_materials_and_symlinked_policy(self):
        self.policy['materials'] = {'secret': {'path': '/private/secret.txt', 'sha256': 'a' * 64}}
        self.write()
        with self.assertRaisesRegex(ValueError, 'policy_material_invalid'):
            communication.load_policy(self.policy_path)
        link = self.root / 'link.json'
        link.symlink_to(self.policy_path)
        with self.assertRaisesRegex(ValueError, 'policy_path_invalid'):
            communication.load_policy(link)

    def test_duplicate_json_fields_are_rejected(self):
        with self.assertRaisesRegex(ValueError, 'duplicate_json_field'):
            communication.decode(b'{"method":"tools/list","method":"tools/call"}')

    def test_cli_lists_tools_without_identity_or_any_socket_request(self):
        request = {'protocol': communication.PROTOCOL, 'method': 'tools/list',
                   'params': {}, 'context': {'operation_scope': 'startup'}}
        environment = {key: value for key, value in os.environ.items() if not key.startswith(('CC_', 'CODEX_THREAD_ID'))}
        result = subprocess.run([sys.executable, '-B', str(Path(communication.__file__).resolve()),
                                 '--policy', str(self.policy_path)], input=json.dumps(request),
                                text=True, capture_output=True, env=environment, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(result.stdout.splitlines()), 1)
        self.assertIn('tools', json.loads(result.stdout))


if __name__ == '__main__':
    unittest.main()
