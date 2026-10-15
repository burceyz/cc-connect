import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import dorothy


class CallbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dest = Path(self.temp.name) / 'job-1'
        self.dest.mkdir()
        self.request = {'task_sha256': 'b' * 64, 'callback_target': {
            'protocol': dorothy.ipc.PROTOCOL, 'socket': '/private/tmp/example.sock',
            'target': {'project': 'atlas', 'session_key': 'key', 'session_id': 's1',
                       'agent_session_id': 'original-thread', 'cwd': str(dorothy.WORKSPACE)}}}
        dorothy.write_json(self.dest / 'callback.json', {'status': 'waiting_task'})
        self.value = {'status': 'completed'}

    def tearDown(self):
        self.temp.cleanup()

    def test_lost_response_is_queried_and_never_blindly_reposted(self):
        calls = []
        def transport(path, method, route, body=None):
            calls.append((method, route, body))
            raise dorothy.ipc.DeliveryUnknown()
        with patch.object(dorothy.ipc, 'call', side_effect=transport):
            dorothy.callback_step(self.dest, self.request, self.value)
            dorothy.callback_step(self.dest, self.request, self.value)
        self.assertEqual([row[0] for row in calls], ['POST', 'GET'])
        self.assertEqual(calls[0][2]['target']['agent_session_id'], 'original-thread')
        self.assertIsNone(calls[1][2])

    def test_receipt_mismatch_stops_delivery(self):
        with patch.object(dorothy.ipc, 'call', return_value={'status': 'delivered', 'input': {}}) as call:
            first = dorothy.callback_step(self.dest, self.request, self.value)
            second = dorothy.callback_step(self.dest, self.request, self.value)
        self.assertEqual(first['status'], 'unknown')
        self.assertEqual(second['status'], 'unknown')
        self.assertEqual(call.call_count, 1)

    def test_confirmed_delivery_is_acknowledged_once(self):
        payload = None
        calls = []
        def transport(path, method, route, body=None):
            nonlocal payload
            calls.append(route)
            if route == '/ipc/event':
                payload = body
                return {'status': 'delivered', 'input': body, 'delivery_target': body['target']}
            self.assertEqual(route, '/ipc/ack')
            return {'input': payload, 'acknowledged_at': 'now'}
        with patch.object(dorothy.ipc, 'call', side_effect=transport):
            result = dorothy.callback_step(self.dest, self.request, self.value)
            dorothy.callback_step(self.dest, self.request, self.value)
        self.assertTrue(result['acknowledged'])
        self.assertEqual(calls, ['/ipc/event', '/ipc/ack'])
        self.assertNotIn('研究正文', payload.keys())

    def test_only_confirmed_unsent_receipt_can_retry(self):
        calls = []
        def transport(path, method, route, body=None):
            calls.append(method)
            return {'input': body, 'status': 'waiting', 'boot': 'a' * 32, 'code': 'turn_not_ready'}
        with patch.object(dorothy.ipc, 'call', side_effect=transport):
            dorothy.callback_step(self.dest, self.request, self.value)
            dorothy.callback_step(self.dest, self.request, self.value)
        self.assertEqual(calls, ['POST', 'POST'])

    def test_new_chat_receiver_ack_is_pinned_in_callback_receipt(self):
        actual = self.request['callback_target']['target'] | {'session_id': 's2', 'agent_session_id': 'new-thread'}
        receipt = {}
        def transport(path, method, route, body=None):
            if route == '/ipc/event':
                self.assertEqual(body['routing'], 'chat')
                receipt.update(input=body, status='delivered', delivery_target=actual)
            else:
                self.assertEqual(body['target'], actual)
                receipt['acknowledged_at'] = 'now'
            return receipt
        with patch.object(dorothy.ipc, 'call', side_effect=transport):
            result = dorothy.callback_step(self.dest, self.request, self.value)
        self.assertEqual(result['delivery_target'], actual)
        self.assertTrue(result['acknowledged'])

    def test_pre_upgrade_inflight_event_keeps_legacy_payload(self):
        event = dorothy.digest(json.dumps({'job': str(self.dest), 'task': self.request['task_sha256'],
                        'target': self.request['callback_target']['target']}, sort_keys=True).encode())
        dorothy.write_json(self.dest / 'callback.json', {'status': 'querying', 'event_id': event, 'query_only': True})
        with patch.object(dorothy.ipc, 'call', side_effect=dorothy.ipc.DeliveryUnknown) as call:
            dorothy.callback_step(self.dest, self.request, self.value)
        self.assertEqual(call.call_args.args[1], 'GET')
        self.assertNotIn('routing', dorothy.read_json(self.dest / 'callback-input.json'))


if __name__ == '__main__':
    unittest.main()
