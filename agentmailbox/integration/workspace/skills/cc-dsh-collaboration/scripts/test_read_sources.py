"""Source/working-directory separation, with fake executors and no external calls."""
from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

SCRIPTS = (Path(__file__).parent if Path(__file__).parent.name == 'scripts' else
           Path(__file__).resolve().parents[2] / 'skills/cc-dsh-collaboration/scripts')
sys.path.insert(0, str(SCRIPTS))
import bridge
import instances
import jobs
import read_sources


class ReadSourceTests(unittest.TestCase):
    def setUp(self):
        base = Path(os.environ.get('CWD_TEST_ROOT', str(bridge.WORKSPACE / 'runtime/tmp')))
        base.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='read-source-', dir=base)
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('synthetic config')
        self.materials = self.root / 'materials'
        self.materials.mkdir()
        self.source = self.materials / '资料 with spaces.md'
        self.source.write_text('source-original', encoding='utf-8')
        self.task = self.root / 'task.md'
        self.task.write_text('Inspect the supplied material.', encoding='utf-8')
        self.fake = self.root / 'fake-dsh.py'
        self.fake.write_text('#!' + sys.executable + '\n'
            'import json,os,sys,time,uuid\n'
            'from pathlib import Path\n'
            'from compression import zstd\n'
            'task=sys.stdin.read()\n'
            'sid=sys.argv[sys.argv.index("--session-id")+1] if "--session-id" in sys.argv else "session-"+uuid.uuid4().hex\n'
            'print(json.dumps({"type":"session","sessionId":sid}),flush=True)\n'
            'time.sleep(0.2)\n'
            'answer=json.dumps({"cwd":os.getcwd(),"mode":os.environ.get("DSH_PERMISSION_MODE"),"prompt":task})\n'
            'native=Path(os.environ["DSH_HOME"])/"sessions"/"synthetic"/sid\n'
            'native.mkdir(parents=True,exist_ok=True)\n'
            'rows=[{"type":"session","id":sid,"cwd":os.getcwd(),"seq":0},'
            '{"type":"permission/preset","data":{"preset":"read-only"},"seq":1},'
            '{"type":"sandbox/mode","data":{"mode":"read-only"},"seq":2},'
            '{"type":"approval/policy","data":{"policy":"never"},"seq":3},'
            '{"type":"user/message","data":{"source":{"kind":"user"},"content":[{"type":"text","text":task}]},"seq":4},'
            '{"type":"assistant/message","data":{"message":{"content":[{"type":"text","text":answer}]}},"seq":5}]\n'
            '(native/"session.v4.jsonl.zstd").write_bytes(zstd.compress("\\n".join(json.dumps(r) for r in rows).encode()))\n'
            'print(json.dumps({"type":"final","text":answer}),flush=True)\n')
        self.fake.chmod(0o700)
        self.children = []
        original = subprocess.Popen

        def tracked(*args, **kwargs):
            child = original(*args, **kwargs)
            self.children.append(child)
            return child

        self.popen_patch = patch.object(subprocess, 'Popen', side_effect=tracked)
        self.popen_patch.start()

    def tearDown(self):
        try:
            for child in self.children:
                child.wait(timeout=12)
            self.temp.cleanup()
        finally:
            self.popen_patch.stop()

    def run_task(self, **kwargs):
        with redirect_stdout(io.StringIO()):
            dest, state = bridge.run(self.task, kwargs.pop('resume', None), 5,
                                     self.runtime, str(self.fake), **kwargs)
        self.assertEqual(state, 'completed')
        return dest, bridge.load_run(dest, self.runtime)[1]

    def submit(self, key, **kwargs):
        return jobs.submit(self.task, key, 5, self.runtime, str(self.fake), **kwargs)

    def wait(self, key):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            value = jobs.status(key, self.runtime)
            if value['status'] not in jobs.ACTIVE and value.get('worker_pid'):
                if os.name != 'nt':
                    try:
                        os.waitpid(value['worker_pid'], 0)
                    except ChildProcessError:
                        pass
                return value
            time.sleep(0.03)
        self.fail('worker did not finish')

    def test_source_gets_private_cwd_and_fixed_read_only_mode(self):
        with patch.dict(os.environ, {'DSH_PERMISSION_MODE': 'danger-full-access'}):
            dest, value = self.run_task(sources=[self.source])
            self.assertEqual(os.environ['DSH_PERMISSION_MODE'], 'danger-full-access')
        answer = json.loads((dest / 'response.md').read_text())
        self.assertEqual(answer['mode'], 'read-only')
        expected = instances.root(self.runtime, value['instance_id']) / 'work'
        self.assertEqual(Path(answer['cwd']), expected)
        self.assertIn(str(self.source), answer['prompt'])
        self.assertEqual((dest / 'task.md').read_bytes(), self.task.read_bytes())
        self.assertEqual(value['read_sources'], [str(self.source)])
        self.assertEqual(self.source.read_text(), 'source-original')

    def test_public_parent_can_be_a_source_but_not_an_explicit_cwd(self):
        self.assertEqual(read_sources.resolve([bridge.WORKSPACE], bridge.WORKSPACE), [str(bridge.WORKSPACE)])
        with self.assertRaisesRegex(ValueError, 'cwd_too_broad'):
            bridge.resolve_cwd(bridge.WORKSPACE, self.runtime)
        self.assertEqual(bridge.resolve_cwd(bridge.WORKSPACE, self.runtime, require_exists=False), bridge.WORKSPACE)

    def test_conflicting_or_invalid_paths_fail_before_acceptance(self):
        with self.assertRaisesRegex(ValueError, 'source_cwd_conflict'):
            self.submit('bad', sources=[self.source], cwd=self.materials)
        with self.assertRaisesRegex(ValueError, 'source_requires_private_instance'):
            self.submit('bad', sources=[self.source], instance_id=None)
        with self.assertRaisesRegex(ValueError, 'source_unavailable'):
            self.submit('bad', sources=[self.materials / 'missing'])
        with self.assertRaisesRegex(ValueError, 'source_outside_workspace'):
            self.submit('bad', sources=[Path(bridge.WORKSPACE).anchor])
        self.assertFalse((self.runtime / 'jobs').exists())
        self.assertFalse((self.runtime / 'instances').exists())

    def test_source_alias_and_duplicate_paths_normalize(self):
        alias = self.root / 'alias'
        alias.symlink_to(self.source)
        self.assertEqual(read_sources.resolve([alias, self.source], bridge.WORKSPACE), [str(self.source)])
        alias.unlink()
        alias.symlink_to(Path(bridge.WORKSPACE).anchor)
        with self.assertRaisesRegex(ValueError, 'source_outside_workspace'):
            read_sources.resolve([alias], bridge.WORKSPACE)

    def test_same_source_jobs_run_independently_and_reuse_the_original_id(self):
        first = self.submit('first', sources=[self.source])
        second = self.submit('second', sources=[self.source])
        self.assertNotEqual(first['cwd'], second['cwd'])
        with instances._workspace_lease(self.runtime, self.materials):
            results = [self.wait(key)['status'] for key in ['first', 'second']]
        self.assertEqual(results, ['completed', 'completed'])
        for key in ['first', 'second']:
            value = jobs.result(key, self.runtime)
            self.assertEqual(json.loads(value['response'])['mode'], 'read-only')
        original = (self.runtime / 'jobs/first/request.json').read_bytes()
        again = self.submit('first')
        self.assertTrue(again['reused'])
        self.assertEqual(again['cwd'], first['cwd'])
        self.assertEqual((self.runtime / 'jobs/first/request.json').read_bytes(), original)
        with self.assertRaisesRegex(ValueError, 'job_id_conflict'):
            self.submit('first', sources=[self.materials])

    def test_receipt_source_tampering_is_rejected(self):
        self.submit('tamper', sources=[self.source])
        self.assertEqual(self.wait('tamper')['status'], 'completed')
        path = self.runtime / 'jobs/tamper/receipt.json'
        value = json.loads(path.read_text())
        value['read_sources'] = [str(self.materials)]
        bridge.write_json(path, value)
        with self.assertRaisesRegex(ValueError, 'read_sources_mismatch'):
            jobs.status('tamper', self.runtime)

    def test_resume_preserves_scope_and_rejects_a_different_source(self):
        first, record = self.run_task(sources=[self.source])
        _, rows = bridge.session_rows(record['session_id'], self.runtime, record['instance_id'])
        with patch.object(bridge, 'session_rows', return_value=(None, rows)):
            _, second = self.run_task(resume=first)
            self.assertEqual(second['instance_id'], record['instance_id'])
            self.assertEqual(second['cwd'], record['cwd'])
            with self.assertRaisesRegex(ValueError, 'read_sources_mismatch'):
                self.run_task(resume=first, sources=[self.materials])

    def test_deleted_source_does_not_prevent_verified_export(self):
        dest, value = self.run_task(sources=[self.source])
        actual = (dest / 'prompt.md').read_text()
        final = (dest / 'response.md').read_text()
        native = self.root / 'native.synthetic'
        native.write_bytes(b'synthetic native record')
        rows = [{'type': 'session', 'id': value['session_id'], 'cwd': value['cwd'], 'seq': 0},
                {'type': 'user/message', 'seq': 1, 'data': {'source': {'kind': 'user'},
                 'content': [{'type': 'text', 'text': actual}]}},
                {'type': 'assistant/message', 'seq': 2, 'data': {'message': {
                 'content': [{'type': 'text', 'text': final}]}}}]
        self.source.unlink()
        with patch.object(bridge, 'session_rows', return_value=(native, rows)), redirect_stdout(io.StringIO()):
            result = bridge.export_run(dest, self.runtime)
        self.assertEqual(result['status'], 'verified')
        self.assertIn(str(self.source), (Path(result['export_dir']) / 'transcript.md').read_text())

    def test_changed_policy_is_not_silently_used_on_resume(self):
        dest, _ = self.run_task(sources=[self.source])
        different = self.root / 'policy.yml'
        different.write_text('changed policy')
        with patch.object(read_sources, 'POLICY', different), self.assertRaisesRegex(ValueError, 'read_sources_mismatch'):
            self.run_task(resume=dest)

    def test_retargeted_source_is_rejected_before_execution(self):
        accepted, _ = read_sources.binding([str(self.source)])
        self.source.unlink()
        self.source.symlink_to(self.task)
        with patch.object(bridge, 'run_process') as launch:
            with self.assertRaisesRegex(ValueError, 'read_sources_mismatch'):
                self.run_task(sources=accepted['read_sources'], expected_source_binding=accepted)
            launch.assert_not_called()
        self.assertFalse((self.runtime / 'instances').exists())

    def test_wider_native_permission_blocks_resume(self):
        first, value = self.run_task(sources=[self.source])
        _, rows = bridge.session_rows(value['session_id'], self.runtime, value['instance_id'])
        rows.append({'type': 'sandbox/mode', 'data': {'mode': 'danger-full-access'}, 'seq': 9})
        with patch.object(bridge, 'session_rows', return_value=(None, rows)), patch.object(bridge, 'run_process') as launch:
            with self.assertRaisesRegex(ValueError, 'source_permission_changed'):
                self.run_task(resume=first)
            launch.assert_not_called()

    def test_prompt_and_policy_integrity_are_required_for_result_and_export(self):
        self.submit('artifacts', sources=[self.source])
        result = self.wait('artifacts')
        self.assertEqual(result['status'], 'completed')
        dest = Path(result['run_dir'])
        for filename in ('prompt.md', 'read-sources.patch.yml'):
            target = dest / filename
            original = target.read_bytes()
            target.write_bytes(b'changed')
            with self.subTest(filename=filename):
                with self.assertRaisesRegex(ValueError, 'artifact_modified'):
                    jobs.result('artifacts', self.runtime)
                with self.assertRaisesRegex(ValueError, 'artifact_modified'), redirect_stdout(io.StringIO()):
                    bridge.export_run(dest, self.runtime)
            target.write_bytes(original)
        value = bridge.load_run(dest, self.runtime)[1]
        with self.assertRaisesRegex(ValueError, 'artifact_modified'):
            read_sources.exported_prompt(dest, b'different task', value)
        altered = value | {'read_sources': [str(self.task)]}
        with self.assertRaisesRegex(ValueError, 'artifact_modified'):
            read_sources.exported_prompt(dest, self.task.read_bytes(), altered)

    def test_missing_native_permission_is_not_verified(self):
        with self.assertRaisesRegex(ValueError, 'source_permission_unverified'):
            read_sources.native_read_only([])

    def test_malformed_binding_fails_closed(self):
        for value in [{'read_sources': [{}]}, {'read_policy_sha256': 'a' * 64},
                      {'read_sources': [], 'read_policy_sha256': 'a' * 64}]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'read_sources_invalid'):
                read_sources.record(value)


if __name__ == '__main__':
    unittest.main()
