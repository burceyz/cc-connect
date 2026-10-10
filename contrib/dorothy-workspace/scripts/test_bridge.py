import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import bridge


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        (self.runtime / bridge.PATCH_NAME).write_text('test config')
        self.task = self.root / 'task.md'
        self.task.write_text('讨论 $() `echo nope` 中文')

    def tearDown(self):
        self.temp.cleanup()

    def child(self, code):
        f = self.root / 'fake-dsh'
        f.write_text('#!' + sys.executable + '\n' + code)
        f.chmod(0o700)
        return str(f)

    def run_child(self, code, timeout=2):
        with redirect_stdout(io.StringIO()):
            return bridge.run(self.task, None, timeout, self.runtime, self.child(code))

    def test_projection_ignores_hidden_and_rejects_bad_events(self):
        p = bridge.Projection()
        for d in [{'type': 'thinking', 'text': 'HIDDEN'}, {'type': 'tool_result', 'result': 'SECRET'},
                  {'type': 'text', 'text': 'bounded...'}, {'type': 'session', 'sessionId': 'session-test'},
                  {'type': 'final', 'text': 'unbounded answer'}]:
            p.feed(json.dumps(d))
        self.assertEqual(p.status(0), 'completed')
        self.assertNotIn('HIDDEN', repr(vars(p)))
        p.feed('not JSON')
        self.assertEqual(p.status(0), 'invalid_event')

    def test_error_and_duplicate_final_are_not_success(self):
        for tail, status in [({'type': 'error', 'message': 'sensitive'}, 'dsh_error'),
                             ({'type': 'final', 'text': 'second'}, 'invalid_event')]:
            p = bridge.Projection()
            for d in [{'type': 'session', 'sessionId': 'session-test'}, {'type': 'final', 'text': 'ok'}, tail]:
                p.feed(json.dumps(d))
            self.assertEqual(p.status(0), status)

    def test_roundtrip_preserves_task_and_only_final(self):
        dest, status = self.run_child('import sys,json\nt=sys.stdin.read()\nprint(json.dumps({"type":"thinking","text":"HIDDEN"}))\nprint(json.dumps({"type":"session","sessionId":"session-test"}))\nprint(json.dumps({"type":"final","text":t}))\nprint("SECRET",file=sys.stderr)\n')
        self.assertEqual(status, 'completed')
        self.assertEqual((dest / 'response.md').read_bytes(), self.task.read_bytes())
        for f in dest.iterdir():
            self.assertNotIn('HIDDEN', f.read_text())
            self.assertNotIn('SECRET', f.read_text())

    def test_missing_final_and_nonzero_exit(self):
        for code, expected in [('import sys\nsys.stdin.read()\n', 'incomplete'),
                               ('import sys\nsys.stdin.read()\nsys.exit(2)\n', 'process_failed')]:
            dest, status = self.run_child(code)
            self.assertEqual(status, expected)
            self.assertFalse((dest / 'response.md').exists())

    def test_model_patch_selected_with_legacy_config_present(self):
        (self.runtime / 'happy2.patch.yml').write_text('legacy config')
        dest, status = self.run_child(
            'import sys,json,os\nsys.stdin.read()\n'
            'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
            'print(json.dumps({"type":"final","text":json.dumps({'
            '"argv":sys.argv[1:],"home":os.environ["DSH_HOME"],"cwd":os.getcwd()})}))\n')
        self.assertEqual(status, 'completed')
        actual = json.loads((dest / 'response.md').read_text())
        self.assertEqual(actual['argv'], ['headless', '--patch',
                         str(self.runtime / 'model.patch.yml'), '--json'])
        record = json.loads((dest / 'result.json').read_text())
        root = bridge.instances.root(self.runtime, record['instance_id'])
        self.assertEqual(Path(actual['home']).resolve(), (root / 'home').resolve())
        self.assertEqual(Path(actual['cwd']).resolve(), (root / 'work').resolve())
        self.assertEqual(record['config_sha256'], bridge.digest(b'test config'))

    def test_missing_model_patch_does_not_fall_back_to_happy2(self):
        (self.runtime / bridge.PATCH_NAME).unlink()
        (self.runtime / 'happy2.patch.yml').write_text('legacy config')
        with self.assertRaises(FileNotFoundError):
            bridge.run(self.task, None, 1, self.runtime, '/no/executable')
        self.assertFalse((self.runtime / 'runs').exists())

    def test_timeout_does_not_hang_on_large_stdin(self):
        self.task.write_text('x' * 200000)
        dest, status = self.run_child('import time\ntime.sleep(20)\n', timeout=0.2)
        self.assertEqual(status, 'timeout')
        self.assertFalse((dest / 'response.md').exists())

    def test_lock_prevents_parallel_write(self):
        with bridge.locked(self.runtime):
            with self.assertRaisesRegex(ValueError, 'bridge_busy'):
                with bridge.locked(self.runtime):
                    pass

    def test_changed_config_prevents_resume(self):
        dest, _ = self.run_child('import sys,json\nsys.stdin.read()\nprint(json.dumps({"type":"session","sessionId":"session-test"}))\nprint(json.dumps({"type":"final","text":"ok"}))\n')
        (self.runtime / bridge.PATCH_NAME).write_text('changed')
        with self.assertRaisesRegex(ValueError, 'config_changed'):
            bridge.run(self.task, dest, 1, self.runtime, '/no/executable')

    def test_scope_and_unowned_run_rejected(self):
        with patch.object(bridge.Path, 'cwd', return_value=self.root):
            with self.assertRaisesRegex(ValueError, 'outside_workspace'):
                bridge.scope()
        with self.assertRaisesRegex(ValueError, 'run_outside_runtime'):
            bridge.load_run(self.root, self.runtime)

    def test_binary_falls_back_to_latest_nvm_dsh_when_path_is_minimal(self):
        nvm = self.root / '.nvm' / 'versions' / 'node'
        older = nvm / 'v9.0.0' / 'bin' / 'dsh'
        newer = nvm / 'v22.20.0' / 'bin' / 'dsh'
        older.parent.mkdir(parents=True)
        newer.parent.mkdir(parents=True)
        older.write_text('')
        newer.write_text('')
        older.chmod(0o700)
        newer.chmod(0o700)
        with patch.dict(os.environ, {'PATH': '/definitely/missing'}, clear=True), \
                patch.object(bridge.Path, 'home', return_value=self.root):
            self.assertEqual(bridge.binary(), str(newer))

    def test_binary_honors_explicit_dsh_bin_failure(self):
        with patch.dict(os.environ, {'DSH_BIN': '/definitely/missing'}, clear=False):
            with self.assertRaisesRegex(ValueError, 'dsh_unavailable'):
                bridge.binary()

    def test_nvm_launcher_uses_sibling_node_with_minimal_service_path(self):
        bin_dir = self.root / '.nvm/versions/node/v22.20.0/bin'
        bin_dir.mkdir(parents=True)
        executable = bin_dir / 'dsh'
        executable.write_text('#!/usr/bin/env node\n')
        executable.chmod(0o700)
        node = bin_dir / 'node'
        node.write_text('#!' + sys.executable + '\nimport sys,json\n'
                        'sys.stdin.read()\n'
                        'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                        'print(json.dumps({"type":"final","text":"node started"}))\n')
        node.chmod(0o700)
        with patch.dict(os.environ, {'PATH': '/missing-service-path'}, clear=True), \
                patch.object(bridge.Path, 'home', return_value=self.root), \
                redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, None, 5, self.runtime)
        self.assertEqual(status, 'completed')
        self.assertEqual((dest / 'response.md').read_text(), 'node started')

    def test_resolved_npm_entry_uses_its_installation_node(self):
        prefix = self.root / '.nvm/versions/node/v22.20.0'
        bin_dir = prefix / 'bin'
        bin_dir.mkdir(parents=True)
        entry = prefix / 'lib/node_modules/@deepseek-ai/dsh/lib/bin.js'
        entry.parent.mkdir(parents=True)
        entry.write_text('#!/usr/bin/env node\n')
        entry.chmod(0o700)
        launcher = bin_dir / 'dsh'
        launcher.symlink_to(entry)
        node = bin_dir / 'node'
        node.write_text('#!' + sys.executable + '\nimport sys,json\n'
                        'sys.stdin.read()\n'
                        'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                        'print(json.dumps({"type":"final","text":"paired node started"}))\n')
        node.chmod(0o700)
        # jobs.submit persists a canonical path, so the worker receives the
        # npm entry point rather than the original bin/dsh symlink.
        accepted_executable = str(launcher.resolve())
        with patch.dict(os.environ, {'PATH': '/missing-service-path'}, clear=True), \
                redirect_stdout(io.StringIO()):
            dest, status = bridge.run(self.task, None, 5, self.runtime, accepted_executable)
        self.assertEqual(status, 'completed')
        self.assertEqual((dest / 'response.md').read_text(), 'paired node started')

    def test_npm_entry_without_paired_node_keeps_existing_path(self):
        entry = self.root / 'prefix/lib/node_modules/dsh/bin.js'
        entry.parent.mkdir(parents=True)
        entry.write_text('#!/usr/bin/env node\n')
        entry.chmod(0o700)
        with patch.dict(os.environ, {'PATH': '/existing/bin'}, clear=True):
            env = bridge.execution_env(str(entry))
        self.assertEqual(env['PATH'].split(os.pathsep), [str(entry.parent), '/existing/bin'])

    def make_export(self):
        dest = self.runtime / 'runs/test'
        dest.mkdir(parents=True)
        task, final = '问', '答'
        (dest / 'task.md').write_text(task)
        (dest / 'response.md').write_text(final)
        cwd = str((self.runtime / 'work').resolve())
        record = {'status': 'completed', 'cwd': cwd, 'session_id': 'session-test',
                  'source_before_seq': -1, 'task_sha256': bridge.digest(task.encode()),
                  'response_sha256': bridge.digest(final.encode())}
        bridge.write_json(dest / 'result.json', record)
        def user(seq, kind='user'):
            return {'type': 'user/message', 'seq': seq, 'data': {'source': {'kind': kind}, 'content': [{'type': 'text', 'text': task}]}}
        def answer(seq):
            return {'type': 'assistant/message', 'seq': seq, 'data': {'message': {'content': [{'type': 'reasoning', 'text': 'HIDDEN'}, {'type': 'text', 'text': final}], 'source': {'replayState': 'SECRET'}}}}
        rows = [{'type': 'session', 'id': 'session-test', 'cwd': cwd}, user(1, 'runtime-context'), user(2), answer(3), user(4), answer(5)]
        source = self.runtime / 'home/sessions/work/session-test/session.v4.jsonl.zstd'
        source.parent.mkdir(parents=True)
        raw = '\n'.join(json.dumps(r) for r in rows).encode()
        source.write_bytes(subprocess.run(['zstd', '-c'], input=raw, stdout=subprocess.PIPE, check=True).stdout)
        return dest, source

    def test_export_filters_and_stops_before_repeated_later_task(self):
        dest, _ = self.make_export()
        with redirect_stdout(io.StringIO()):
            manifest = bridge.export_run(dest, self.runtime)
        self.assertEqual([m['seq'] for m in manifest['messages']], [2, 3])
        text = (Path(manifest['export_dir']) / 'transcript.md').read_text()
        self.assertNotIn('HIDDEN', text)
        self.assertNotIn('SECRET', text)
        self.assertEqual(text.split('<!-- BEGIN 3:1 -->\n')[1].split('\n<!-- END 3:1 -->')[0], '答')
        with redirect_stdout(io.StringIO()):
            second = bridge.export_run(dest, self.runtime)
        self.assertNotEqual(second['export_dir'], manifest['export_dir'])
        self.assertEqual(second['transcript_sha256'], manifest['transcript_sha256'])

    def test_missing_source_is_not_claimed_complete(self):
        dest, source = self.make_export()
        source.unlink()
        with self.assertRaisesRegex(ValueError, 'source_unavailable'):
            bridge.export_run(dest, self.runtime)
        self.assertFalse((dest / 'exports').exists())

    def test_modified_artifact_rejected(self):
        dest, _ = self.make_export()
        (dest / 'response.md').write_text('changed')
        with self.assertRaisesRegex(ValueError, 'artifact_modified'):
            bridge.export_run(dest, self.runtime)

    def test_multi_text_final_exports_every_block(self):
        dest, source = self.make_export()
        rows = [json.loads(s) for s in subprocess.check_output(['zstd', '-dc', str(source)]).splitlines()]
        rows[3]['data']['message']['content'].append({'type': 'text', 'text': '收尾'})
        raw = '\n'.join(json.dumps(r) for r in rows).encode()
        source.write_bytes(subprocess.run(['zstd', '-c'], input=raw, stdout=subprocess.PIPE, check=True).stdout)
        (dest / 'response.md').write_text('答收尾')
        record = json.loads((dest / 'result.json').read_text())
        record['response_sha256'] = bridge.digest('答收尾'.encode())
        bridge.write_json(dest / 'result.json', record)
        with redirect_stdout(io.StringIO()):
            manifest = bridge.export_run(dest, self.runtime)
        self.assertEqual(len(manifest['messages']), 3)
        self.assertIn('收尾', (Path(manifest['export_dir']) / 'transcript.md').read_text())

    def test_export_write_failure_can_retry_without_model_call(self):
        dest, _ = self.make_export()
        with patch.object(bridge, 'write_json', side_effect=OSError('disk full')):
            with self.assertRaises(OSError):
                bridge.export_run(dest, self.runtime)
        self.assertEqual(list((dest / 'exports').iterdir()), [])
        with redirect_stdout(io.StringIO()):
            self.assertEqual(bridge.export_run(dest, self.runtime)['status'], 'verified')

    def test_incomplete_resume_requires_explicit_review(self):
        dest, _ = self.make_export()
        record = json.loads((dest / 'result.json').read_text())
        record['status'] = 'timeout'
        record['config_sha256'] = bridge.digest((self.runtime / bridge.PATCH_NAME).read_bytes())
        bridge.write_json(dest / 'result.json', record)
        with self.assertRaisesRegex(ValueError, 'incomplete_resume_requires_review'):
            bridge.run(self.task, dest, 1, self.runtime, '/unused')

    def test_child_keeps_lock_after_parent_closes_its_copy(self):
        with bridge.locked(self.runtime) as lock:
            proc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(10)'], pass_fds=(lock.fileno(),))
        try:
            with self.assertRaisesRegex(ValueError, 'bridge_busy'):
                with bridge.locked(self.runtime):
                    pass
        finally:
            proc.terminate()
            proc.wait(timeout=3)
        with bridge.locked(self.runtime):
            pass

    def test_doctor_cli_does_not_claim_authentication(self):
        out = io.StringIO()
        with patch.object(bridge, 'RUNTIME', self.runtime), patch.object(bridge, 'scope'), \
             patch.object(sys, 'argv', ['bridge.py', 'doctor']), \
             patch.object(bridge.shutil, 'which', return_value='/executable'), redirect_stdout(out):
            self.assertEqual(bridge.main(), 0)
        result = json.loads(out.getvalue())
        self.assertFalse(result['auth_tested'])
        self.assertFalse(result['network_tested'])

    def test_cli_rejects_resume_flag_without_prior_run(self):
        err = io.StringIO()
        with patch.object(bridge, 'scope'), patch.object(sys, 'argv',
             ['bridge.py', 'run', '--task', str(self.task), '--resume-incomplete']), \
             patch.object(sys, 'stderr', err):
            self.assertEqual(bridge.main(), 1)
        self.assertEqual(json.loads(err.getvalue()), {'status': 'failed', 'code': 'resume_required'})


if __name__ == '__main__':
    unittest.main()
