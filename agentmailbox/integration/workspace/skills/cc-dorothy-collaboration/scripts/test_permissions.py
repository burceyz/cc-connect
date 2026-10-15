"""Real macOS file-boundary probes, without a model or network request."""
from contextlib import ExitStack
import errno
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

import dorothy
import native


WORKSPACE = Path(__file__).resolve().parents[3]
PROBE_ROOT = WORKSPACE / '.local/dorothy-worker-implementation-20261012'
DENIAL_ERRNOS = {errno.EACCES, errno.EPERM}


@unittest.skipUnless(sys.platform == 'darwin', 'requires macOS Seatbelt')
class SeatbeltPermissionTests(unittest.TestCase):
    def setUp(self):
        self.assertTrue(Path('/usr/bin/sandbox-exec').is_file(), 'Seatbelt runner is unavailable')
        PROBE_ROOT.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='permissions-', dir=PROBE_ROOT)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        # An outside-write probe in /tmp would itself be allowed by the policy.
        self.system_roots = {Path('/tmp').resolve(), Path(tempfile.gettempdir()).resolve()}
        for root in self.system_roots:
            self.assertFalse(self.root.is_relative_to(root), 'outside probe is in an allowed temp root')
        self.cwd = self.root / 'work "中文'
        self.cwd.mkdir()
        self.outside = self.root / 'outside'
        self.outside.mkdir()
        self.marker = self.outside / 'marker.txt'
        self.marker.write_text('outside-original', encoding='utf-8')
        self.assertTrue(os.access(self.marker, os.W_OK), 'host fixture must be writable before confinement')

    def run_sandbox(self, code, *arguments, input=None):
        command = ['/usr/bin/sandbox-exec', '-p', native.sandbox_profile(self.cwd), '--',
                   sys.executable, '-I', '-B', '-c', code, *(str(arg) for arg in arguments)]
        result = subprocess.run(command, cwd=self.cwd, input=input, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr.decode('utf-8', errors='replace'))
        return result

    def probe(self, code, *arguments):
        return json.loads(self.run_sandbox(code, *arguments).stdout)

    def assert_denied(self, value):
        self.assertIn(value, DENIAL_ERRNOS, 'operation must be denied by the actual sandbox')

    def test_cwd_write_and_read_succeed_with_quoted_unicode_path(self):
        path = self.cwd / 'report.txt'
        result = self.probe('''
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
path.write_text('任务资料🙂', encoding='utf-8')
print(json.dumps({'text': path.read_text(encoding='utf-8')}))
''', path)
        self.assertEqual(result['text'], '任务资料🙂')
        self.assertEqual(path.read_text(encoding='utf-8'), '任务资料🙂')

    def test_each_system_temp_area_is_writable(self):
        with ExitStack() as stack:
            for root in sorted(self.system_roots):
                with self.subTest(root=str(root)):
                    folder = stack.enter_context(tempfile.TemporaryDirectory(
                        prefix='dorothy-permission-', dir=root))
                    path = Path(folder) / 'temp-write.txt'
                    result = self.probe('''
import json, sys
from pathlib import Path
path = Path(sys.argv[1])
path.write_text('temporary-output', encoding='utf-8')
print(json.dumps({'text': path.read_text(encoding='utf-8')}))
''', path)
                    self.assertEqual(result['text'], 'temporary-output')
                    self.assertEqual(path.read_text(encoding='utf-8'), 'temporary-output')

    def test_outside_is_readable_but_existing_and_new_writes_are_denied(self):
        new_file = self.outside / 'new-file.txt'
        result = self.probe('''
import json, sys
from pathlib import Path
existing, new_file = map(Path, sys.argv[1:])
result = {'read': existing.read_text(encoding='utf-8')}
for key, path in [('overwrite_errno', existing), ('create_errno', new_file)]:
    try:
        path.write_text('escaped-write', encoding='utf-8')
        result[key] = None
    except OSError as error:
        result[key] = error.errno
print(json.dumps(result))
''', self.marker, new_file)
        self.assertEqual(result['read'], 'outside-original')
        self.assert_denied(result['overwrite_errno'])
        self.assert_denied(result['create_errno'])
        self.assertEqual(self.marker.read_text(encoding='utf-8'), 'outside-original')
        self.assertFalse(new_file.exists())

    def test_shell_and_grandchild_inherit_boundary(self):
        work_file = self.cwd / 'child-output.txt'
        result = self.probe('''
import json, subprocess, sys
work_file, outside = sys.argv[1:]
shell = subprocess.run(['/bin/sh', '-c',
    'printf child-output > "$1"; cat "$2"; if printf escaped > "$2"; then exit 19; fi',
    'permission-child', work_file, outside], capture_output=True, text=True)
grandchild = subprocess.run(['/bin/sh', '-c', 'exec "$1" -I -B -c "$2" "$3"',
    'permission-grandchild', sys.executable,
    'import json,sys; from pathlib import Path; '
    'p=Path(sys.argv[1]); print(p.read_text()); p.write_text("escaped")', outside],
    capture_output=True, text=True)
print(json.dumps({'shell_exit': shell.returncode, 'shell_read': shell.stdout,
                  'grandchild_exit': grandchild.returncode, 'grandchild_read': grandchild.stdout,
                  'grandchild_stderr': grandchild.stderr}))
''', work_file, self.marker)
        self.assertEqual(result['shell_exit'], 0)
        self.assertEqual(result['shell_read'], 'outside-original')
        self.assertEqual(work_file.read_text(encoding='utf-8'), 'child-output')
        self.assertNotEqual(result['grandchild_exit'], 0)
        self.assertEqual(result['grandchild_read'], 'outside-original\n')
        self.assertIn('PermissionError', result['grandchild_stderr'])
        self.assertEqual(self.marker.read_text(encoding='utf-8'), 'outside-original')

    def test_new_symlink_cannot_write_outside(self):
        link = self.cwd / 'outside-link'
        result = self.probe('''
import json, sys
from pathlib import Path
link, outside = map(Path, sys.argv[1:])
link.symlink_to(outside)
result = {'read': (link / 'marker.txt').read_text(encoding='utf-8')}
try:
    (link / 'marker.txt').write_text('escaped-through-symlink', encoding='utf-8')
    result['write_errno'] = None
except OSError as error:
    result['write_errno'] = error.errno
print(json.dumps(result))
''', link, self.outside)
        self.assertTrue(link.is_symlink())
        self.assertEqual(result['read'], 'outside-original')
        self.assert_denied(result['write_errno'])
        self.assertEqual(self.marker.read_text(encoding='utf-8'), 'outside-original')

    def test_new_hardlink_cannot_write_outside(self):
        link = self.cwd / 'hardlink.txt'
        result = self.probe('''
import json, os, sys
from pathlib import Path
outside, link = map(Path, sys.argv[1:])
result = {'link_created': False}
try:
    os.link(outside, link)
    result['link_created'] = True
    link.write_text('escaped-through-hardlink', encoding='utf-8')
    result['denial_errno'] = None
except OSError as error:
    result['denial_errno'] = error.errno
print(json.dumps(result))
''', self.marker, link)
        self.assert_denied(result['denial_errno'])
        self.assertEqual(self.marker.read_text(encoding='utf-8'), 'outside-original')
        if not result['link_created']:
            self.assertFalse(link.exists())

    def test_pty_shell_runs_without_expanding_regular_file_writes(self):
        work_file = self.cwd / 'pty-output.txt'
        outside_new = self.outside / 'pty-escape.txt'
        result = self.probe('''
import errno, json, os, pty, sys
work_file, outside, outside_new = sys.argv[1:]
master, slave = pty.openpty()
slave_name = os.ttyname(slave)
os.close(master)
os.close(slave)
script = ('printf pty-work-output > "$1" || exit 10; '
          'if printf escaped > "$2"; then exit 11; fi; '
          'if printf escaped > "$3"; then exit 12; fi; '
          'cat "$2" || exit 13; printf "\\npty-shell-finished\\n"')
pid, master = pty.fork()
if pid == 0:
    os.execv('/bin/sh', ['/bin/sh', '-c', script, 'pty-child', work_file, outside, outside_new])
output = bytearray()
try:
    # Drain while the PTY is live; waiting first can discard queued macOS output.
    while True:
        try:
            chunk = os.read(master, 4096)
        except OSError as error:
            if error.errno == errno.EIO:
                break
            raise
        if not chunk:
            break
        output.extend(chunk)
    _, status = os.waitpid(pid, 0)
finally:
    os.close(master)
print(json.dumps({'slave': slave_name, 'shell_exit': os.waitstatus_to_exitcode(status),
                  'output': output.decode('utf-8', errors='replace')}))
''', work_file, self.marker, outside_new)
        self.assertRegex(result['slave'], r'^/dev/ttys[0-9]+$')
        self.assertEqual(result['shell_exit'], 0, result['output'])
        self.assertIn('outside-original', result['output'])
        self.assertIn('pty-shell-finished', result['output'])
        self.assertEqual(work_file.read_text(encoding='utf-8'), 'pty-work-output')
        self.assertEqual(self.marker.read_text(encoding='utf-8'), 'outside-original')
        self.assertFalse(outside_new.exists())

    def test_pty_grant_does_not_cover_a_host_created_terminal(self):
        master, slave = os.openpty()
        self.addCleanup(os.close, master)
        self.addCleanup(os.close, slave)
        host_terminal = os.ttyname(slave)
        result = self.probe('''
import json, os, sys
master, slave = os.openpty()
try:
    result = {'own_terminal': os.ttyname(slave)}
    os.write(slave, b'own-pty')
    result['own_output'] = os.read(master, 100).decode()
    try:
        other = os.open(sys.argv[1], os.O_RDWR | os.O_NOCTTY)
    except OSError as error:
        result['other_errno'] = error.errno
    else:
        os.close(other)
        result['other_errno'] = None
finally:
    os.close(master)
    os.close(slave)
print(json.dumps(result))
''', host_terminal)
        self.assertNotEqual(result['own_terminal'], host_terminal)
        self.assertEqual(result['own_output'], 'own-pty')
        self.assert_denied(result['other_errno'])

    def test_standard_io_pipes_and_dev_null_remain_usable(self):
        content = ('管道输出🙂\n' * 10000).encode('utf-8')
        result = self.run_sandbox('''
import sys
data = sys.stdin.buffer.read()
with open('/dev/null', 'wb') as sink:
    sink.write(b'discard')
sys.stdout.buffer.write(data)
sys.stderr.write('stderr-pipe-ok')
''', input=content)
        self.assertEqual(result.stdout, content)
        self.assertEqual(result.stderr, b'stderr-pipe-ok')


class HostBoundaryTests(unittest.TestCase):
    def test_system_temp_root_cannot_cover_host_control(self):
        for root in (WORKSPACE / '.local', dorothy.SKILL, dorothy.HERMES_SKILL,
                     dorothy.HERMES_RUNTIME):
            with self.subTest(temp_root=str(root)):
                with patch.object(tempfile, 'tempdir', str(root)):
                    with self.assertRaisesRegex(ValueError, 'temp_overlaps_host_control'):
                        dorothy.task_cwd(dorothy.RUNTIME, 'permission-review', WORKSPACE / 'agent')

    def test_prepare_failure_removes_its_partial_native_directory(self):
        PROBE_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='permissions-prepare-', dir=PROBE_ROOT) as folder:
            root = Path(folder)
            work = root / 'work'
            work.mkdir()
            auth = root / 'empty-auth'
            auth.write_bytes(b'')
            store = Path(tempfile.gettempdir()).resolve() / ('dorothy-permission-' + uuid.uuid4().hex)
            self.assertFalse(store.exists())
            try:
                with self.assertRaisesRegex(ValueError, 'authentication_file_invalid'):
                    native.prepare_store(store, work, auth)
                self.assertFalse(store.exists(), 'failed preparation must not leave an unrecorded native store')
            finally:
                if store.exists():
                    shutil.rmtree(store)

    def test_prepare_does_not_remove_an_existing_native_directory(self):
        with tempfile.TemporaryDirectory(prefix='dorothy-permission-', dir=tempfile.gettempdir()) as folder:
            store = Path(folder)
            marker = store / 'owned-by-another-attempt'
            marker.write_text('preserve', encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'native_store_path_occupied'):
                native.prepare_store(store, PROBE_ROOT, PROBE_ROOT / 'unused-auth')
            self.assertEqual(marker.read_text(encoding='utf-8'), 'preserve')

    def test_shared_host_code_cannot_be_selected_as_writable_cwd(self):
        shared = WORKSPACE / 'skills/cc-dsh-collaboration'
        for path in (shared, shared / 'scripts'):
            with self.subTest(path=str(path)):
                with self.assertRaisesRegex(ValueError, 'cwd_overlaps_host_control'):
                    dorothy.task_cwd(dorothy.RUNTIME, 'permission-review', path)

    def test_archive_and_restore_preserve_native_executable(self):
        PROBE_ROOT.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix='permissions-archive-', dir=PROBE_ROOT) as folder:
            root = Path(folder)
            source = root / 'source'
            (source / 'bin').mkdir(parents=True)
            executable = source / 'bin/helper'
            executable.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
            executable.chmod(0o700)
            secret = source / 'private-state'
            secret.write_text('synthetic-native-state', encoding='utf-8')
            secret.chmod(0o600)
            native.copy_tree(source, root / 'archive')
            native.copy_tree(root / 'archive', root / 'restored')
            self.assertEqual((root / 'archive/bin/helper').stat().st_mode & 0o777, 0o700)
            self.assertEqual((root / 'restored/bin/helper').stat().st_mode & 0o777, 0o700)
            self.assertEqual((root / 'restored/private-state').stat().st_mode & 0o777, 0o600)

    def test_child_environment_excludes_atlas_handles_and_tool_credentials(self):
        values = {'PATH': '/usr/bin:/bin', 'LANG': 'en_US.UTF-8',
                  'CC_PROJECT': 'host-project', 'CC_SESSION_KEY': 'host-session',
                  'OPENAI_API_KEY': 'synthetic-api-key', 'AGY_PERMISSION_TOKEN': 'synthetic-token',
                  'DSH_HOME': '/synthetic/hermes', 'PYTHONPATH': '/synthetic/modules'}
        with patch.dict(os.environ, values, clear=True):
            result = dorothy.child_environment()
        self.assertEqual(result, {'PATH': '/usr/bin:/bin', 'LANG': 'en_US.UTF-8'})


if __name__ == '__main__':
    unittest.main()
