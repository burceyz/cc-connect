"""Process-level isolation, overlap and inherited-capacity regression checks."""
from contextlib import ExitStack
import fcntl
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import unittest

import instances


class InstanceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='dsh-instances-')
        self.runtime = Path(self.temp.name).resolve() / 'runtime'
        self.runtime.mkdir()
        self.ids = [instances.allocate(self.runtime) for _ in range(instances.CAPACITY + 1)]
        self.cwds = [instances.root(self.runtime, value) / 'work' for value in self.ids]

    def tearDown(self):
        self.temp.cleanup()

    def test_auto_allocation_private_home_and_work(self):
        self.assertEqual(len(set(self.ids)), instances.CAPACITY + 1)
        value = instances.prepare(self.runtime)
        home = instances.root(self.runtime, value)
        self.assertEqual(stat.S_IMODE(home.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((home / 'home').stat().st_mode), 0o700)
        self.assertEqual(list((home / 'home').iterdir()), [])
        self.assertEqual(instances.prepare(self.runtime, value), value)

    def test_root_never_creates_and_legacy_prepare_is_explicit(self):
        missing = self.runtime / 'missing'
        self.assertEqual(instances.root(missing, None), missing)
        self.assertFalse(missing.exists())
        with self.assertRaisesRegex(ValueError, 'instance_unavailable'):
            instances.root(self.runtime, 'unknown')
        self.assertFalse((self.runtime / 'instances/unknown').exists())
        self.assertIsNone(instances.prepare(self.runtime, None))
        self.assertTrue((self.runtime / 'home').is_dir())
        self.assertTrue((self.runtime / 'work').is_dir())

    def test_deleted_work_keeps_history_addressable_without_recreating_it(self):
        value = self.ids[0]
        target = instances.root(self.runtime, value)
        (target / 'work').rmdir()
        self.assertEqual(instances.root(self.runtime, value), target)
        self.assertEqual(instances.prepare(self.runtime, value), value)
        self.assertFalse((target / 'work').exists())
        with self.assertRaisesRegex(ValueError, 'cwd_not_directory'):
            with instances.execution(self.runtime, value, target / 'work'):
                pass

    def test_invalid_ids_and_symlink_routes_are_rejected(self):
        for value in ['../other', '', '/tmp', instances.AUTO, 12, 'a' * 81]:
            with self.subTest(value=value), self.assertRaisesRegex(ValueError, 'invalid_instance_id'):
                instances.root(self.runtime, value)
        alias = self.runtime / 'instances/alias'
        alias.symlink_to(instances.root(self.runtime, self.ids[0]), target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'instance_outside_runtime'):
            instances.root(self.runtime, 'alias')
        home = instances.root(self.runtime, self.ids[0]) / 'home'
        home.rmdir()
        home.symlink_to(self.runtime, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'instance_outside_runtime'):
            instances.root(self.runtime, self.ids[0])

    def test_proxy_copy_uses_only_whitelisted_values_and_private_mode(self):
        instances.prepare(self.runtime, None)
        source = self.runtime / 'home/.env'
        source.write_text('HTTP_PROXY=http://127.0.0.1:8080\n'
                          'export HTTPS_PROXY="http://127.0.0.1:8081" # comment\n'
                          'NO_PROXY="localhost,*.example.test"\n'
                          'API_KEY=fixture-not-for-copy\n'
                          'http_proxy=http://ignored.example\n')
        (self.runtime / 'home/sessions').mkdir()
        (self.runtime / 'home/sessions/secret').write_text('not for copy')
        value = instances.allocate(self.runtime)
        home = instances.root(self.runtime, value) / 'home'
        copied = home / '.env'
        pairs = dict(line.split('=', 1) for line in copied.read_text().splitlines())
        self.assertEqual(set(pairs), instances.PROXY_KEYS)
        self.assertEqual(json.loads(pairs['HTTPS_PROXY']), 'http://127.0.0.1:8081')
        self.assertEqual(stat.S_IMODE(copied.stat().st_mode), 0o600)
        self.assertEqual([p.name for p in home.iterdir()], ['.env'])
        source.write_text('HTTPS_PROXY="unterminated\n')
        with self.assertRaisesRegex(ValueError, 'invalid_proxy_environment'):
            instances.allocate(self.runtime)
        source.unlink()
        source.symlink_to(self.runtime / 'unrelated')
        with self.assertRaisesRegex(ValueError, 'proxy_environment_symlink'):
            instances.allocate(self.runtime)

    def test_capacity_is_six(self):
        self.assertEqual(instances.CAPACITY, 6)

    def test_capacity_slots_are_independent_and_released(self):
        with ExitStack() as stack:
            for value, cwd in zip(self.ids[:instances.CAPACITY], self.cwds[:instances.CAPACITY]):
                fds = stack.enter_context(instances.execution(self.runtime, value, cwd))
                self.assertEqual(len(fds), 3)
                self.assertTrue(all(isinstance(fd, int) for fd in fds))
            with self.assertRaisesRegex(ValueError, 'capacity_busy'):
                with instances.execution(self.runtime, self.ids[instances.CAPACITY], self.cwds[instances.CAPACITY]):
                    pass
            # Export locks do not use a main-agent capacity slot.
            with instances.locked(self.runtime, self.ids[instances.CAPACITY]):
                pass
        with instances.execution(self.runtime, self.ids[instances.CAPACITY], self.cwds[instances.CAPACITY]):
            pass

    def test_same_instance_is_busy_even_with_another_workspace(self):
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]):
            with self.assertRaisesRegex(ValueError, 'bridge_busy'):
                with instances.execution(self.runtime, self.ids[0], self.cwds[1]):
                    pass
            with self.assertRaisesRegex(ValueError, 'bridge_busy'):
                with instances.locked(self.runtime, self.ids[0]):
                    pass

    def test_equal_alias_and_nested_workspaces_conflict_in_both_directions(self):
        parent = self.runtime / 'target'
        child = parent / 'child'
        child.mkdir(parents=True)
        alias = self.runtime / 'alias'
        alias.symlink_to(parent, target_is_directory=True)
        for first, second in [(parent, parent), (parent, alias), (parent, child), (child, parent)]:
            with self.subTest(first=first, second=second):
                with instances.execution(self.runtime, self.ids[0], first):
                    with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                        with instances.execution(self.runtime, self.ids[1], second):
                            pass
                with instances.execution(self.runtime, self.ids[1], second):
                    pass

    def test_failed_overlap_releases_instance_and_capacity_locks(self):
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]):
            with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                with instances.execution(self.runtime, self.ids[1], self.cwds[0]):
                    pass
            with instances.execution(self.runtime, self.ids[1], self.cwds[1]):
                with instances.execution(self.runtime, self.ids[2], self.cwds[2]):
                    pass

    def test_legacy_execution_participates_in_capacity_and_workspace_leases(self):
        instances.prepare(self.runtime, None)
        with instances.execution(self.runtime, None, self.runtime / 'work'):
            with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                with instances.execution(self.runtime, self.ids[0], self.runtime / 'work'):
                    pass
            with ExitStack() as stack:
                for value, cwd in zip(self.ids[:instances.CAPACITY - 1], self.cwds[:instances.CAPACITY - 1]):
                    stack.enter_context(instances.execution(self.runtime, value, cwd))
                with self.assertRaisesRegex(ValueError, 'capacity_busy'):
                    with instances.execution(self.runtime, self.ids[instances.CAPACITY - 1],
                                             self.cwds[instances.CAPACITY - 1]):
                        pass

    def test_child_inherits_all_leases_until_it_exits(self):
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]) as fds:
            child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], pass_fds=fds)
        try:
            with self.assertRaisesRegex(ValueError, 'bridge_busy'):
                with instances.locked(self.runtime, self.ids[0]):
                    pass
            with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                with instances.execution(self.runtime, self.ids[1], self.cwds[0]):
                    pass
            with ExitStack() as stack:
                for value, cwd in zip(self.ids[1:instances.CAPACITY], self.cwds[1:instances.CAPACITY]):
                    stack.enter_context(instances.execution(self.runtime, value, cwd))
                with self.assertRaisesRegex(ValueError, 'capacity_busy'):
                    with instances.execution(self.runtime, self.ids[instances.CAPACITY],
                                             self.cwds[instances.CAPACITY]):
                        pass
        finally:
            child.terminate()
            child.wait(timeout=3)
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]):
            pass

    def _process(self, value, cwd):
        source = ('import sys\nfrom pathlib import Path\nimport instances\n'
                  'sys.stdin.readline()\n'
                  'try:\n'
                  ' with instances.execution(Path(sys.argv[1]),sys.argv[2],Path(sys.argv[3])):\n'
                  '  print("held",flush=True)\n'
                  '  sys.stdin.readline()\n'
                  'except ValueError as exc:\n'
                  ' print(str(exc),flush=True)\n')
        return subprocess.Popen([sys.executable, '-B', '-c', source, str(self.runtime), value, str(cwd)],
                                cwd=Path(__file__).parent, stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    def test_real_processes_fill_all_capacity_slots(self):
        children = []
        try:
            for value, cwd in zip(self.ids, self.cwds):
                child = self._process(value, cwd)
                children.append(child)
                child.stdin.write('go\n')
                child.stdin.flush()
            lines = [child.stdout.readline().strip() for child in children]
            self.assertEqual(sorted(lines), ['capacity_busy'] + ['held'] * instances.CAPACITY)
        finally:
            for child in children:
                child.communicate('release\n', timeout=5)
                self.assertEqual(child.returncode, 0)

    def test_simultaneous_processes_cannot_both_claim_the_same_workspace(self):
        children = [self._process(value, self.cwds[0]) for value in self.ids[:2]]
        try:
            for child in children:
                child.stdin.write('go\n')
                child.stdin.flush()
            lines = [child.stdout.readline().strip() for child in children]
            self.assertEqual(sorted(lines), ['held', 'workspace_busy'])
        finally:
            for child in children:
                child.communicate('release\n', timeout=5)
                self.assertEqual(child.returncode, 0)

    def test_registry_is_free_while_long_execution_leases_are_held(self):
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]):
            with (self.runtime / 'coordination/registry.lock').open('r+') as registry:
                fcntl.flock(registry, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_held_invalid_workspace_record_fails_closed(self):
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]):
            lease = instances.workspace_lock_path(self.runtime, self.cwds[0])
            lease.write_text('not json')
            with self.assertRaisesRegex(ValueError, 'workspace_lease_invalid'):
                with instances.execution(self.runtime, self.ids[1], self.cwds[1]):
                    pass
        # Released stale metadata is overwritten before a lease is reused.
        with instances.execution(self.runtime, self.ids[0], self.cwds[0]):
            pass

    def test_lock_symlink_and_invalid_cwd_rejected(self):
        target = instances.root(self.runtime, self.ids[0])
        (target / 'bridge.lock').symlink_to(self.runtime / 'outside.lock')
        with self.assertRaisesRegex(ValueError, 'lock_unavailable'):
            with instances.locked(self.runtime, self.ids[0]):
                pass
        self.assertFalse((self.runtime / 'outside.lock').exists())
        for cwd, error in [('', 'invalid_cwd'), (self.runtime / 'missing', 'cwd_not_directory')]:
            with self.subTest(cwd=cwd), self.assertRaisesRegex(ValueError, error):
                with instances.execution(self.runtime, self.ids[1], cwd):
                    pass


if __name__ == '__main__':
    unittest.main()
