"""Regression tests for Dorothy and shared CWD validation.

Verifies:
1. Rejection of public parent containers (workdir, .worktrees, .task-clones, .local/dorothy-work, WORKSPACE).
2. Normalization and rejection of symlink aliases pointing to public containers.
3. Permitting default per-task directories, specific repos/worktrees, and independent directories.
4. Hermes read-only sources on public parent directories without acquiring directory leases.
5. Preserving directory lease mutual exclusion on real overlapping write scopes.
6. Session continuation (followup) preserving cwd, not auto-rewriting/migrating, and rejecting unsafe old cwds.
"""
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

import dorothy
import instances
import read_sources as source_policy


ENTRY = Path(dorothy.__file__).resolve()


class DorothyCwdTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='.dorothy-cwd-test-', dir=dorothy.WORKSPACE)
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        self.fake = self.root / 'fake-agy'
        self.fake.write_text('#!' + sys.executable + '\nexit 0\n', encoding='utf-8')
        self.fake.chmod(0o700)
        self.auth = self.root / 'fake-token'
        self.auth.write_text('fixture-token', encoding='utf-8')
        dorothy.write_json(self.runtime / 'config.json', {
            'executable': str(self.fake), 'auth_file': str(self.auth),
        })
        self.task_file = self.root / 'task.md'
        self.task_file.write_text('fixture task', encoding='utf-8')

        # Ensure test fixture runtime directories are ready
        dorothy.HERMES_RUNTIME.mkdir(parents=True, exist_ok=True, mode=0o700)

        # Ensure public container directories exist under workspace for testing
        self.containers = {
            'workdir': dorothy.WORKSPACE / 'workdir',
            '.worktrees': dorothy.WORKSPACE / '.worktrees',
            '.task-clones': dorothy.WORKSPACE / '.task-clones',
            '.local/dorothy-work': dorothy.WORKSPACE / '.local/dorothy-work',
        }
        self.created_dirs = []
        for name, path in self.containers.items():
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
                self.created_dirs.append(path)

    def tearDown(self):
        for path in reversed(self.created_dirs):
            if path.exists():
                try:
                    path.rmdir()
                except OSError:
                    pass
        self.temp.cleanup()

    def cli(self, action, job, *options, expected=0):
        invocation = [sys.executable, '-B', str(ENTRY), action, '--job', job,
                      '--runtime', str(self.runtime), *map(str, options)]
        completed = subprocess.run(invocation, capture_output=True, text=True, timeout=15)
        self.assertEqual(completed.returncode, expected, completed.stdout + completed.stderr)
        try:
            return json.loads(completed.stdout)
        except json.JSONDecodeError:
            self.fail('CLI returned invalid JSON: ' + completed.stdout + completed.stderr)

    def test_public_containers_rejected_as_broad_cwd(self):
        """Regression: Dorothy previously accepted workdir and .worktrees as CWD."""
        for name, target in self.containers.items():
            safe_name = name.replace('/', '-').replace('.', '')
            with self.subTest(container=name):
                with self.assertRaisesRegex(ValueError, 'cwd_too_broad'):
                    dorothy.task_cwd(self.runtime, 'job-' + safe_name, target)

                # CLI submission also rejects before creating any job or acquiring leases
                result = self.cli('submit', 'broad-' + safe_name,
                                  '--task', self.task_file, '--cwd', target, expected=1)
                self.assertEqual(result['error'], 'cwd_too_broad')
                self.assertIn('公共容器', result['message'])

        # Workspace root is rejected as well
        with self.assertRaisesRegex(ValueError, 'cwd_requires_workspace_subdirectory'):
            dorothy.task_cwd(self.runtime, 'root-job', dorothy.WORKSPACE)

    def test_symlink_aliases_to_public_containers_rejected(self):
        """Symlinks resolving to public containers must be normalized and rejected."""
        aliases = self.root / 'aliases'
        aliases.mkdir()
        for name, target in self.containers.items():
            safe_name = name.replace('/', '-').replace('.', '')
            link = aliases / ('link-' + safe_name)
            link.symlink_to(target, target_is_directory=True)
            with self.subTest(alias=name):
                with self.assertRaisesRegex(ValueError, 'cwd_too_broad'):
                    dorothy.task_cwd(self.runtime, 'alias-job', link)

                result = self.cli('submit', 'link-' + safe_name,
                                  '--task', self.task_file, '--cwd', link, expected=1)
                self.assertEqual(result['error'], 'cwd_too_broad')

        link_root = aliases / 'link-workspace-root'
        link_root.symlink_to(dorothy.WORKSPACE, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'cwd_requires_workspace_subdirectory'):
            dorothy.task_cwd(self.runtime, 'link-root-job', link_root)

    def test_independent_and_specific_task_directories_accepted(self):
        """Default task CWD, specific repo/worktree dirs, and independent dirs remain valid."""
        # 1. Default CWD: per-job directory under .local/dorothy-work/<key>
        default_dir = dorothy.task_cwd(self.runtime, 'default-job-1', None)
        self.assertEqual(default_dir, (dorothy.WORKSPACE / '.local/dorothy-work/default-job-1').resolve())
        self.assertTrue(default_dir.is_dir())
        try:
            default_dir.rmdir()
        except OSError:
            pass

        # 2. Specific project directory inside workdir (not workdir itself)
        with tempfile.TemporaryDirectory(prefix='.dorothy-test-repo-', dir=self.containers['workdir']) as temp_repo:
            specific_repo = Path(temp_repo).resolve()
            resolved = dorothy.task_cwd(self.runtime, 'repo-job', specific_repo)
            self.assertEqual(resolved, specific_repo)

        # 3. Specific worktree directory inside .worktrees (not .worktrees itself)
        with tempfile.TemporaryDirectory(prefix='.dorothy-test-wt-', dir=self.containers['.worktrees']) as temp_wt:
            specific_wt = Path(temp_wt).resolve()
            resolved = dorothy.task_cwd(self.runtime, 'wt-job', specific_wt)
            self.assertEqual(resolved, specific_wt)

        # 4. Independent test directory
        custom_dir = self.root / 'custom-independent'
        custom_dir.mkdir()
        resolved = dorothy.task_cwd(self.runtime, 'custom-job', custom_dir)
        self.assertEqual(resolved, custom_dir.resolve())

    def test_shared_read_only_source_interoperability(self):
        """Public parent directories can be used as Hermes read-only sources without locking."""
        # Hermes source policy resolves public parent directory as a read-only source
        sources = [dorothy.WORKSPACE, dorothy.WORKSPACE / 'workdir']
        resolved_sources = source_policy.resolve(sources, dorothy.WORKSPACE)
        self.assertEqual(resolved_sources, [str(dorothy.WORKSPACE.resolve()),
                                           str((dorothy.WORKSPACE / 'workdir').resolve())])

        # Read-only sources do NOT acquire directory leases in coordination
        # Verify Dorothy can acquire its directory lease on an independent directory
        independent_dir = self.root / 'independent-work'
        independent_dir.mkdir()
        with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, independent_dir):
            # Lease is held by Dorothy on independent_dir; sources are still readable
            self.assertTrue(independent_dir.is_dir())
            self.assertTrue(dorothy.WORKSPACE.is_dir())

    def test_real_write_conflict_preserved(self):
        """Ancestor/descendant and identical directory write mutual exclusion must be kept."""
        repo = self.root / 'repo-conflict-test'
        repo.mkdir()
        nested = repo / 'sub-package'
        nested.mkdir()
        sibling = self.root / 'repo-sibling'
        sibling.mkdir()

        # When a lease is held on repo:
        with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, repo):
            # 1. Identical directory conflicts
            with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, repo):
                    pass

            # 2. Nested child directory conflicts
            with self.assertRaisesRegex(ValueError, 'workspace_busy'):
                with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, nested):
                    pass

            # 3. Sibling directory does NOT conflict
            with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, sibling):
                pass

    def test_followup_preserves_cwd_and_rejects_unsafe_old_public_cwd(self):
        """Continuation keeps parent cwd, does not rewrite/relocate, and rejects broad cwd."""
        # Simulate an old completed job that was accepted with workdir before the fix
        old_job_dir = self.runtime / 'jobs/old-broad-job'
        old_job_dir.mkdir(parents=True)
        old_task = b'old broad task'
        (old_job_dir / 'task.md').write_bytes(old_task)
        old_request = {
            'version': 1, 'job_id': 'old-broad-job', 'root_job': 'old-broad-job',
            'parent_job': None, 'cwd': str((dorothy.WORKSPACE / 'workdir').resolve()),
            'task_sha256': dorothy.digest(old_task), 'timeout': 1800, 'delivery': 'manual',
            'config': dorothy.configuration(self.runtime),
            'created_at': dorothy.now(),
        }
        (old_job_dir / 'native').mkdir()
        dorothy.write_json(old_job_dir / 'request.json', old_request)
        dorothy.write_json(old_job_dir / 'state.json', {
            'status': 'completed', 'native_sha256': dorothy.tree_digest(old_job_dir / 'native'),
            'conversation_id': 'fixture-conv-1', 'updated_at': dorothy.now(),
        })
        dorothy.write_json(old_job_dir / 'session.json', {'latest_job': 'old-broad-job'})

        # Followup inheriting the broad cwd is rejected
        with self.assertRaisesRegex(ValueError, 'cwd_too_broad'):
            dorothy.task_cwd(self.runtime, 'followup-broad', None, parent_request=old_request)

        # And CLI followup also rejects and explains the reason
        result = self.cli('followup', 'followup-broad-cli', '--from-job', 'old-broad-job',
                          '--task', self.task_file, expected=1)
        self.assertEqual(result['error'], 'cwd_too_broad')
        self.assertIn('公共容器', result['message'])

        # Verify old request record was NOT rewritten or relocated
        re_read = dorothy.read_json(old_job_dir / 'request.json')
        self.assertEqual(re_read['cwd'], str((dorothy.WORKSPACE / 'workdir').resolve()))
        self.assertEqual(re_read['job_id'], 'old-broad-job')

        # Now test followup on a safe specific directory
        safe_dir = self.root / 'safe-specific-repo'
        safe_dir.mkdir()
        safe_job_dir = self.runtime / 'jobs/safe-job'
        safe_job_dir.mkdir(parents=True)
        (safe_job_dir / 'task.md').write_bytes(old_task)
        safe_request = {
            'version': 1, 'job_id': 'safe-job', 'root_job': 'safe-job',
            'parent_job': None, 'cwd': str(safe_dir.resolve()),
            'task_sha256': dorothy.digest(old_task), 'timeout': 1800, 'delivery': 'manual',
            'config': dorothy.configuration(self.runtime),
            'created_at': dorothy.now(),
        }
        dorothy.write_json(safe_job_dir / 'request.json', safe_request)

        inherited = dorothy.task_cwd(self.runtime, 'followup-safe', None, parent_request=safe_request)
        self.assertEqual(inherited, safe_dir.resolve())


if __name__ == '__main__':
    unittest.main()
