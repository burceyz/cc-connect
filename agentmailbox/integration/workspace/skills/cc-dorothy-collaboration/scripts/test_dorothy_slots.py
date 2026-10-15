"""Run-slot leases and legacy-record compatibility without native process inspection."""
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import dorothy


class SlotLeaseTests(unittest.TestCase):
    def setUp(self):
        # status() resolves the runtime under WORKSPACE, so keep the fixture there.
        parent = dorothy.WORKSPACE / '.local'
        parent.mkdir(parents=True, exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix='dorothy-slots-', dir=parent)
        self.root = Path(self.temp.name).resolve()
        self.runtime = self.root / 'runtime'
        self.runtime.mkdir()
        self.work = self.root / 'work'
        self.work.mkdir()
        self.fake = self.root / 'fake-dorothy'
        self.fake.write_text('#!/bin/sh\nexit 0\n', encoding='utf-8')
        self.fake.chmod(0o700)
        self.auth = self.root / 'fake-oauth-token'
        self.auth.write_text('fixture-only-not-a-real-credential', encoding='utf-8')
        dorothy.write_json(self.runtime / 'config.json',
                           {'executable': str(self.fake), 'auth_file': str(self.auth)})

    def tearDown(self):
        self.temp.cleanup()

    def test_six_slots_seventh_busy_and_released_slot_reopens(self):
        held = [dorothy.acquire_slot(self.runtime) for _ in range(dorothy.SLOTS)]
        self.assertEqual([index for index, _ in held], list(range(dorothy.SLOTS)))
        with self.assertRaisesRegex(ValueError, 'dorothy_busy'):
            dorothy.acquire_slot(self.runtime)
        held[3][1].close()
        reopened, handle = dorothy.acquire_slot(self.runtime)
        self.assertEqual(reopened, 3)
        handle.close()
        for _, item in held:
            item.close()

    def test_legacy_run_lock_occupies_only_slot_zero(self):
        with dorothy.locked(self.runtime / 'run.lock'):
            held = [dorothy.acquire_slot(self.runtime) for _ in range(dorothy.SLOTS - 1)]
            self.assertEqual([index for index, _ in held], list(range(1, dorothy.SLOTS)))
            with self.assertRaisesRegex(ValueError, 'dorothy_busy'):
                dorothy.acquire_slot(self.runtime)
            for _, item in held:
                item.close()
        index, handle = dorothy.acquire_slot(self.runtime)
        self.assertEqual(index, 0)
        handle.close()

    def test_slot_count_is_six(self):
        self.assertEqual(dorothy.SLOTS, 6)

    def test_record_without_slot_is_slot_zero(self):
        self.assertEqual(dorothy.request_slot({}), 0)
        self.assertEqual(dorothy.request_slot({'slot': 4}), 4)
        for invalid in (-1, dorothy.SLOTS, '2', 1.5, True, None):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, 'slot_invalid'):
                dorothy.request_slot({'slot': invalid})

    def test_status_checks_the_recorded_slot(self):
        dest = self.runtime / 'jobs/queued'
        dest.mkdir(parents=True)
        task = b'queued fixture task'
        (dest / 'task.md').write_bytes(task)
        dorothy.write_json(dest / 'request.json', {
            'task_sha256': dorothy.digest(task), 'cwd': str(self.work),
            'root_job': 'queued', 'created_at': dorothy.now(), 'slot': 3})
        dorothy.write_json(dest / 'state.json', {'status': 'queued'})
        with dorothy.locked(dorothy.slot_lock_path(self.runtime, 3)):
            accepting = dorothy.status('queued', self.runtime)
            self.assertEqual(accepting['status'], 'queued')
            self.assertEqual(accepting['cleanup']['worker'], 'unknown')
        abandoned = dorothy.status('queued', self.runtime)
        self.assertEqual(abandoned['status'], 'interrupted')
        self.assertEqual(abandoned['cleanup']['worker'], 'absent')

    def test_inherited_lease_must_match_the_recorded_slot(self):
        first = dorothy.slot_lock_path(self.runtime, 1)
        second = dorothy.slot_lock_path(self.runtime, 2)
        first.touch()
        second.touch()
        with first.open('r') as handle:
            dorothy.inherited_lease(handle.fileno(), first)
            with self.assertRaisesRegex(ValueError, 'worker_lease_mismatch'):
                dorothy.inherited_lease(handle.fileno(), second)

    def test_inherited_slot_lease_survives_closing_the_submitting_descriptor(self):
        index, handle = dorothy.acquire_slot(self.runtime)
        child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                 pass_fds=(handle.fileno(),))
        handle.close()
        try:
            self.assertTrue(dorothy.lease_held(dorothy.slot_lock_path(self.runtime, index)))
        finally:
            child.terminate()
            child.wait(timeout=5)
        self.assertFalse(dorothy.lease_held(dorothy.slot_lock_path(self.runtime, index)))

    def test_submit_fills_six_directories_and_rejects_the_seventh(self):
        # Replace the detached _worker with a sleeper that inherits the submitted
        # leases, so slot accounting is exercised without native process inspection.
        children = []
        original_popen = subprocess.Popen

        def sleeper(argv, *args, **kwargs):
            if '_worker' in argv:
                child = original_popen([sys.executable, '-c', 'import time; time.sleep(30)'],
                                       stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, start_new_session=True,
                                       pass_fds=kwargs.get('pass_fds', ()))
                children.append(child)
                return child
            return original_popen(argv, *args, **kwargs)

        with patch.object(dorothy.subprocess, 'Popen', side_effect=sleeper):
            try:
                for index in range(dorothy.SLOTS):
                    directory = self.work / ('dir-%d' % index)
                    directory.mkdir()
                    task = self.root / ('task-%d.md' % index)
                    task.write_text('fixture task %d' % index, encoding='utf-8')
                    dorothy.submit('job-%d' % index, task, cwd=directory, runtime=self.runtime)
                seventh = self.work / 'dir-seventh'
                seventh.mkdir()
                task = self.root / 'task-seventh.md'
                task.write_text('seventh task', encoding='utf-8')
                with self.assertRaisesRegex(ValueError, 'dorothy_busy'):
                    dorothy.submit('job-seventh', task, cwd=seventh, runtime=self.runtime)
                self.assertFalse((self.runtime / 'jobs/job-seventh').exists())
            finally:
                for child in children:
                    child.terminate()
                    child.wait(timeout=5)
        for index in range(dorothy.SLOTS):
            self.assertFalse(dorothy.lease_held(dorothy.slot_lock_path(self.runtime, index)))
        with dorothy.instances.workspace_lease(dorothy.HERMES_RUNTIME, self.work):
            pass


if __name__ == '__main__':
    unittest.main()
