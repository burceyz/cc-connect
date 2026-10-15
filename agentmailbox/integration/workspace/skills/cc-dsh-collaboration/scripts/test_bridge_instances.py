"""Native source routing and backwards compatibility across isolated homes."""
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import subprocess
import unittest
from unittest.mock import patch

import bridge
import instances
import test_bridge


class InstanceRoutingTests(unittest.TestCase):
    setUp = test_bridge.BridgeTests.setUp
    tearDown = test_bridge.BridgeTests.tearDown
    child = test_bridge.BridgeTests.child
    make_export = test_bridge.BridgeTests.make_export
    def test_isolated_export_during_other_instance_run(self):
        dest, source = self.make_export()
        instance = instances.allocate(self.runtime)
        root = instances.root(self.runtime, instance)
        native = root / source.relative_to(self.runtime)
        native.parent.mkdir(parents=True)
        native.write_bytes(source.read_bytes())
        record = json.loads((dest / 'result.json').read_text())
        record['instance_id'] = instance
        record['cwd'] = str(root / 'work')
        rows = [json.loads(line) for line in subprocess.check_output(['zstd', '-dc', str(native)]).splitlines()]
        rows[0]['cwd'] = record['cwd']
        native.write_bytes(subprocess.run(['zstd', '-c'], input='\n'.join(json.dumps(r) for r in rows).encode(),
                                         stdout=subprocess.PIPE, check=True).stdout)
        bridge.write_json(dest / 'result.json', record)
        other = instances.allocate(self.runtime)
        with instances.execution(self.runtime, other, instances.root(self.runtime, other) / 'work'), \
                redirect_stdout(io.StringIO()):
            exported = bridge.export_run(dest, self.runtime)
        self.assertEqual(exported['instance_id'], instance)
        with bridge.locked(root), self.assertRaisesRegex(ValueError, 'bridge_busy'):
            bridge.export_run(dest, self.runtime)

    def test_legacy_resume_stays_in_legacy_home(self):
        dest, _ = self.make_export()
        record = json.loads((dest / 'result.json').read_text())
        record['config_sha256'] = bridge.digest((self.runtime / bridge.PATCH_NAME).read_bytes())
        bridge.write_json(dest / 'result.json', record)
        before = (dest / 'result.json').read_bytes()
        code = ('import sys,json,os\nsys.stdin.read()\n'
                'print(json.dumps({"type":"session","sessionId":"session-test"}))\n'
                'print(json.dumps({"type":"final","text":os.environ["DSH_HOME"]}))\n')
        with redirect_stdout(io.StringIO()):
            resumed, status = bridge.run(self.task, dest, 3, self.runtime, self.child(code))
        self.assertEqual(status, 'completed')
        self.assertEqual(Path((resumed / 'response.md').read_text()), (self.runtime / 'home').resolve())
        self.assertNotIn('instance_id', bridge.load_run(resumed, self.runtime)[1])
        self.assertEqual((dest / 'result.json').read_bytes(), before)

    def test_wrong_resume_instance_is_rejected_before_launch(self):
        dest, _ = self.make_export()
        instance = instances.allocate(self.runtime)
        with self.assertRaisesRegex(ValueError, 'instance_mismatch'):
            bridge.run(self.task, dest, 1, self.runtime, '/unused', instance_id=instance)


if __name__ == '__main__':
    unittest.main()
