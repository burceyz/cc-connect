#!/usr/bin/env python3
"""Small deterministic probe, executed by DSH's sandboxed bash tool."""
import argparse
import errno
import hashlib
from pathlib import Path
import subprocess
import sys
import tempfile


def probe(cwd, outside, nonce):
    target = Path(cwd).resolve()
    if Path.cwd().resolve() != target:
        raise ValueError('probe_cwd_mismatch')
    blocked = Path(outside)
    if blocked.exists() or blocked.is_symlink() or blocked.resolve().is_relative_to(target):
        raise ValueError('invalid_outside_probe')
    payload = ('DSH shell probe ' + nonce).encode()
    # These files are private probes, never existing repository material.
    with tempfile.TemporaryDirectory(prefix='.dsh-shell-probe-', dir=target) as folder:
        file = Path(folder) / 'sample.txt'
        file.write_bytes(payload)
        if hashlib.sha256(file.read_bytes()).digest() != hashlib.sha256(payload).digest():
            raise ValueError('probe_roundtrip_failed')
        result = subprocess.run(
            [sys.executable, '-B', '-c',
             'import unittest\n'
             'class Check(unittest.TestCase):\n'
             ' def test_read(self):\n'
             '  from pathlib import Path\n'
             '  self.assertEqual(Path("sample.txt").read_bytes(), ' + repr(payload) + ')\n'
             'unittest.main()'], cwd=folder, capture_output=True, timeout=20)
        if result.returncode != 0:
            raise ValueError('probe_test_failed')
    with tempfile.TemporaryDirectory(prefix='dsh-shell-probe-') as folder:
        file = Path(folder) / 'sample.txt'
        file.write_bytes(payload)
        if file.read_bytes() != payload:
            raise ValueError('temp_roundtrip_failed')
    try:
        with blocked.open('xb') as file:
            file.write(payload)
    except OSError as exc:
        if exc.errno not in {errno.EPERM, errno.EACCES, errno.EROFS}:
            raise ValueError('outside_probe_inconclusive') from None
    else:
        # The parent owns this unique path and removes it after recording failure.
        raise ValueError('outside_write_was_allowed')
    print('DSH_PREFLIGHT_OK::' + nonce)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cwd', required=True)
    parser.add_argument('--outside', required=True)
    parser.add_argument('--nonce', required=True)
    args = parser.parse_args()
    probe(args.cwd, args.outside, args.nonce)
