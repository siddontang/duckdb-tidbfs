#!/usr/bin/env python3
"""Fetch only the pinned public C++ SDK sources; no Go sources are built."""
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
REVISION = (ROOT / 'cpp-sdk-revision.txt').read_text().strip()


def source():
    override = os.environ.get('DRIVE9_CPP_SOURCE')
    if override:
        path = Path(override).resolve()
    else:
        checkout = ROOT / 'build/cpp-sdk' / REVISION
        if not (checkout / '.git').exists():
            checkout.mkdir(parents=True, exist_ok=True)
            subprocess.run(['git', 'init', '-q', str(checkout)], check=True)
            subprocess.run(['git', '-C', str(checkout), 'remote', 'add', 'origin',
                            'https://github.com/mem9-ai/drive9.git'], check=True)
            subprocess.run(['git', '-C', str(checkout), 'sparse-checkout', 'set', 'clients/drive9-cpp'], check=True)
        head = subprocess.run(['git', '-C', str(checkout), 'rev-parse', '--verify', 'HEAD'],
                              capture_output=True, text=True)
        if head.returncode or head.stdout.strip() != REVISION:
            subprocess.run(['git', '-C', str(checkout), 'fetch', '--depth=1', '--filter=blob:none',
                            'origin', REVISION], check=True)
            subprocess.run(['git', '-C', str(checkout), 'checkout', '--detach', REVISION], check=True)
        dirty = subprocess.check_output(['git', '-C', str(checkout), 'status', '--porcelain'], text=True)
        if dirty:
            raise RuntimeError('Pinned SDK checkout has local changes; use DRIVE9_CPP_SOURCE for development')
        path = checkout / 'clients/drive9-cpp'
    if not (path / 'include/drive9/client.hpp').is_file():
        raise RuntimeError('C++ SDK source is incomplete')
    return path


if __name__ == '__main__':
    print(source())
