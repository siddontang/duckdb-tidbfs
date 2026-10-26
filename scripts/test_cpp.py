#!/usr/bin/env python3
"""Build and run the public C++ SDK's standalone or disposable-server tests."""
import argparse
import hashlib
import os
from pathlib import Path
import subprocess
from fetch_cpp import source

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    sdk = source()
    build = ROOT / 'build/cpp-sdk-tests' / hashlib.sha256(str(sdk).encode()).hexdigest()[:12]
    cmake = os.environ.get('CMAKE', 'cmake')
    command = [cmake, '-S', str(sdk), '-B', str(build), '-DCMAKE_BUILD_TYPE=Debug']
    if os.environ.get('OPENSSL_ROOT_DIR'):
        command += ['-DOPENSSL_ROOT_DIR=' + os.environ['OPENSSL_ROOT_DIR']]
    subprocess.run(command, check=True)
    subprocess.run([cmake, '--build', str(build), '--parallel', os.environ.get('BUILD_JOBS', '2')], check=True)
    if args.live:
        subprocess.run([str(build / 'drive9_live_test')], check=True)
    else:
        subprocess.run([os.environ.get('CTEST', 'ctest'), '--test-dir', str(build), '--output-on-failure'], check=True)


if __name__ == '__main__':
    main()
