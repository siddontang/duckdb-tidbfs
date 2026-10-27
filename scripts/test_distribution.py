#!/usr/bin/env python3
"""Verify the community-built loadable and its HTTP behavior without credentials."""
import argparse
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def dependencies(extension):
    if platform.system() == 'Darwin':
        output = subprocess.check_output(['otool', '-L', str(extension)], text=True)
        libraries = [line.strip().split(' (', 1)[0] for line in output.splitlines()[1:]]
        # otool also prints the dylib's own install name.
        libraries = [name for name in libraries if Path(name).name != extension.name]
        unexpected = [name for name in libraries if not name.startswith(('/usr/lib/', '/System/Library/'))]
    elif platform.system() == 'Linux':
        output = subprocess.check_output(['readelf', '-d', str(extension)], text=True)
        libraries = re.findall(r'\(NEEDED\).*\[(.+?)\]', output)
        allowed = re.compile(r'^(lib(c|m|pthread|dl|rt|stdc\+\+|gcc_s)\.so(?:\.[0-9]+)*|ld-linux-x86-64\.so\.2)$')
        unexpected = [name for name in libraries if not allowed.fullmatch(name)]
    else:
        raise RuntimeError('Distribution tests support Linux and macOS only')
    if unexpected:
        raise RuntimeError('Non-system runtime dependencies: ' + ', '.join(unexpected))
    print('Runtime dependency audit passed: ' + ', '.join(libraries), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, required=True)
    args = parser.parse_args()
    build = args.build_dir.resolve()
    cli = build / 'duckdb'
    extension = build / 'extension/tidbfs/tidbfs.duckdb_extension'
    if not cli.is_file() or not extension.is_file():
        raise RuntimeError('Build the DuckDB CLI and tidbfs loadable before testing publication')
    dependencies(extension)
    # Loading from an otherwise empty directory checks that no companion
    # runtime or build-tree file is needed. Force LOAD, rather than relying
    # only on SQLLogicTest's optional `require`, which can skip an extension.
    with tempfile.TemporaryDirectory(prefix='tidbfs-distribution-') as temporary:
        copied = Path(temporary) / extension.name
        shutil.copyfile(extension, copied)
        env = dict(os.environ, DUCKDB_AGENT_MODE='0', DUCKDB=str(cli), TIDBFS_TEST_EXTENSION=str(copied))
        statement = "LOAD '" + str(copied).replace("'", "''") + "'; "
        statement += "SELECT extension_version FROM duckdb_extensions() WHERE extension_name='tidbfs' AND loaded;"
        result = subprocess.run([str(cli), '-unsigned', '-bail', '-csv', '-noheader'],
                                input=statement, text=True, capture_output=True, env=env, check=True)
        if result.stdout.strip() != '0.4.0':
            raise RuntimeError('Loadable extension registration/version mismatch: ' + result.stdout)
        subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', 'test', '-v'],
                       cwd=ROOT, env=env, check=True)
    print('Community artifact load and HTTP regression tests passed', flush=True)


if __name__ == '__main__':
    main()
