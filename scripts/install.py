#!/usr/bin/env python3
"""Install the single native C++ extension, then verify LOAD."""
import argparse
import csv
import io
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def install(duckdb, directory=None):
    extension = ROOT / 'build/tidbfs.duckdb_extension'
    if not extension.is_file():
        raise RuntimeError('Build the extension with make build first')
    settings = f'SET extension_directories=[{quote(Path(directory).resolve())}]; ' if directory else ''
    command = [duckdb, '-unsigned', '-bail', '-csv', '-noheader']
    result = subprocess.run(command, input=settings + f'FORCE INSTALL {quote(extension)}; '
                            "SELECT install_path FROM duckdb_extensions() WHERE extension_name='tidbfs';",
                            text=True, capture_output=True, check=True)
    rows = list(csv.reader(io.StringIO(result.stdout)))
    if len(rows) != 1 or len(rows[0]) != 1 or not rows[0][0]:
        raise RuntimeError('DuckDB did not report an installed extension path')
    installed = Path(rows[0][0])
    subprocess.run(command, input=settings + 'LOAD tidbfs;', text=True, check=True)
    print('Installed and loaded ' + str(installed))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--duckdb', default=os.environ.get('DUCKDB', 'duckdb'))
    parser.add_argument('--directory', help='Use this DuckDB extension installation/search directory')
    args = parser.parse_args()
    install(args.duckdb, args.directory)
