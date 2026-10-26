#!/usr/bin/env python3
"""Run native database tests against disposable TiDB and an updated fs binary.

Requires pymysql, matching DuckDB/extension binaries, tidb-server and the fs
server. No existing services, databases or filesystem credentials are used.
"""
import argparse
from collections import Counter
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.request
import uuid

import pymysql
from fault_proxy import FaultProxy

ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


def wait(check, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return check()
        except Exception:
            time.sleep(0.25)
    raise RuntimeError('Disposable service did not become ready; inspect build/native-live logs')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--tidb-server', required=True)
    parser.add_argument('--fs-server', required=True)
    parser.add_argument('--fs-source', help='Also run relevant Go tests in this fs checkout')
    parser.add_argument('--skip-restart', action='store_true')
    parser.add_argument('--quack', action='store_true', help='Also run the multi-client Quack integration suite')
    parser.add_argument('--quack-only', action='store_true', help='Run Quack integration instead of native_smoke.py')
    parser.add_argument('--log-dir', default=str(ROOT / 'build/native-live'))
    parser.add_argument('--rustfs-server', help='Use a disposable RustFS S3 backend and enable object fault tests (requires boto3)')
    parser.add_argument('tests', nargs='*', help='Optional unittest names from native_smoke.py')
    args = parser.parse_args()
    if (args.quack or args.quack_only) and not args.rustfs_server:
        parser.error('Quack fault tests require a disposable --rustfs-server')
    if args.quack and args.quack_only:
        parser.error('Choose --quack or --quack-only')
    processes, logs, proxies = [], [], []
    log_dir = Path(args.log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    def launch(command, name, env=None):
        log = open(log_dir / (name + '.log'), 'w')
        logs.append(log)
        child = subprocess.Popen(command, env=env, stdout=log, stderr=log)
        processes.append(child)
        return child

    with tempfile.TemporaryDirectory(prefix='tidbfs-live-') as tmp:
        sql_port, status_port, fs_port = port(), port(), port()
        try:
            launch([str(Path(args.tidb_server).resolve()), '--store=unistore', '--path=' + tmp + '/tidb',
                    '--host=127.0.0.1', '-P=' + str(sql_port), '--status=' + str(status_port),
                    '--status-host=127.0.0.1', '--log-slow-query=' + str(log_dir / 'tidb-slow.log'),
                    '-L=error'], 'tidb', dict(os.environ, TZ='UTC'))
            db = wait(lambda: pymysql.connect(host='127.0.0.1', port=sql_port, user='root'))
            suffix = uuid.uuid4().hex[:12]
            live_db, tests_db = 'tidbfs_live_' + suffix, 'tidbfs_tests_' + suffix
            with db.cursor() as cursor:
                cursor.execute('CREATE DATABASE ' + live_db)
                cursor.execute('CREATE DATABASE ' + tests_db)
            db.close()
            print('Disposable TiDB ready', flush=True)
            # Never inherit a production DRIVE9 configuration into this harness.
            env = {k: v for k, v in os.environ.items() if not k.startswith('DRIVE9_')}
            env.update(TZ='UTC', DUCKDB_AGENT_MODE='0', DRIVE9_TENANT_PROVIDER='local',
                       DRIVE9_TENANT_MAINTENANCE_INTERVAL='1s',
                       DRIVE9_LISTEN_ADDR='127.0.0.1:' + str(fs_port), DRIVE9_METRICS_LISTEN='off',
                       DRIVE9_LOCAL_DSN=f'root@tcp(127.0.0.1:{sql_port})/{live_db}?parseTime=true',
                       DRIVE9_S3_DIR=tmp + '/objects', DRIVE9_TOKEN_SIGNING_KEY=secrets.token_hex(32),
                       DRIVE9_MASTER_KEY=secrets.token_hex(32))
            if args.rustfs_server:
                import boto3
                from botocore.config import Config
                rustfs_port = port()
                access, secret = secrets.token_hex(12).upper(), secrets.token_hex(24)
                # Do not inherit an operator's storage, IAM, KMS or telemetry
                # settings into this loopback-only disposable object server.
                rustfs_env = {k: v for k, v in env.items() if not k.startswith(('RUSTFS_', 'MINIO_', 'OTEL_'))}
                rustfs_env.update(RUSTFS_ACCESS_KEY=access, RUSTFS_SECRET_KEY=secret,
                                  RUSTFS_CONSOLE_ENABLE='false', RUSTFS_REGION='us-east-1')
                Path(tmp, 'rustfs').mkdir()
                launch([str(Path(args.rustfs_server).resolve()), 'server', tmp + '/rustfs',
                        '--address', f'127.0.0.1:{rustfs_port}'], 'rustfs', rustfs_env)
                rustfs_url = f'http://127.0.0.1:{rustfs_port}'
                s3 = boto3.client('s3', endpoint_url=rustfs_url, region_name='us-east-1',
                                  aws_access_key_id=access, aws_secret_access_key=secret,
                                  config=Config(connect_timeout=2, read_timeout=2,
                                                retries={'max_attempts': 0}, s3={'addressing_style': 'path'}))
                wait(s3.list_buckets)
                s3.create_bucket(Bucket='tidbfs-test')
                proxy = FaultProxy(rustfs_url, objects=True)
                proxies.append(proxy)
                env.update(DRIVE9_S3_BUCKET='tidbfs-test', DRIVE9_S3_ENDPOINT=proxy.url,
                           DRIVE9_S3_REGION='us-east-1', DRIVE9_S3_FORCE_PATH_STYLE='true',
                           DRIVE9_S3_ACCESS_KEY_ID=access, DRIVE9_S3_SECRET_ACCESS_KEY=secret,
                           DRIVE9_EXTENT_ALLOW_STATIC_CREDENTIALS='1',
                           TIDBFS_OBJECT_ENDPOINT=proxy.url,
                           TIDBFS_FAULT_PROXY=proxy.url, TIDBFS_FAULT_TOKEN=proxy.token)
                print('Disposable RustFS S3 backend ready', flush=True)
            server_command = [str(Path(args.fs_server).resolve())]
            server = launch(server_command, 'server', env)
            endpoint = f'http://127.0.0.1:{fs_port}'

            def health():
                with urllib.request.urlopen(endpoint + '/healthz', timeout=2) as response:
                    return response.read()

            wait(health)
            with urllib.request.urlopen(urllib.request.Request(endpoint + '/v1/provision', method='POST'), timeout=90) as response:
                token = json.load(response)['api_key']

            def ready():
                request = urllib.request.Request(endpoint + '/v1/fs/', method='HEAD',
                                                  headers={'Authorization': 'Bearer ' + token})
                with urllib.request.urlopen(request, timeout=5) as response:
                    return response.read()

            wait(ready)
            if args.fs_source:
                test_env = env.copy()
                test_env['DRIVE9_TEST_TIDB_DSN'] = f'root@tcp(127.0.0.1:{sql_port})/{tests_db}?parseTime=true'
                with open(log_dir / 'go-tests.log', 'w') as output:
                    subprocess.run(['go', 'test', './pkg/datastore', './pkg/backend', './pkg/server', './pkg/tenant/schema',
                                    './cmd/drive9-server', '-p=1', '-count=1', '-run',
                                    'TestExtent|TestFileRead|TestStatForRead|TestReadPlan|TestAppendLog|Test.*IfRevision|Test.*RevisionCAS|TestSharedTiDBSchemaStatementsContainsAllTables|TestCurrentSharedTiDBSchemaVersionIsDerivedFromSharedStatements|Test.*DumpInitSQL'],
                                   cwd=args.fs_source, env=test_env, stdout=output, stderr=output, check=True)
                print('Go storage, isolation, protocol, range and schema regressions passed', flush=True)
            env.update(DRIVE9_BASE=endpoint, DRIVE9_API_KEY=token, TIDBFS_TEST_LOG_DIR=str(log_dir))
            cpp_env = env.copy()
            if os.environ.get('DRIVE9_CPP_SOURCE'):
                cpp_env['DRIVE9_CPP_SOURCE'] = os.environ['DRIVE9_CPP_SOURCE']
            subprocess.run(['python3', 'scripts/test_cpp.py', '--live'], cwd=ROOT, env=cpp_env, check=True)
            if not args.quack_only:
                subprocess.run(['python3', 'scripts/native_smoke.py'] + args.tests, cwd=ROOT, env=env, check=True)
            if args.quack or args.quack_only:
                subprocess.run(['python3', 'scripts/quack_smoke.py'] + (args.tests if args.quack_only else []),
                               cwd=ROOT, env=env, check=True)
            if not args.skip_restart and not args.quack_only:
                # Acknowledge a transaction without checkpointing, kill both the
                # writer and fs server, then restart fs over the same TiDB state.
                def quote(value):
                    return "'" + str(value).replace("'", "''") + "'"
                init = (f"LOAD {quote(ROOT / 'build/tidbfs.duckdb_extension')}; "
                        f"CREATE SECRET fs (TYPE tidbfs, ENDPOINT {quote(endpoint)}, TOKEN {quote(token)}); ")
                uri = 'tidbfs+db:///restart-' + uuid.uuid4().hex + '.duckdb'
                attach = f'ATTACH {quote(uri)} AS remote; USE remote; '
                marker = Path(tmp) / 'committed.csv'
                child = subprocess.Popen([os.environ.get('DUCKDB', 'duckdb'), '-unsigned', '-bail'],
                                         stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
                processes.append(child)
                child.stdin.write(init + attach + "SET checkpoint_threshold='1GB'; CREATE TABLE durable AS SELECT 123456 n; "
                                  + f"COPY (SELECT 1) TO {quote(marker)};\n")
                child.stdin.flush()

                def committed():
                    if child.poll() is not None:
                        raise RuntimeError('Writer exited before its durable marker')
                    return marker.read_bytes()

                wait(committed)
                child.kill()
                child.wait(timeout=15)
                child.stdin.close()
                child.stderr.close()
                server.kill()
                server.wait(timeout=15)
                server = launch(server_command, 'server-restarted', env)
                wait(health)
                wait(ready)

                def recover():
                    result = subprocess.run([os.environ.get('DUCKDB', 'duckdb'), '-unsigned', '-bail', '-csv', '-noheader'],
                        input=init + attach + 'SELECT n FROM durable; CHECKPOINT;', text=True, capture_output=True, timeout=60)
                    if result.returncode:
                        raise RuntimeError('Waiting for crashed extent session to expire')
                    if not result.stdout.strip().endswith('123456'):
                        raise AssertionError('Committed value did not survive server restart')
                    return True

                wait(recover, timeout=150)
                print('Client + fs SIGKILL, server restart and WAL recovery passed', flush=True)
        finally:
            # Keep credential-free fault summaries even when a test fails.
            for index, proxy in enumerate(proxies):
                with proxy.mu:
                    counts = Counter((event.get('method'), event.get('op'), event.get('status'),
                                      event.get('fault')) for event in proxy.events)
                (log_dir / f'proxy-{index}-summary.json').write_text(json.dumps([
                    {'method': key[0], 'op': key[1], 'status': key[2], 'fault': key[3], 'count': count}
                    for key, count in counts.items()], indent=2) + '\n')
            for child in reversed(processes):
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
            for log in logs:
                log.close()
            for proxy in proxies:
                proxy.close()


if __name__ == '__main__':
    main()
