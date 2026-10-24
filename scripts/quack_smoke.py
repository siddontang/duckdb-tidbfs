#!/usr/bin/env python3
"""Real Quack clients -> DuckDB worker -> existing extent storage.

Run through test_native_live.py --quack-only for disposable TiDB/RustFS.
Every client and worker is a separate CLI process. Clients receive only the
Quack token; filesystem credentials and tidbfs are loaded only by workers.
"""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import queue
import secrets
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid

from partition_proxy import Partition, PartitionProxy
from fault_proxy import FaultProxy

ROOT = Path(__file__).resolve().parents[1]
DUCKDB = os.environ.get('DUCKDB', 'duckdb')
ENDPOINT = os.environ['DRIVE9_BASE']
FS_TOKEN = os.environ['DRIVE9_API_KEY']
QUACK = os.environ.get('TIDBFS_QUACK_EXTENSION', 'quack')
EXTENSION = Path(os.environ.get('TIDBFS_TEST_EXTENSION', ROOT / 'build/tidbfs.duckdb_extension')).resolve()
HEARTBEAT = int(os.environ.get('TIDBFS_QUACK_HEARTBEAT_MS', '12000'))


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]


class CLI:
    """Bounded persistent CLI commands, including expected SQL failures."""
    def __init__(self, directory, name, redactions=()):
        self.redactions = (FS_TOKEN,) + tuple(redactions)
        self.log_path = Path(directory) / (name + '.stderr.log')
        self.log = self.log_path.open('w')
        self.lines = queue.Queue()
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(('DRIVE9_', 'AWS_', 'TIDBFS_FAULT_'))}
        env.update(DUCKDB_AGENT_MODE='0', TZ='UTC')
        self.process = subprocess.Popen([DUCKDB, '-unsigned', '-csv', '-noheader'],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log,
            text=True, bufsize=1, env=env)

        def receive():
            for line in self.process.stdout:
                self.lines.put(line.rstrip('\n'))
            self.lines.put(None)

        self.reader = threading.Thread(target=receive, daemon=True)
        self.reader.start()

    def redact(self, text):
        for secret in self.redactions:
            text = text.replace(secret, '<redacted>')
        return text

    def execute(self, statement, *, error=False, timeout=90):
        offset = len(self.log_path.read_text())
        marker = 'quack_test_' + uuid.uuid4().hex
        if self.process.poll() is not None:
            raise RuntimeError('DuckDB process exited: ' + self.redact(self.log_path.read_text())[-2500:])
        self.process.stdin.write(statement.rstrip('; \n') + ';\n.print ' + marker + '\n')
        self.process.stdin.flush()
        rows = []
        deadline = time.monotonic() + timeout
        while True:
            try:
                line = self.lines.get(timeout=max(0.01, deadline - time.monotonic()))
            except queue.Empty:
                raise TimeoutError('Quack CLI command timed out; inspect private build logs') from None
            if line is None:
                raise RuntimeError('DuckDB exited: ' + self.redact(self.log_path.read_text())[-2500:])
            if line == marker:
                break
            if line:
                rows.append(line)
        diagnostic = self.redact(self.log_path.read_text()[offset:])
        if error:
            if not diagnostic:
                raise AssertionError('SQL unexpectedly succeeded: ' + repr(rows))
            return diagnostic
        if diagnostic:
            raise RuntimeError(diagnostic)
        return rows

    def close(self):
        if self.process.poll() is None:
            self.process.stdin.close()
            try:
                self.process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=10)
        self.reader.join(timeout=5)
        for stream in (self.process.stdin, self.process.stdout, self.log):
            if stream and not stream.closed:
                stream.close()
        if os.environ.get('TIDBFS_TEST_LOG_DIR') and self.log_path.exists():
            destination = Path(os.environ['TIDBFS_TEST_LOG_DIR']) / self.log_path.name
            destination.write_text(self.redact(self.log_path.read_text()))

    def kill(self):
        self.process.kill()
        self.process.wait(timeout=10)


class QuackTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='tidbfs-quack-')
        self.addCleanup(self.tmp.cleanup)
        self.uri = 'tidbfs+db:///quack-' + uuid.uuid4().hex + '.duckdb'
        self.token = secrets.token_hex(24)
        self.address = 'quack:127.0.0.1:' + str(port())
        self.counter = 0

    def cli(self, role):
        self.counter += 1
        cli = CLI(self.tmp.name, role + '-' + str(self.counter), (self.token,))
        self.addCleanup(cli.close)
        return cli

    def prepare_worker(self, *, endpoint=ENDPOINT, name='worker'):
        worker = self.cli(name)
        worker.execute(f"LOAD {quote(EXTENSION)}; LOAD {quote(QUACK)}; "
            f"CREATE SECRET fs (TYPE tidbfs, ENDPOINT {quote(endpoint)}, TOKEN {quote(FS_TOKEN)}); "
            f"SET GLOBAL tidbfs_heartbeat_ms={HEARTBEAT}; SET threads=4; "
            "SET checkpoint_threshold='1GB';")
        return worker

    def attach_worker(self, worker, *, readonly=False, retry=False):
        statement = f'ATTACH {quote(self.uri)} AS shared' + (' (READ_ONLY)' if readonly else '')
        deadline = time.monotonic() + 240
        while True:
            try:
                worker.execute(statement)
                break
            except RuntimeError as exc:
                if not retry or 'locked' not in str(exc).lower() or time.monotonic() >= deadline:
                    raise
                time.sleep(0.5)
        worker.execute('USE shared;')

    def serve(self, worker, name):
        worker.execute(f"CALL quack_identify(name={quote(name)}); "
                       f"CALL quack_serve({quote(self.address)}, token={quote(self.token)});")

    def worker(self, *, endpoint=ENDPOINT, readonly=False, name='worker-A'):
        worker = self.prepare_worker(endpoint=endpoint, name=name)
        self.attach_worker(worker, readonly=readonly)
        self.serve(worker, name)
        return worker

    def client(self):
        client = self.cli('client')
        client.execute(f"LOAD {quote(QUACK)}; SET http_timeout=10; "
                       f"ATTACH {quote(self.address)} AS q (TOKEN {quote(self.token)}); CONNECT q; USE shared;")
        return client

    def verify(self, client, rows):
        expected = hashlib.md5('\n'.join(f'{i}:{hashlib.md5(str(i).encode()).hexdigest()}'
                                        for i in range(rows)).encode()).hexdigest()
        self.assertEqual(client.execute("SELECT count(*), md5(string_agg(id::VARCHAR || ':' || payload, "
                         "chr(10) ORDER BY id)) FROM ledger;"), [f'{rows},{expected}'])

    def seed(self, worker, rows=128):
        worker.execute('CREATE TABLE ledger(id BIGINT PRIMARY KEY, payload VARCHAR); '
                       f'INSERT INTO ledger SELECT i, md5(i::VARCHAR) FROM range({rows}) t(i); CHECKPOINT shared;')

    def wal_exists(self):
        path = self.uri[len('tidbfs+db://'):] + '.wal'
        request = urllib.request.Request(ENDPOINT + '/v1/fs' + path, method='HEAD',
                                         headers={'Authorization': 'Bearer ' + FS_TOKEN})
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertGreater(int(response.headers['Content-Length']), 0)

    def replacement(self, old, *, kill=False):
        new = self.prepare_worker(name='worker-B')
        with self.assertRaisesRegex(RuntimeError, 'locked'):
            self.attach_worker(new)
        if kill:
            old.kill()
        else:
            old.close()
            self.assertEqual(old.process.returncode, 0, 'clean handoff must close the old worker successfully')
        self.attach_worker(new, retry=kill)
        self.serve(new, 'worker-B')
        return new

    def test_parallel_buffer_limited_scans_and_writes(self):
        worker = self.worker()
        # 128 MiB of uncompressed columns exceeds the 64 MiB DuckDB buffer
        # pool. Every reader uses the same native database file handle.
        worker.execute("SET force_compression='uncompressed'; SET memory_limit='128MB'; "
            "CREATE TABLE scan_data AS SELECT i::BIGINT id, hash(i) a, hash(i+1) b, hash(i+2) c "
            "FROM range(4194304) t(i); CREATE TABLE commits(i INTEGER); CHECKPOINT shared;",
            timeout=300)
        scan = 'SELECT sum(id), bit_xor(a), bit_xor(b), bit_xor(c) FROM scan_data'
        expected = worker.execute(scan)
        worker.execute("SET memory_limit='64MB';")
        readers = [self.client() for _ in range(4)]
        writer = self.client()
        gate = threading.Barrier(5, timeout=60)

        def read(client):
            gate.wait()
            for _ in range(6):
                self.assertEqual(client.execute(scan, timeout=180), expected)

        def write():
            gate.wait()
            for i in range(12):
                writer.execute(f'INSERT INTO commits VALUES ({i});')

        with ThreadPoolExecutor(max_workers=5) as pool:
            tasks = [pool.submit(read, client) for client in readers] + [pool.submit(write)]
            for task in tasks:
                task.result(timeout=300)
        self.assertEqual(writer.execute('SELECT count(*), sum(i) FROM commits;'), ['12,66'])
        # Closing the old worker must release all handles after concurrent
        # scans. Reopen, checkpoint and shrink a table to exercise mutations.
        for client in readers + [writer]:
            client.close()
        replacement = self.replacement(worker)
        self.assertEqual(replacement.execute(scan), expected)
        replacement.execute('DELETE FROM commits WHERE i >= 3; CHECKPOINT shared;')
        observer = self.client()
        self.assertEqual(observer.execute('SELECT count(*), sum(i) FROM commits;'), ['3,3'])
        self.assertEqual(observer.execute(scan), expected)

    def test_concurrent_clients_commit_and_read(self):
        worker = self.worker()
        self.seed(worker, 0)
        observer = self.client()
        clients = [self.client() for _ in range(4)]
        ready = threading.Barrier(5, timeout=60)
        commit = threading.Event()

        def write(index):
            first = index * 128
            clients[index].execute('BEGIN; INSERT INTO ledger '
                f'SELECT i, md5(i::VARCHAR) FROM range({first}, {first + 128}) t(i);')
            ready.wait()
            if not commit.wait(timeout=60):
                raise TimeoutError('Concurrent COMMIT gate timed out')
            clients[index].execute('COMMIT;')

        with ThreadPoolExecutor(max_workers=4) as pool:
            futures = [pool.submit(write, i) for i in range(4)]
            try:
                ready.wait()
                self.assertEqual(observer.execute('SELECT count(*) FROM ledger;'), ['0'])
            finally:
                commit.set()
            for _ in range(8):
                count, invalid = map(int, observer.execute('SELECT count(*), '
                    'count(*) FILTER (WHERE payload != md5(id::VARCHAR)) FROM ledger;')[0].split(','))
                self.assertIn(count, (0, 128, 256, 384, 512))
                self.assertEqual(invalid, 0)
            for future in futures:
                future.result(timeout=90)
        self.verify(observer, 512)
        # This connection has neither the filesystem secret nor tidbfs loaded.
        observer.execute('DISCONNECT;')
        self.assertEqual(observer.execute("SELECT count(*) FROM duckdb_extensions() "
                                         "WHERE extension_name='tidbfs' AND loaded;"), ['0'])
        self.assertEqual(observer.execute("SELECT count(*) FROM duckdb_secrets() WHERE type='tidbfs';"), ['0'])

    def test_conflicting_writers_rollback_and_snapshot(self):
        worker = self.worker()
        worker.execute('CREATE TABLE balances(id INTEGER PRIMARY KEY, value INTEGER); '
                       'INSERT INTO balances VALUES (0,100),(1,100);')
        a, b, reader = self.client(), self.client(), self.client()
        a.execute('BEGIN; UPDATE balances SET value=value+10 WHERE id=0;')
        b.execute('BEGIN;')
        self.assertEqual(b.execute('SELECT value FROM balances WHERE id=0;'), ['100'])
        self.assertIn('conflict', b.execute('UPDATE balances SET value=999 WHERE id=0;', error=True).lower())
        b.execute('ROLLBACK;')
        a.execute('COMMIT;')
        reader.execute('BEGIN;')
        self.assertEqual(reader.execute('SELECT sum(value) FROM balances;'), ['210'])
        a.execute('BEGIN; UPDATE balances SET value=value+20 WHERE id=1; COMMIT;')
        self.assertEqual(reader.execute('SELECT sum(value) FROM balances;'), ['210'])
        reader.execute('COMMIT;')
        self.assertEqual(reader.execute('SELECT sum(value) FROM balances;'), ['230'])
        b.execute('BEGIN; DELETE FROM balances; ROLLBACK;')
        self.assertEqual(a.execute('SELECT count(*),sum(value) FROM balances;'), ['2,230'])

    def test_clean_worker_handoff_and_readonly_clients(self):
        worker = self.worker()
        self.seed(worker)
        client = self.client()
        self.verify(client, 128)
        client.close()
        new = self.replacement(worker)
        client = self.client()
        self.verify(client, 128)
        client.execute("INSERT INTO ledger VALUES (128,md5('128'));")
        client.close()
        new.execute('CHECKPOINT shared;')
        new.close()
        reader = self.worker(readonly=True, name='readonly-worker')
        a, b = self.client(), self.client()
        self.verify(a, 129)
        self.verify(b, 129)
        self.assertIn('read-only', a.execute('INSERT INTO ledger VALUES (999,\'forbidden\');', error=True).lower())

    def test_sigkill_worker_recovers_committed_not_uncommitted(self):
        worker = self.worker()
        self.seed(worker)
        committed, pending = self.client(), self.client()
        committed.execute('INSERT INTO ledger SELECT i, md5(i::VARCHAR) FROM range(128,256) t(i);')
        pending.execute("BEGIN; INSERT INTO ledger VALUES (999999,'uncommitted');")
        self.wal_exists()
        self.verify(committed, 256)
        new = self.replacement(worker, kill=True)
        self.assertTrue(committed.execute('SELECT count(*) FROM ledger;', error=True, timeout=30))
        client = self.client()
        self.assertEqual(client.execute('SELECT name FROM whoami();'), ['worker-B'])
        self.verify(client, 256)
        self.assertEqual(client.execute('SELECT count(*) FROM ledger WHERE id=999999;'), ['0'])
        client.execute("INSERT INTO ledger VALUES (256,md5('256'));")
        self.verify(client, 257)

    def test_partitioned_worker_cannot_commit_after_takeover(self):
        objects = os.environ['TIDBFS_OBJECT_ENDPOINT']
        partition = Partition()
        object_route = PartitionProxy(objects, partition, lane='objects')
        meta_route = PartitionProxy(ENDPOINT, partition, lane='metadata', object_endpoint=object_route.url)
        self.addCleanup(object_route.close)
        self.addCleanup(meta_route.close)
        worker = self.worker(endpoint=meta_route.url)
        self.seed(worker)
        client = self.client()
        client.execute("INSERT INTO ledger VALUES (128,md5('128'));")
        self.wal_exists()
        client.execute("BEGIN; INSERT INTO ledger VALUES (999999,'uncommitted');")
        if os.environ.get('TIDBFS_REQUIRE_SESSION_GUARD') == '1':
            registrations = [e for e in partition.snapshot() if e.get('op') == 'new_session']
            self.assertTrue(registrations and all(e.get('session_guard') for e in registrations))
        self.assertTrue(any(e.get('lane') == 'objects' and e.get('status') == 200
                            for e in partition.snapshot()))
        partition.cut()
        self.addCleanup(partition.restore)
        for address in (meta_route.url + '/healthz', object_route.url + '/'):
            try:
                urllib.request.urlopen(address, timeout=5).close()
            except urllib.error.HTTPError:
                self.fail('A storage route still returned an HTTP response after partition')
            except OSError:
                pass
            else:
                self.fail('A storage route was not disconnected')
        self.assertEqual(client.execute('SELECT 42;'), ['42'])
        new = self.prepare_worker(name='worker-B')
        with self.assertRaisesRegex(RuntimeError, 'locked'):
            self.attach_worker(new)
        self.attach_worker(new, retry=True)
        # A's SQL listener stays alive while only its storage routes are cut.
        self.address = 'quack:127.0.0.1:' + str(port())
        self.serve(new, 'worker-B')
        fresh = self.client()
        self.assertEqual(fresh.execute('SELECT name FROM whoami();'), ['worker-B'])
        self.verify(fresh, 129)
        fresh.execute("INSERT INTO ledger VALUES (129,md5('129'));")
        partition.restore()
        with urllib.request.urlopen(meta_route.url + '/healthz', timeout=5):
            pass
        try:
            urllib.request.urlopen(object_route.url + '/', timeout=5).close()
        except urllib.error.HTTPError as exc:
            self.assertEqual(exc.code, 403)  # unauthenticated S3, but reachable again
        self.assertIsNone(worker.process.poll())
        failure = client.execute("INSERT INTO ledger VALUES (999998,'stale'); COMMIT;", error=True)
        self.assertIn('session failed or expired', failure)
        self.verify(fresh, 130)
        self.assertEqual(fresh.execute('SELECT count(*) FROM ledger WHERE id>=999998;'), ['0'])

    def test_ambiguous_metadata_write_fails_and_reconciles(self):
        route = FaultProxy(ENDPOINT)
        self.addCleanup(route.close)
        worker = self.worker(endpoint=route.url)
        self.seed(worker)
        client = self.client()
        with route.mu:
            route.events = []
            route.rule = {'method': 'POST', 'op': 'write', 'mode': 'drop_ack', 'count': 1}
        diagnostic = client.execute("INSERT INTO ledger VALUES (128,md5('128'));", error=True)
        self.assertTrue(diagnostic)
        writes = [e for e in route.events if e['op'] == 'write']
        self.assertEqual(len(writes), 1, 'an ambiguous metadata publication must not be replayed')
        self.assertEqual(writes[0]['status'], 200)
        new = self.replacement(worker, kill=True)
        fresh = self.client()
        # A lost acknowledgement leaves an unknown transaction outcome.
        count = int(fresh.execute('SELECT count(*) FROM ledger;')[0])
        self.assertIn(count, (128, 129))
        self.verify(fresh, count)
        self.assertEqual(fresh.execute('SELECT count(*)-count(DISTINCT id) FROM ledger;'), ['0'])

    def test_transient_object_faults_through_quack(self):
        worker = self.worker()
        self.seed(worker, 0)
        client = self.client()
        for index, mode in enumerate(('unavailable', 'drop_ack')):
            request = urllib.request.Request(os.environ['TIDBFS_FAULT_PROXY'] + '/_tidbfs_fault',
                data=json.dumps({'method': 'PUT', 'mode': mode, 'count': 1}).encode(), method='POST',
                headers={'Authorization': 'Bearer ' + os.environ['TIDBFS_FAULT_TOKEN']})
            with urllib.request.urlopen(request, timeout=10):
                pass
            start = index * 4096
            client.execute('INSERT INTO ledger SELECT i, md5(i::VARCHAR) '
                           f'FROM range({start},{start + 4096}) t(i);', timeout=180)
            self.verify(client, start + 4096)
            request = urllib.request.Request(os.environ['TIDBFS_FAULT_PROXY'] + '/_tidbfs_fault',
                headers={'Authorization': 'Bearer ' + os.environ['TIDBFS_FAULT_TOKEN']})
            with urllib.request.urlopen(request, timeout=10) as response:
                events = json.load(response)['events']
            injected = [e for e in events if e['fault'] == mode]
            self.assertEqual(len(injected), 1)
            self.assertEqual(injected[0]['status'], 503 if mode == 'unavailable' else 200)


if __name__ == '__main__':
    unittest.main(verbosity=2)
