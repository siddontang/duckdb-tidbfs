#!/usr/bin/env python3
"""Live conformance tests; uses the existing extent API and TiDB.

Uses fresh database names. Never runs against or modifies an existing database.
The crash/heartbeat checks deliberately span the 60-second extent session.
"""
import os
import json
import http.server
import socket
import threading
import urllib.request
import urllib.error
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
import uuid
import signal
from fault_proxy import FaultProxy

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = os.environ['DRIVE9_BASE']
TOKEN = os.environ['DRIVE9_API_KEY']
DUCKDB = os.environ.get('DUCKDB', 'duckdb')


def quote(s):
    return "'" + str(s).replace("'", "''") + "'"


INIT = (f"LOAD {quote(ROOT / 'build/tidbfs.duckdb_extension')}; "
        f"CREATE SECRET fs (TYPE tidbfs, ENDPOINT {quote(ENDPOINT)}, TOKEN {quote(TOKEN)}); ")


def sql(statement, fail=False, endpoint=ENDPOINT, timeout=180, extension=None):
    init = INIT if extension is None else INIT.replace(quote(ROOT / 'build/tidbfs.duckdb_extension'), quote(extension))
    result = subprocess.run([DUCKDB, '-unsigned', '-bail', '-csv', '-noheader'],
                            input=init.replace(quote(ENDPOINT), quote(endpoint)) + statement, text=True, capture_output=True, timeout=timeout)
    error = result.stderr.replace(TOKEN, '<redacted>')
    if fail:
        if not result.returncode:
            raise AssertionError('Statement unexpectedly succeeded')
        return error
    if result.returncode:
        raise RuntimeError(error)
    return result.stdout.strip()


class NativeDatabaseTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('TIDBFS_INTEROP_EXTENSION'), 'requires an independently built prior client extension')
    def test_prior_client_file_format_interoperability(self):
        prior = Path(os.environ['TIDBFS_INTEROP_EXTENSION']).resolve()
        sql(self.attach + 'CREATE TABLE interop AS SELECT i, i*3 AS v FROM range(100000) t(i); CHECKPOINT;', extension=prior)
        self.assertEqual(sql(self.attach + 'SELECT sum(v) FROM interop;').splitlines()[-1], '14999850000')
        sql(self.attach + 'UPDATE interop SET v=v+1 WHERE i<100; INSERT INTO interop VALUES (100000, 7); CHECKPOINT;')
        self.assertEqual(sql(self.readonly + 'SELECT count(*), sum(v) FROM interop;', extension=prior).splitlines()[-1], '100001,14999850107')
        # Force a fresh prior-client reservation AFTER C++ publishes objects.
        # Its writes must not reuse C++ slice IDs or overwrite immutable blocks.
        sql(self.attach + 'INSERT INTO interop VALUES (100001, 11); CHECKPOINT;', extension=prior)
        self.assertEqual(sql(self.readonly + 'SELECT count(*), sum(v) FROM interop;').splitlines()[-1], '100002,14999850118')
        parquet = self.uri + '.parquet'
        sql(f"COPY (SELECT i FROM range(8192) t(i)) TO {quote(parquet)} (FORMAT PARQUET);")
        # Keep the C++ Parquet inode live while the prior client creates a new
        # inode, catching an inclusive/exclusive inode-counter mismatch too.
        other = self.uri + '.prior.parquet'
        sql(f"COPY (SELECT i+100 AS i FROM range(1024) t(i)) TO {quote(other)} (FORMAT PARQUET);", extension=prior)
        self.assertEqual(sql(f"SELECT count(*), sum(i) FROM read_parquet({quote(parquet.replace('tidbfs+db:', 'tidbfs:'))});", extension=prior).splitlines()[-1], '8192,33550336')
        self.assertEqual(sql(f"SELECT count(*), sum(i) FROM read_parquet({quote(other.replace('tidbfs+db:', 'tidbfs:'))});").splitlines()[-1], '1024,626176')

    def setUp(self):
        self.uri = 'tidbfs+db:///conformance-' + uuid.uuid4().hex + '.duckdb'
        self.attach = f'ATTACH {quote(self.uri)} AS remote; USE remote; '
        self.readonly = f'ATTACH {quote(self.uri)} AS remote (READ_ONLY); USE remote; '
        self.tmp = tempfile.TemporaryDirectory(prefix='tidbfs-native-test-')
        self.addCleanup(self.tmp.cleanup)
        self.children = []
        self.addCleanup(self.stop_children)

    def stop_children(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=15)
            for stream in (child.stdin, child.stdout, child.stderr):
                if stream:
                    stream.close()

    def hold(self, statement, *, bail=True, endpoint=ENDPOINT):
        marker = Path(self.tmp.name) / (uuid.uuid4().hex + '.csv')
        child = subprocess.Popen([DUCKDB, '-unsigned'] + (['-bail'] if bail else []), stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.children.append(child)
        child.stdin.write(INIT.replace(quote(ENDPOINT), quote(endpoint)) + statement +
                          f"\nCOPY (SELECT 1) TO {quote(marker)};\n")
        child.stdin.flush()
        until = time.monotonic() + 120
        while not marker.exists():
            if child.poll() is not None:
                raise RuntimeError(child.stderr.read().replace(TOKEN, '<redacted>'))
            if time.monotonic() > until:
                raise RuntimeError('Database process did not reach its durable marker')
            time.sleep(0.05)
        return child

    def finish(self, child):
        child.stdin.close()
        self.assertEqual(child.wait(timeout=120), 0,
                         child.stderr.read().replace(TOKEN, '<redacted>'))

    def recover(self, statement):
        deadline = time.monotonic() + 150
        while True:
            try:
                return sql(statement)
            except RuntimeError as error:
                if 'locked' not in str(error) or time.monotonic() >= deadline:
                    raise
                time.sleep(1)

    def test_catalog_transactions_checkpoint_reopen(self):
        sql(self.attach + """
            CREATE SCHEMA app;
            CREATE SEQUENCE app.ids START 10001;
            CREATE TABLE app.items(id INTEGER PRIMARY KEY, value VARCHAR CHECK(length(value)>0));
            INSERT INTO app.items SELECT i, md5(i::VARCHAR) FROM range(10000) t(i);
            CREATE INDEX value_idx ON app.items(value);
            CREATE VIEW app.summary AS SELECT count(*) n, sum(id) total FROM app.items;
            CREATE MACRO app.twice(x) AS x*2;
            ALTER TABLE app.items ADD COLUMN active BOOLEAN DEFAULT true;
            BEGIN; UPDATE app.items SET id=id+20000; DELETE FROM app.items WHERE id%2=0; ROLLBACK;
            BEGIN; INSERT INTO app.items VALUES (nextval('app.ids'),'committed',true); COMMIT;
            ANALYZE; CHECKPOINT; VACUUM;
        """)
        self.assertTrue(sql(self.readonly + "SELECT n,total,app.twice(21) FROM app.summary;").endswith('10001,50005001,42'))
        sql(self.attach + "BEGIN; DELETE FROM app.items WHERE id=0; UPDATE app.items SET value='updated' WHERE id=1; COMMIT; CHECKPOINT;")
        self.assertTrue(sql(self.readonly + "SELECT count(*),sum(id),count(*) FILTER(WHERE value='updated') FROM app.items;").endswith('10000,50005001,1'))

    def test_multiple_databases_and_arbitrary_filename(self):
        other = self.uri.replace('.duckdb', '.bin')
        result = sql(self.attach + f"ATTACH {quote(other)} AS other; "
                     "CREATE TABLE remote.a AS SELECT 42 n; "
                     "CREATE TABLE other.b AS SELECT 2 n; "
                     "CHECKPOINT remote; CHECKPOINT other; "
                     "SELECT a.n+b.n FROM remote.a a, other.b b; DETACH other;")
        self.assertTrue(result.endswith('44'), result)
        self.assertTrue(sql(f"ATTACH {quote(other)} AS other (READ_ONLY); SELECT n FROM other.b;").endswith('2'))

    def test_wal_recovers_committed_not_uncommitted_after_sigkill(self):
        child = self.hold(self.attach + """
            SET checkpoint_threshold='1GB';
            CREATE TABLE recovered(id INTEGER);
            BEGIN; INSERT INTO recovered SELECT i FROM range(10000) t(i); COMMIT;
            BEGIN; INSERT INTO recovered VALUES (999999);
        """)
        child.kill()
        child.wait(timeout=15)
        # A crashed process cannot release its lease. Let the actual server
        # clock expire it, without touching any metadata directly.
        result = self.recover(self.attach + 'SELECT count(*),sum(id) FROM recovered; CHECKPOINT;')
        self.assertTrue(result.endswith('10000,49995000'), result)
        self.assertTrue(sql(self.readonly + 'SELECT max(id) FROM recovered;').endswith('9999'))

    def test_writer_exclusion_renewal_and_shared_readers(self):
        writer = self.hold(self.attach + 'CREATE TABLE t AS SELECT 42 AS answer; CHECKPOINT;')
        self.assertIn('locked', sql(self.attach + 'SELECT * FROM t;', fail=True))
        self.assertIn('locked', sql(self.readonly + 'SELECT * FROM t;', fail=True))
        # More than one full lease: the heartbeat must keep the writer alive.
        time.sleep(65)
        self.assertIn('locked', sql(self.attach + 'SELECT * FROM t;', fail=True))
        self.finish(writer)
        reader1 = self.hold(self.readonly + 'SELECT * FROM t;')
        reader2 = self.hold(self.readonly + 'SELECT * FROM t;')
        self.assertIn('locked', sql(self.attach + 'SELECT * FROM t;', fail=True))
        self.finish(reader1)
        self.finish(reader2)
        self.assertTrue(sql(self.attach + 'SELECT * FROM t;').endswith('42'))

    def test_lost_write_ack_is_not_retried_and_database_recovers(self):
        sql(self.attach + 'CREATE TABLE uncertain(id INTEGER); CHECKPOINT;')
        writes = []

        class DropAck(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_HEAD(self):
                self.do_POST()

            def do_POST(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
                op = self.headers.get('X-Drive9-Extent-Op')
                request = urllib.request.Request(ENDPOINT + self.path, data=body if body else None, method=self.command,
                    headers={'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json',
                             'X-Drive9-Extent-Op': op or ''})
                try:
                    response = urllib.request.urlopen(request, timeout=120)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    data, code = response.read(), response.code
                if op == 'write':
                    writes.append(code)
                    if len(writes) == 1:
                        # The server committed and replied; the client never
                        # receives the acknowledgement. Never replay this PUT.
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        proxy = http.server.ThreadingHTTPServer(('127.0.0.1', 0), DropAck)
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        try:
            error = sql(self.attach + 'INSERT INTO uncertain VALUES (7);', fail=True,
                        endpoint='http://127.0.0.1:' + str(proxy.server_port))
            self.assertIn('Error', error)
            self.assertEqual(writes, [200], 'Ambiguous write was replayed')
        finally:
            proxy.shutdown()
            proxy.server_close()
            thread.join()
        # Either transaction outcome is legal after lost acknowledgement;
        # duplication or corruption is not. The application must reconcile.
        result = self.recover(self.attach + 'SELECT count(*) FROM uncertain; CHECKPOINT;')
        self.assertIn(result.splitlines()[-1], ('0', '1'))

    def test_expired_paused_writer_cannot_resume_writes(self):
        writer = self.hold(self.attach + 'CREATE TABLE paused AS SELECT 1 n; CHECKPOINT;')
        writer.send_signal(signal.SIGSTOP)
        try:
            # This is an actual missed-heartbeat interval, without editing DB
            # rows or adding a server fault-injection endpoint.
            time.sleep(65)
            self.recover(self.attach + 'INSERT INTO paused VALUES (2); CHECKPOINT;')
        finally:
            writer.send_signal(signal.SIGCONT)
        writer.stdin.write('INSERT INTO paused VALUES (99);\n')
        writer.stdin.flush()
        writer.stdin.close()
        self.assertNotEqual(writer.wait(timeout=90), 0)
        self.assertIn('session failed or expired', writer.stderr.read())
        self.assertTrue(sql(self.readonly + 'SELECT sum(n) FROM paused;').endswith('3'))

    def test_heartbeat_is_fixed_for_live_extent_client(self):
        error = sql(self.attach + 'CREATE TABLE heartbeat_config AS SELECT 42 n; CHECKPOINT; '
                    'SET GLOBAL tidbfs_heartbeat_ms=2000; '
                    f'ATTACH {quote(self.uri + ".other")} AS other;', fail=True)
        self.assertIn('fresh DuckDB instance', error)
        self.assertTrue(sql(self.readonly + 'SELECT n FROM heartbeat_config;').endswith('42'))

    def test_heartbeat_is_fixed_across_uri_routes(self):
        parquet = self.uri + '.parquet'
        ordinary = parquet.replace('tidbfs+db://', 'tidbfs://', 1)
        sql(f"COPY (SELECT 42 n) TO {quote(parquet)} (FORMAT PARQUET);")
        for first, second in ((ordinary, parquet), (parquet, ordinary)):
            with self.subTest(first=first):
                error = sql(f'SELECT n FROM read_parquet({quote(first)}); '
                            'SET GLOBAL tidbfs_heartbeat_ms=20000; '
                            f'SELECT n FROM read_parquet({quote(second)});', fail=True)
                self.assertIn('fresh DuckDB instance', error)
        error = sql(f'SELECT n FROM read_parquet({quote(ordinary)}); '
                    'SET GLOBAL tidbfs_heartbeat_ms=20000; ' + self.attach, fail=True)
        self.assertIn('fresh DuckDB instance', error)

    def test_uri_routes_share_one_extent_session(self):
        parquet = self.uri + '.parquet'
        ordinary = parquet.replace('tidbfs+db://', 'tidbfs://', 1)
        sql(f"COPY (SELECT 42 n) TO {quote(parquet)} (FORMAT PARQUET);")
        for first, second in ((ordinary, parquet), (parquet, ordinary)):
            with self.subTest(first=first):
                proxy = FaultProxy(ENDPOINT)
                try:
                    attach = f'ATTACH {quote(self.uri + uuid.uuid4().hex)} AS remote; USE remote; '
                    result = sql(f'SELECT n FROM read_parquet({quote(first)}); '
                                 f'SELECT n FROM read_parquet({quote(second)}); ' + attach +
                                 'SELECT 43; USE memory; DETACH remote;', endpoint=proxy.url)
                    self.assertTrue(result.endswith('43'), result)
                    with proxy.mu:
                        registrations = [e for e in proxy.events if e['op'] == 'new_session']
                    self.assertEqual(len(registrations), 1, 'URI routes created separate extent sessions')
                finally:
                    proxy.close()

    def test_analytical_extent_scans_exclude_cross_route_writers(self):
        rows = 131072
        replacement = ('COPY (SELECT 7 i, repeat(md5(7::VARCHAR),2) payload) TO {} '
                       '(FORMAT PARQUET, USE_TMP_FILE false);')
        for direct in (False, True):
            parquet = self.uri + uuid.uuid4().hex + '.parquet'
            ordinary = parquet.replace('tidbfs+db://', 'tidbfs://', 1)
            read_uri, write_uri = (parquet, ordinary) if direct else (ordinary, parquet)
            other_uri = ordinary if direct else parquet
            with self.subTest(read_uri=read_uri):
                sql(f'COPY (SELECT i, repeat(md5(i::VARCHAR),2) payload FROM range({rows}) t(i)) '
                    f'TO {quote(parquet)} (FORMAT PARQUET, COMPRESSION UNCOMPRESSED, ROW_GROUP_SIZE 2048);')
                # Pause the required slice-map response, rather than a possible
                # background S3 prefetch. The query cannot finish this read
                # until the gate opens. No metadata/session rows are edited.
                proxy = FaultProxy(ENDPOINT)
                self.addCleanup(proxy.close)
                with proxy.mu:
                    proxy.rule = {'method': 'POST', 'op': 'read', 'mode': 'hold_ack', 'count': 1}
                marker = Path(self.tmp.name) / (uuid.uuid4().hex + '.csv')
                child = subprocess.Popen([DUCKDB, '-unsigned', '-bail', '-csv', '-noheader'],
                                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                self.children.append(child)
                child.stdin.write(INIT.replace(quote(ENDPOINT), quote(proxy.url)) + 'SET threads=4; '
                                  f'SELECT count(*),sum(i),count(*) FILTER(WHERE payload<>repeat(md5(i::VARCHAR),2)) '
                                  f'FROM (SELECT * FROM read_parquet({quote(read_uri)}) UNION ALL '
                                  f'SELECT * FROM read_parquet({quote(other_uri)})); '
                                  f'COPY (SELECT 1) TO {quote(marker)};\n')
                child.stdin.flush()
                try:
                    until = time.monotonic() + 45
                    while True:
                        with proxy.mu:
                            events = [e.copy() for e in proxy.events]
                        if any(e['fault'] == 'hold_ack' and e.get('status') == 200 for e in events):
                            self.assertEqual(sum(e['op'] == 'setlk' for e in events), 1,
                                             'Query released or reacquired its pinned read lock')
                            break
                        if child.poll() is not None:
                            self.fail(child.stderr.read().replace(TOKEN, '<redacted>'))
                        if time.monotonic() > until:
                            self.fail('Analytical query did not reach the gated slice-map read')
                        time.sleep(0.05)
                    self.assertFalse(marker.exists(), 'Reader completed before the writer raced it')
                    # Write through the direct extent adapter, which requests
                    # the existing exclusive inode lock. Ordinary HTTP
                    # namespace replacement is a separate advisory-lock limit.
                    self.assertIn('locked', sql(replacement.format(quote(parquet)), fail=True))
                finally:
                    proxy.release.set()
                until = time.monotonic() + 45
                while not marker.exists():
                    if child.poll() is not None:
                        self.fail(child.stderr.read().replace(TOKEN, '<redacted>'))
                    if time.monotonic() > until:
                        self.fail('Analytical query did not finish after the read gate opened')
                    time.sleep(0.05)
                # Keep the reader's DuckDB instance/session alive: QueryEnd,
                # rather than disconnect, must release every pinned read lock.
                self.assertIsNone(child.poll())
                sql(replacement.format(quote(parquet)))
                sql(replacement.format(quote(write_uri)))
                with proxy.mu:
                    locks = [e.copy() for e in proxy.events if e['op'] == 'setlk']
                self.assertEqual(len(locks), 2, 'Query must acquire once and release once')
                self.assertNotEqual(locks[0]['ltype'], locks[1]['ltype'])
                self.finish(child)
                result = child.stdout.read().strip()
                self.assertEqual(result.splitlines()[-1], f'{rows*2},{rows*(rows-1)},0')
                self.assertEqual(sql(f'SELECT i FROM read_parquet({quote(read_uri)});').splitlines()[-1], '7')

    def test_failed_analytical_extent_query_releases_read_lock(self):
        parquet = self.uri + '.parquet'
        ordinary = parquet.replace('tidbfs+db://', 'tidbfs://', 1)
        for read_uri, write_uri in ((ordinary, parquet), (parquet, ordinary)):
            with self.subTest(read_uri=read_uri):
                sql(f'COPY (SELECT 42 n) TO {quote(parquet)} (FORMAT PARQUET);')
                proxy = FaultProxy(ENDPOINT)
                self.addCleanup(proxy.close)
                # Leave the CLI/instance alive after the deliberate error, so
                # disconnect cannot conceal a missing QueryEnd release.
                child = self.hold(
                    f"SELECT error('deliberate analytical failure') FROM read_parquet({quote(read_uri)});",
                    bail=False, endpoint=proxy.url)
                with proxy.mu:
                    locks = [e.copy() for e in proxy.events if e['op'] == 'setlk']
                self.assertEqual(len(locks), 2, 'Failed query leaked its shared read lock')
                self.assertNotEqual(locks[0]['ltype'], locks[1]['ltype'])
                sql(f'COPY (SELECT 43 n) TO {quote(write_uri)} (FORMAT PARQUET, USE_TMP_FILE false);')
                child.stdin.close()
                self.assertNotEqual(child.wait(timeout=30), 0)
                self.assertIn('deliberate analytical failure', child.stderr.read())

    def test_delayed_metadata_ack_times_out_without_replay(self):
        sql(self.attach + 'CREATE TABLE delayed(id INTEGER); CHECKPOINT;')
        proxy = FaultProxy(ENDPOINT)
        self.addCleanup(proxy.close)
        with proxy.mu:
            proxy.rule = {'method': 'POST', 'op': 'write', 'mode': 'delay_ack', 'count': 1}
        started = time.monotonic()
        error = sql(self.attach + 'INSERT INTO delayed VALUES (7);', fail=True, endpoint=proxy.url)
        self.assertIn('Error', error)
        self.assertLess(time.monotonic() - started, 45, 'Metadata request did not honor its deadline')
        with proxy.mu:
            writes = [e.copy() for e in proxy.events if e['op'] == 'write']
        self.assertEqual(len(writes), 1, 'Timed-out publication was replayed')
        self.assertEqual(writes[0]['status'], 200, 'Fault did not follow a successful server write')
        result = self.recover(self.attach + 'SELECT count(*) FROM delayed; CHECKPOINT;')
        self.assertIn(result.splitlines()[-1], ('0', '1'))

    def test_extent_files_share_the_normal_namespace(self):
        # No hidden /data mapping and no new file tables: normal /v1/fs sees
        # the database, and ordinary tidbfs analytical readers read extent files.
        sql(self.attach + 'CREATE TABLE visible AS SELECT 42 n; CHECKPOINT;')
        path = self.uri[len('tidbfs+db://'):]
        request = urllib.request.Request(ENDPOINT + '/v1/fs' + path, method='HEAD',
                                         headers={'Authorization': 'Bearer ' + TOKEN})
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.headers['X-Dat9-Content-Layout'], 'extent')
            self.assertGreater(int(response.headers['Content-Length']), 0)
        parquet = self.uri + '.parquet'
        sql(f"COPY (SELECT 42 n) TO {quote(parquet)} (FORMAT PARQUET);")
        ordinary = parquet.replace('tidbfs+db://', 'tidbfs://', 1)
        self.assertTrue(sql(f"SELECT n FROM read_parquet({quote(ordinary)});").endswith('42'))
        sql(f"COPY (SELECT 43 n) TO {quote(ordinary)} (FORMAT PARQUET, USE_TMP_FILE false);")
        self.assertTrue(sql(f"SELECT n FROM read_parquet({quote(ordinary)});").endswith('43'))

    def test_temporary_extent_copy_replaces_both_namespace_views(self):
        # Exercise DuckDB's MoveFile, including URI-root JoinPath handling and
        # a parent created through the classic API. Both must bind the same
        # destination inode after publishing/replacing an extent temp file.
        directory = self.uri[len('tidbfs+db://'):] + '.files'
        request = urllib.request.Request(ENDPOINT + '/v1/fs' + directory + '?mkdir=1',
                                         method='POST', headers={'Authorization': 'Bearer ' + TOKEN})
        with urllib.request.urlopen(request, timeout=10):
            pass
        for path in (directory + '.parquet', directory + '/result.parquet'):
            native, ordinary = 'tidbfs+db://' + path, 'tidbfs://' + path
            previous_inode = None
            for value in (11, 22):
                with self.subTest(path=path, value=value):
                    sql(f"COPY (SELECT {value} n) TO {quote(native)} (FORMAT PARQUET, USE_TMP_FILE true);")
                    for uri in (native, ordinary):
                        self.assertEqual(sql(f'SELECT n FROM read_parquet({quote(uri)});').splitlines()[-1], str(value))
                    request = urllib.request.Request(ENDPOINT + '/v1/fs' + path, method='HEAD',
                                                     headers={'Authorization': 'Bearer ' + TOKEN})
                    with urllib.request.urlopen(request, timeout=10) as response:
                        self.assertEqual(response.headers['X-Dat9-Content-Layout'], 'extent')
                        inode = int(response.headers['X-Dat9-Extent-Ino'])
                        self.assertGreater(inode, 0)
                        self.assertNotEqual(inode, previous_inode, 'COPY did not replace the destination inode')
                        previous_inode = inode
        # The temporary projection must disappear along with its extent dentry.
        listing = sql(f"SELECT count(*) FROM glob({quote('tidbfs://' + directory + '/*')});")
        self.assertEqual(listing.splitlines()[-1], '1')

    def test_external_sort_with_remote_temporary_files(self):
        result = sql(self.attach + f"""
            SET threads=1; SET preserve_insertion_order=false; SET memory_limit='64MB';
            SET temp_directory={quote(self.uri + '.tmp')};
            SELECT sum(length(payload)) FROM (
                SELECT md5(i::VARCHAR) payload FROM range(2000000) t(i) ORDER BY payload
            );
        """)
        self.assertTrue(result.endswith('64000000'), result)
        # Temporary storage can outlive DETACH; reattach must still work.
        result = sql(self.attach + f"SET temp_directory={quote(self.uri + '.tmp')}; "
                     "CREATE TABLE t AS SELECT 42 n; USE memory; DETACH remote; "
                     + self.attach + "SELECT n FROM t;")
        self.assertTrue(result.endswith('42'), result)

    def test_out_of_memory_cleans_up_without_abort(self):
        error = sql(self.attach + f"SET threads=2; SET memory_limit='24MB'; "
                    f"SET temp_directory={quote(self.uri + '.tmp')}; "
                    "SELECT sum(length(payload)) FROM (SELECT md5(i::VARCHAR) payload "
                    "FROM range(1000000) t(i) ORDER BY payload);", fail=True)
        self.assertIn('Out of Memory', error)
        self.assertNotIn('terminating', error)
        self.assertTrue(sql(self.attach + 'SELECT 42;').endswith('42'))

    def object_fault(self, rule=None):
        endpoint = os.environ.get('TIDBFS_FAULT_PROXY')
        if not endpoint:
            self.skipTest('Requires --rustfs-server and its disposable fault proxy')
        request = urllib.request.Request(endpoint + '/_tidbfs_fault',
                    data=json.dumps(rule).encode() if rule is not None else None,
                    headers={'Authorization': 'Bearer ' + os.environ['TIDBFS_FAULT_TOKEN']})
        with urllib.request.urlopen(request, timeout=5) as response:
            return json.load(response)['events']

    def object_fixture(self):
        self.object_fault({})
        self.addCleanup(self.object_fault, {})
        sql(self.attach + "CREATE TABLE objects AS SELECT i, md5(i::VARCHAR) payload FROM range(50000) t(i); CHECKPOINT;")

    def assert_object_rows(self, count=50000):
        result = sql(self.readonly + 'SELECT count(*),sum(i),sum(length(payload)),count(DISTINCT i),'
                     'count(*) FILTER(WHERE payload<>md5(i::VARCHAR)) FROM objects;')
        self.assertTrue(result.endswith(f'{count},{count*(count-1)//2},{count*32},{count},0'), result)

    def test_s3_transient_upload_failure_retries_without_duplication(self):
        self.object_fixture()
        self.object_fault({'method': 'PUT', 'mode': 'unavailable', 'count': 2})
        sql(self.attach + "INSERT INTO objects SELECT i, md5(i::VARCHAR) FROM range(50000,60000) t(i); CHECKPOINT;")
        events = self.object_fault()
        self.assertEqual(sum(e['fault'] == 'unavailable' for e in events), 2)
        self.assert_object_rows(60000)

    def test_s3_successful_upload_with_lost_ack_is_safe_to_retry(self):
        self.object_fixture()
        self.object_fault({'method': 'PUT', 'mode': 'drop_ack', 'count': 1})
        sql(self.attach + "INSERT INTO objects SELECT i, md5(i::VARCHAR) FROM range(50000,60000) t(i); CHECKPOINT;")
        events = self.object_fault()
        dropped = [e for e in events if e['fault'] == 'drop_ack']
        self.assertEqual(len(dropped), 1)
        self.assertEqual(dropped[0]['status'], 200)
        retries = [e for e in events if e['method'] == 'PUT' and e['path'] == dropped[0]['path']]
        self.assertGreaterEqual(len(retries), 2, 'Successful immutable object PUT was not retried')
        self.assert_object_rows(60000)

    def test_s3_transient_download_failure_recovers_in_fresh_process(self):
        self.object_fixture()
        self.object_fault({'method': 'GET', 'mode': 'unavailable', 'count': 2})
        self.assert_object_rows()
        self.assertEqual(sum(e['fault'] == 'unavailable' for e in self.object_fault()), 2)

    def test_s3_truncated_download_never_returns_partial_query_results(self):
        self.object_fixture()
        self.object_fault({'method': 'GET', 'mode': 'short_read', 'count': 1})
        self.assert_object_rows()
        self.assertEqual(sum(e['fault'] == 'short_read' for e in self.object_fault()), 1)

    def test_s3_failed_upload_cannot_acknowledge_commit(self):
        self.object_fixture()
        self.object_fault({'method': 'PUT', 'mode': 'denied', 'count': -1})
        # The upstream runtime permits several minutes of object upload
        # retries. Wait for its actual terminal failure, rather than killing it
        # and accidentally treating a client timeout as a rejected COMMIT.
        error = sql(self.attach + "INSERT INTO objects SELECT i, md5(i::VARCHAR) FROM range(50000,60000) t(i);", fail=True, timeout=480)
        self.assertIn('Error', error)
        self.assertTrue(any(e['fault'] == 'denied' for e in self.object_fault()))
        self.object_fault({})
        # No write reached S3, so no new WAL record may become visible.
        self.recover(self.attach + 'CHECKPOINT;')
        self.assert_object_rows()

    def test_s3_checkpoint_crash_after_upload_recovers_committed_wal(self):
        self.object_fault({})
        self.addCleanup(self.object_fault, {})
        writer = self.hold(self.attach + "SET checkpoint_threshold='1GB'; "
                           "CREATE TABLE objects AS SELECT i, md5(i::VARCHAR) payload FROM range(50000) t(i);")
        # Keep the successful object PUT response behind a barrier, before
        # the extent client can publish its new slice and finish checkpoint.
        self.object_fault({'method': 'PUT', 'mode': 'hold_ack', 'count': 1})
        writer.stdin.write('CHECKPOINT;\n')
        writer.stdin.flush()
        deadline = time.monotonic() + 90
        while True:
            held = [e for e in self.object_fault() if e['fault'] == 'hold_ack' and e.get('status') == 200]
            if held:
                break
            if writer.poll() is not None or time.monotonic() > deadline:
                self.fail('Checkpoint did not reach the successful S3 upload barrier')
            time.sleep(0.05)
        writer.kill()
        writer.wait(timeout=15)
        self.object_fault({})
        self.recover(self.attach + 'CHECKPOINT;')
        self.assert_object_rows()


if __name__ == '__main__':
    unittest.main(verbosity=2)
