"""Exercise the compiled extension through the real DuckDB CLI, over HTTP."""

import json
import base64
from concurrent.futures import ThreadPoolExecutor
import faulthandler
import os
from pathlib import Path
import re
import socket
import shutil
import signal
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[1]
DUCKDB = os.environ.get("DUCKDB", "duckdb")
EXTENSION = Path(os.environ.get("TIDBFS_TEST_EXTENSION", ROOT / "build/tidbfs.duckdb_extension")).resolve()


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


class Fixture:
    def __init__(self, files):
        self.files = files
        self.requests = []
        self.bytes = 0
        self.fault = None
        self.object_expired = False
        self.object_reads = 0
        self.revisions = {}
        self.directories = {"/"}
        self.uploads = {}
        self.inline_threshold = 1024 * 1024
        self.max_upload_bytes = 128 * 1024 * 1024
        self.upload_expired = False
        self.mutation_lock = threading.RLock()
        self.commit_barrier = None
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def handle(self):
                try:
                    super().handle()
                except (BrokenPipeError, ConnectionResetError):
                    pass  # bounded-body rejection intentionally closes the connection

            def do_HEAD(self):
                self.serve(True)

            def do_GET(self):
                self.serve(False)

            def do_POST(self):
                self.mutate()

            def do_PUT(self):
                self.mutate()

            def do_DELETE(self):
                self.mutate()

            def mutate(self):
                length = int(self.headers.get("Content-Length", "0"))
                target = urlsplit(self.path)
                path = unquote(target.path)
                object_write = path.startswith("/upload-parts/")
                self.observation = {"method": self.command, "path": path, "object": object_write,
                                         "authorization": self.headers.get("Authorization"),
                                         "if_match": self.headers.get("If-Match"), "range": None, "list": False,
                                         "body_read": False}
                fixture.requests.append(self.observation)
                body = self.rfile.read(length)
                self.observation["body_read"] = True
                if object_write and self.headers.get("Authorization"):
                    return self.reply(403)
                if not object_write and self.headers.get("Authorization") != "Bearer fixture-token":
                    return self.reply(403)
                if fixture.commit_barrier and self.command == "PUT" and path == "/v1/fs/output/race.csv":
                    fixture.commit_barrier.wait(timeout=15)
                with fixture.mutation_lock:
                    if object_write:
                        _, _, upload_id, number = path.split("/")
                        if fixture.fault == "upload_fail":
                            return self.reply(500)
                        if fixture.fault == "upload_expire" and not fixture.upload_expired:
                            fixture.upload_expired = True
                            return self.reply(403)
                        fixture.uploads[upload_id]["parts"][int(number)] = body
                        return self.reply(200, {"ETag": f'"part-{number}"'})
                    if path.startswith("/v2/uploads/"):
                        data = json.loads(body)
                        if path == "/v2/uploads/initiate":
                            upload_id = str(len(fixture.uploads) + 1)
                            fixture.uploads[upload_id] = dict(data, parts={})
                            return self.reply(202, body=json.dumps({"upload_id": upload_id,
                                              "part_size": 1024 * 1024, "checksum_contract": {"required": False}}).encode())
                        _, _, _, upload_id, action = path.split("/")
                        upload = fixture.uploads[upload_id]
                        if action == "presign":
                            number = data["part_number"]
                            part_size = min(1024 * 1024, upload["total_size"] - (number - 1) * 1024 * 1024)
                            url = fixture.endpoint + f"/upload-parts/{upload_id}/{number}"
                            if fixture.fault == "upload_unsafe":
                                url = "http://example.invalid/upload?secret=never-expose"
                            return self.reply(200, body=json.dumps({"number": number, "size": part_size, "url": url,
                                              "headers": {"Authorization": "Bearer must-not-forward", "If-Match": "must-not-forward"}}).encode())
                        if action == "abort":
                            upload["aborted"] = True
                            return self.reply(200)
                        if action == "complete":
                            expected = upload["expected_revision"]
                            current = fixture.revisions.get(upload["path"], 1 if upload["path"] in fixture.files else 0)
                            if fixture.fault == "write_conflict" or current != expected:
                                return self.reply(409)
                            contents = b"".join(upload["parts"][part["number"]] for part in data["parts"])
                            if len(contents) != upload["total_size"]:
                                return self.reply(400)
                            fixture.files[upload["path"]] = contents
                            fixture.revisions[upload["path"]] = current + 1
                            upload["complete"] = True
                            return self.reply(200, body=b'{"status":"completed"}')
                        return self.reply(400)
                    path = path[len("/v1/fs"):]
                    if self.command == "PUT":
                        expected = int(self.headers.get("X-Dat9-Expected-Revision", "-1"))
                        current = fixture.revisions.get(path, 1 if path in fixture.files else 0)
                        if fixture.fault == "write_conflict" or expected != current:
                            return self.reply(409)
                        if fixture.fault == "write_fail" or (fixture.fault == "schema_fail" and path.endswith("/schema.sql")):
                            return self.reply(500)
                        if length and length >= fixture.inline_threshold:
                            return self.reply(400)
                        fixture.files[path] = body
                        fixture.revisions[path] = current + 1
                        if fixture.fault == "write_drop_ack":
                            self.connection.shutdown(socket.SHUT_RDWR)
                            self.close_connection = True
                            return
                        return self.reply(200, body=json.dumps({"revision": current + 1}).encode())
                    if self.command == "DELETE":
                        if target.query == "recursive":
                            for name in list(fixture.files):
                                if name.startswith(path):
                                    del fixture.files[name]
                            fixture.directories = {name for name in fixture.directories if not name.startswith(path)}
                            return self.reply(200)
                        if path not in fixture.files:
                            return self.reply(404)
                        del fixture.files[path]
                        fixture.revisions.pop(path, None)
                        return self.reply(200)
                    if target.query == "mkdir":
                        fixture.directories.add(path)
                        return self.reply(200)
                    if target.query == "rename":
                        source = self.headers["X-Dat9-Rename-Source"]
                        if self.headers.get("X-Dat9-Path-Encoding") == "base64url":
                            source = base64.urlsafe_b64decode(source + "=" * (-len(source) % 4)).decode()
                        if source not in fixture.files:
                            return self.reply(404)
                        fixture.files[path] = fixture.files.pop(source)
                        fixture.revisions[path] = fixture.revisions.pop(source, 1)
                        return self.reply(200)
                    return self.reply(400)

            def reply(self, status, headers=None, body=b"", head=False):
                observation = getattr(self, "observation", None)
                if observation is not None:
                    observation.update(status=status, reply_bytes=0 if head else len(body), reply_sent=False)
                self.send_response(status)
                for key, value in (headers or {}).items():
                    self.send_header(key, str(value))
                if "Content-Length" not in (headers or {}):
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                if not head:
                    try:
                        self.wfile.write(body)
                    except (BrokenPipeError, ConnectionResetError):
                        return
                if observation is not None:
                    observation["reply_sent"] = True

            def serve(self, head):
                self.observation = None
                target = urlsplit(self.path)
                if target.path == "/v1/status":
                    return self.reply(200, body=json.dumps({"inline_threshold": fixture.inline_threshold,
                                      "max_upload_bytes": fixture.max_upload_bytes}).encode())
                object_read = target.path.startswith("/objects/")
                path = unquote(target.path[len("/objects") if object_read else len("/v1/fs"):])
                self.observation = {"method": self.command, "path": path, "object": object_read,
                                         "authorization": self.headers.get("Authorization"),
                                         "if_match": self.headers.get("If-Match"), "range": self.headers.get("Range"),
                                         "list": bool(target.query)}
                fixture.requests.append(self.observation)
                if object_read and self.headers.get("Authorization"):
                    return self.reply(403)
                if not object_read and self.headers.get("Authorization") != "Bearer fixture-token":
                    return self.reply(403)
                if head and fixture.fault == "open_race" and path == "/output/open-race.csv":
                    # DuckDB's bind/ownership probes saw an absent destination.
                    # Another publisher wins just as this writer's open fails.
                    probes = sum(r["method"] == "HEAD" and r["path"] == path for r in fixture.requests)
                    if probes == 4:
                        fixture.files[path] = b"id\n77\n"
                        return self.reply(503, head=True)
                if target.query == "list=1":
                    entries = {}
                    for name, body in fixture.files.items():
                        if name.startswith(path):
                            child = name[len(path):].split("/")[0]
                            if child:
                                entries[child] = {"name": child, "size": len(body),
                                                  "isDir": "/" in name[len(path):]}
                    for name in fixture.directories:
                        if name.startswith(path):
                            child = name[len(path):].split("/")[0]
                            if child:
                                entries[child] = {"name": child, "isDir": True}
                    return self.reply(200, body=json.dumps({"entries": list(entries.values())}).encode())
                directory = path.rstrip("/") + "/"
                if head and (directory in fixture.directories or any(name.startswith(directory) for name in fixture.files)):
                    return self.reply(200, {"X-Dat9-IsDir": "true", "Content-Length": 0}, head=True)
                if path not in fixture.files:
                    return self.reply(404)
                body = fixture.files[path]
                revision = fixture.revisions.get(path, 1)
                etag = '"object-v1"' if object_read else f'"inode-v{revision}"'
                headers = {"ETag": etag, "Accept-Ranges": "bytes", "X-Dat9-IsDir": "false",
                           "X-Dat9-Content-Layout": "single", "X-Dat9-Mtime": "1700000000", "X-Dat9-Revision": revision}
                if head:
                    headers["Content-Length"] = len(body)
                    if fixture.fault == "legacy":
                        del headers["ETag"]
                    if fixture.fault == "extent":
                        headers["X-Dat9-Content-Layout"] = "extent"
                    return self.reply(200, headers, head=True)
                if fixture.fault == "changed" or (self.headers.get("If-Match") and self.headers["If-Match"] != etag):
                    return self.reply(412)
                if path.startswith("/redirect/") and not object_read:
                    headers["Location"] = "/objects" + target.path[len("/v1/fs"):]
                    if fixture.fault == "unsafe_redirect":
                        headers["Location"] = "http://example.invalid/secret?credential=must-not-leak"
                    return self.reply(302, headers)
                if object_read:
                    fixture.object_reads += 1
                    if fixture.fault == "always_expired" or (fixture.fault == "expire_once" and self.headers.get("If-Match") == '"object-v1"' and not fixture.object_expired):
                        fixture.object_expired = True
                        return self.reply(403)
                match = re.fullmatch(r"bytes=(\d+)-(\d+)", self.headers.get("Range", ""))
                if not match:
                    return self.reply(400)
                start, end = map(int, match.groups())
                data = body[start:end + 1]
                fixture.bytes += len(data)
                headers["Content-Range"] = f"bytes {start}-{end}/{len(body)}"
                if fixture.fault == "wrong_range":
                    headers["Content-Range"] = f"bytes {start + 1}-{end + 1}/{len(body)}"
                if fixture.fault == "short":
                    data = data[:-1]
                if fixture.fault == "encoded":
                    headers["Content-Encoding"] = "gzip"
                if fixture.fault == "ignore_range":
                    return self.reply(200, headers, body)
                return self.reply(206, headers, data)

        class Server(ThreadingHTTPServer):
            request_queue_size = 128

        self.server = Server(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class ExtensionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not EXTENSION.is_file():
            raise RuntimeError("Build the tidbfs extension before running tests")
        cls.temp = tempfile.TemporaryDirectory()
        file = Path(cls.temp.name) / "orders.parquet"
        sql = (f"COPY (SELECT i::BIGINT AS id, (i % 17)::INTEGER AS bucket, "
               f"md5(i::VARCHAR) || md5((i+1)::VARCHAR) || md5((i+2)::VARCHAR) AS payload "
               f"FROM range(100000) t(i)) TO {sql_string(file)} (FORMAT PARQUET, ROW_GROUP_SIZE 10000);")
        subprocess.run([DUCKDB, "-c", sql], check=True, capture_output=True)
        cls.parquet = file.read_bytes()
        footer = Path(cls.temp.name) / "footer.parquet"
        columns = ",".join(f"i::BIGINT AS c{n}" for n in range(32))
        subprocess.run([DUCKDB, "-c", f"COPY (SELECT {columns} FROM range(32768) t(i)) TO {sql_string(footer)} (FORMAT PARQUET, ROW_GROUP_SIZE 2048);"], check=True, capture_output=True)
        cls.footer = footer.read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.fixture = Fixture({"/data/orders.parquet": self.parquet,
                                "/data/footer.parquet": self.footer,
                                "/data/items.csv": b"id,name\n1,first\n2,second\n",
                                "/data/items.json": b'{"id":1}\n{"id":2}\n',
                                "/redirect/orders.parquet": self.parquet,
                                "/partition/day=1/orders.parquet": self.parquet,
                                "/partition/day=2/orders.parquet": self.parquet,
                                "/data/中文 % #.parquet": self.parquet})
        self.addCleanup(self.fixture.close)

    def query(self, sql, ok=True, prefetch=65536):
        script = (f"LOAD {sql_string(EXTENSION)}; "
                  f"CREATE SECRET fs (TYPE tidbfs, ENDPOINT {sql_string(self.fixture.endpoint)}, TOKEN 'fixture-token'); "
                  f"SET tidbfs_read_ahead={prefetch}; SET threads=4; " + sql)
        if os.environ.get("TIDBFS_TEST_ASYNC_THREADS"):
            script = f"SET async_threads={int(os.environ['TIDBFS_TEST_ASYNC_THREADS'])}; " + script
        # SQL, including credentials, travels on stdin, never process arguments.
        command = [DUCKDB, "-unsigned", "-csv", "-noheader", "-bail"]
        trace_path = None
        if os.environ.get("TIDBFS_TRACE_HTTP") == "1" and shutil.which("strace"):
            trace_dir = ROOT / 'build/http-traces'
            trace_dir.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=trace_dir, prefix=self._testMethodName + '-', delete=False) as trace:
                trace_path = Path(trace.name)
            # No payload bytes or headers enter the syscall trace.
            command = ['strace', '-f', '-tt', '-T', '-s', '32', '-e', 'trace=network,poll,ppoll',
                       '-e', 'raw=sendto,recvfrom,sendmsg,recvmsg,sendmmsg,recvmmsg',
                       '-o', str(trace_path), '--'] + command
        with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, text=True, start_new_session=bool(trace_path)) as child:
            try:
                stdout, stderr = child.communicate(script, timeout=45)
            except subprocess.TimeoutExpired:
                # Keep the actual timeout as a failure, but capture where the
                # CLI and fixture were blocked before killing the child.
                keys = ('method', 'path', 'range', 'body_read', 'status', 'reply_bytes', 'reply_sent')
                print("Timed-out HTTP requests:", [{k: r[k] for k in keys if k in r}
                                                   for r in self.fixture.requests], flush=True)
                faulthandler.dump_traceback()
                if trace_path:
                    if shutil.which('ss'):
                        sockets = subprocess.run(['ss', '-tainp'], capture_output=True, text=True, timeout=10)
                        lines = sockets.stdout.splitlines()
                        selected = set()
                        for i, line in enumerate(lines):
                            if ':' + str(self.fixture.server.server_port) in line:
                                selected.update((i, i+1))
                        print('Fixture TCP sockets:', '\n'.join(line for i, line in enumerate(lines)
                              if i in selected), flush=True)
                    os.killpg(child.pid, signal.SIGKILL)
                elif os.environ.get("TIDBFS_GDB_ON_TIMEOUT") == "1" and shutil.which("gdb"):
                    try:
                        subprocess.run(["sudo", "-n", "gdb", "--batch", "-p", str(child.pid),
                                        "-ex", "thread apply all bt", "-ex", "detach"], timeout=20, check=False)
                    except subprocess.TimeoutExpired:
                        pass
                child.kill()
                child.communicate()
                if trace_path:
                    print('HTTP syscall trace:', str(trace_path), '\n',
                          '\n'.join(trace_path.read_text().splitlines()[-240:]), flush=True)
                raise
        result = subprocess.CompletedProcess(command, child.returncode, stdout, stderr)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
        return result.stdout.removeprefix("true\n").strip(), result.stderr

    def test_parquet_projection_and_row_group_pruning(self):
        out, _ = self.query("SELECT sum(id) FROM read_parquet('tidbfs:///data/orders.parquet') WHERE id < 2048;")
        self.assertEqual(out, str(2047 * 2048 // 2))
        narrow = self.fixture.bytes
        self.assertLess(narrow, len(self.parquet) // 5)
        self.fixture.bytes = 0
        out, _ = self.query("SELECT sum(length(payload)) FROM read_parquet('tidbfs:///data/orders.parquet');")
        self.assertEqual(out, str(100000 * 96))
        self.assertLess(narrow, self.fixture.bytes // 5)
        print(f"\nParquet bytes: narrow={narrow}, wide={self.fixture.bytes}, file={len(self.parquet)}")

    def test_partition_glob(self):
        out, _ = self.query("SELECT sum(id) FROM read_parquet('tidbfs:///partition/day=*/*.parquet', hive_partitioning=true) WHERE day=2;")
        self.assertEqual(out, str(99999 * 100000 // 2))
        content = [r for r in self.fixture.requests if r["method"] == "GET" and not r["list"]]
        day1 = sum(int(r["range"].split("-")[1]) - int(r["range"].split("=")[1].split("-")[0]) + 1
                   for r in content if "day=1" in r["path"])
        self.assertLessEqual(day1, 2 * 65536)  # schema/footer probes only

    def test_recursive_glob_and_encoded_paths(self):
        out, _ = self.query("SELECT count(*) FROM read_parquet('tidbfs:///partition/**/*.parquet');")
        self.assertEqual(out, "200000")
        out, _ = self.query("SELECT count(*) FROM read_parquet('tidbfs:///data/中文 % #.parquet');")
        self.assertEqual(out, "100000")

    def test_object_redirect_reuse_strips_credentials(self):
        out, _ = self.query("SELECT sum(length(payload)) FROM read_parquet('tidbfs:///redirect/orders.parquet');")
        self.assertEqual(out, str(100000 * 96))
        objects = [r for r in self.fixture.requests if r["object"]]
        redirects = [r for r in self.fixture.requests if not r["object"] and r["method"] == "GET"]
        self.assertGreater(len(objects), len(redirects))
        self.assertTrue(all(r["authorization"] is None for r in objects))
        self.assertTrue(all(r["if_match"] in (None, '"object-v1"') for r in objects))
        self.assertTrue(all(r["if_match"] == '"inode-v1"' for r in redirects))
        self.assertEqual(len([r for r in self.fixture.requests if r["method"] == "HEAD"]), 1)

    def test_query_metadata_cache_expires(self):
        sql = "SELECT count(*) FROM read_parquet('tidbfs:///data/orders.parquet');"
        out, _ = self.query(sql + sql)
        self.assertEqual(out, "100000\n100000")
        self.assertEqual(len([r for r in self.fixture.requests if r["method"] == "HEAD"]), 2)

    def test_parallel_row_groups_and_other_formats(self):
        out, _ = self.query("SET threads=16; SELECT sum(length(payload)) FROM read_parquet('tidbfs:///redirect/orders.parquet');")
        self.assertEqual(out, "9600000")
        self.assertEqual(len([r for r in self.fixture.requests if r["method"] == "HEAD"]), 1)
        out, _ = self.query("SELECT sum(id) FROM read_csv('tidbfs:///data/items.csv');")
        self.assertEqual(out, "3")
        out, _ = self.query("SELECT sum(id) FROM read_json('tidbfs:///data/items.json');")
        self.assertEqual(out, "3")

    def test_database_locks_and_invalid_paths(self):
        _, error = self.query("ATTACH 'tidbfs:///data/live.duckdb' AS remote;", ok=False)
        self.assertIn("database file locks", error)
        self.assertNotIn("/data/live.duckdb", self.fixture.files)
        for path in ("tidbfs://host/file.parquet", "tidbfs:///data/../file.parquet"):
            with self.subTest(path=path):
                self.fixture.requests.clear()
                self.query(f"SELECT * FROM read_parquet({sql_string(path)});", ok=False)
                self.assertEqual(self.fixture.requests, [])

    def test_heartbeat_configuration_bounds(self):
        out, _ = self.query("SELECT current_setting('tidbfs_heartbeat_ms');")
        self.assertEqual(out, '12000')
        _, error = self.query('SET SESSION tidbfs_heartbeat_ms=1000;', ok=False)
        self.assertIn('tidbfs_heartbeat_ms is global', error)
        for ms in (-1, 0, 999, 600001, 'NULL'):
            with self.subTest(heartbeat_ms=ms):
                self.fixture.requests.clear()
                _, error = self.query(f"SET GLOBAL tidbfs_heartbeat_ms={ms}; "
                                      "SELECT * FROM read_csv('tidbfs:///data/items.csv');", ok=False)
                self.assertIn('tidbfs_heartbeat_ms must be between 1000 and 600000', error)
                self.assertEqual(self.fixture.requests, [])

    def test_copy_formats_and_overwrite(self):
        for format in ("PARQUET", "CSV", "JSON"):
            with self.subTest(format=format):
                suffix = format.lower()
                path = f"tidbfs:///output/中文 % #.{suffix}"
                self.query(f"COPY (SELECT i AS id FROM range(50) t(i)) TO {sql_string(path)} (FORMAT {format});")
                out, _ = self.query(f"SELECT sum(id) FROM read_{suffix}({sql_string(path)});")
                self.assertEqual(out, "1225")
                self.query(f"COPY (SELECT 99 AS id) TO {sql_string(path)} (FORMAT {format});")
                out, _ = self.query(f"SELECT sum(id) FROM read_{suffix}({sql_string(path)});")
                self.assertEqual(out, "99")

    def test_partition_output_append_and_overwrite(self):
        sql = "COPY (SELECT i AS id, i % 3 AS bucket FROM range(30) t(i)) TO 'tidbfs:///out' (FORMAT PARQUET, PARTITION_BY(bucket)"
        self.query(sql + ");")
        out, _ = self.query("SELECT sum(id) FROM read_parquet('tidbfs:///out/**/*.parquet', hive_partitioning=true);")
        self.assertEqual(out, "435")
        self.query(sql + ");", ok=False)
        self.query(sql + ", APPEND);")
        out, _ = self.query("SELECT count(*) FROM read_parquet('tidbfs:///out/**/*.parquet');")
        self.assertEqual(out, "60")
        self.query(sql + ", OVERWRITE);")
        out, _ = self.query("SELECT count(*) FROM read_parquet('tidbfs:///out/**/*.parquet');")
        self.assertEqual(out, "30")

    def test_export_import_database(self):
        self.query("CREATE TABLE items AS SELECT i AS id, 'row-' || i AS label FROM range(100) t(i); "
                   "CREATE VIEW counts AS SELECT count(*) AS n FROM items; "
                   "EXPORT DATABASE 'tidbfs:///backup' (FORMAT PARQUET);")
        self.assertIn("/backup/schema.sql", list(self.fixture.files))
        self.assertIn("/backup/load.sql", list(self.fixture.files))
        out, _ = self.query("IMPORT DATABASE 'tidbfs:///backup'; SELECT n FROM counts; SELECT sum(id) FROM items;")
        self.assertEqual(out, "100\n4950")

    def test_export_reports_deferred_publication_failure(self):
        self.fixture.fault = "schema_fail"
        _, error = self.query("CREATE TABLE items AS SELECT 1 AS id; EXPORT DATABASE 'tidbfs:///failed-backup';", ok=False)
        self.assertIn("HTTP 500", error)
        self.assertNotIn("/failed-backup/schema.sql", list(self.fixture.files))

    def test_concurrent_publication_conflict(self):
        self.fixture.commit_barrier = threading.Barrier(2)
        def writer(value):
            # Return raw results because exactly one of the competing writers must fail.
            script = (f"LOAD {sql_string(EXTENSION)}; CREATE SECRET fs (TYPE tidbfs, "
                      f"ENDPOINT {sql_string(self.fixture.endpoint)}, TOKEN 'fixture-token'); "
                      f"COPY (SELECT {value} AS id) TO 'tidbfs:///output/race.csv' (FORMAT CSV);")
            return subprocess.run([DUCKDB, "-unsigned", "-csv", "-noheader", "-bail"],
                                  input=script, text=True, capture_output=True, timeout=30)
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(writer, (1, 2)))
        self.assertEqual(sum(r.returncode == 0 for r in results), 1)
        self.assertTrue(any("write conflict" in r.stderr for r in results))
        self.assertIn(self.fixture.files["/output/race.csv"], (b"id\n1\n", b"id\n2\n"))
        self.assertFalse(any(r["method"] == "DELETE" for r in self.fixture.requests),
                         "a failed COPY must not delete the concurrent publisher's file")

    def test_multipart_copy_and_credential_boundary(self):
        self.query("COPY (SELECT * FROM read_parquet('tidbfs:///data/orders.parquet')) "
                   "TO 'tidbfs:///output/large.parquet' (FORMAT PARQUET);")
        self.assertTrue(any(upload.get("complete") for upload in self.fixture.uploads.values()))
        objects = [r for r in self.fixture.requests if r["object"] and r["method"] == "PUT"]
        self.assertGreater(len(objects), 1)
        self.assertTrue(all(r["authorization"] is None and r["if_match"] is None for r in objects))
        out, _ = self.query("SELECT sum(length(payload)) FROM read_parquet('tidbfs:///output/large.parquet');")
        self.assertEqual(out, "9600000")

    def test_concurrent_multipart_copies_with_many_row_groups(self):
        # Independent CLI processes contend for the same HTTP fixture while
        # each async writer drains many row groups, rather than one large group.
        ready = threading.Barrier(4, timeout=15)

        def copy(index):
            ready.wait()
            self.query("COPY (SELECT * FROM read_parquet('tidbfs:///data/orders.parquet')) "
                       f"TO 'tidbfs:///output/concurrent-{index}.parquet' "
                       "(FORMAT PARQUET, ROW_GROUP_SIZE 2048, COMPRESSION UNCOMPRESSED);")

        with ThreadPoolExecutor(max_workers=4) as pool:
            for result in pool.map(copy, range(4)):
                pass
        self.assertEqual(len(self.fixture.uploads), 4)
        self.assertTrue(all(upload.get("complete") for upload in self.fixture.uploads.values()))
        for index in range(4):
            self.assertGreater(len(self.fixture.files[f"/output/concurrent-{index}.parquet"]),
                               8 * 1024 * 1024, "exercise DuckDB's remote async scheduling threshold")
            out, _ = self.query("SELECT count(*), count(DISTINCT id), min(id), max(id), "
                                "count(*) FILTER (WHERE bucket != id % 17 OR payload != "
                                "md5(id::VARCHAR) || md5((id+1)::VARCHAR) || md5((id+2)::VARCHAR)) "
                                f"FROM read_parquet('tidbfs:///output/concurrent-{index}.parquet');")
            self.assertEqual(out, "100000,100000,0,99999,0")
        objects = [r for r in self.fixture.requests if r["object"] and r["method"] == "PUT"]
        self.assertGreater(len(objects), 4)
        self.assertTrue(all(r["authorization"] is None and r["if_match"] is None for r in objects))

    def test_failed_publication_preserves_destination(self):
        for fault in ("write_fail", "write_conflict"):
            with self.subTest(fault=fault):
                self.fixture.fault = fault
                original = self.fixture.files["/data/items.csv"]
                self.query("COPY (SELECT 99 AS id) TO 'tidbfs:///data/items.csv' (FORMAT CSV);", ok=False)
                self.assertEqual(self.fixture.files["/data/items.csv"], original)

    def test_failed_multipart_is_aborted(self):
        for fault in ("upload_fail", "write_conflict", "upload_unsafe"):
            with self.subTest(fault=fault):
                self.fixture.fault = fault
                self.fixture.uploads.clear()
                _, error = self.query("COPY (SELECT * FROM read_parquet('tidbfs:///data/orders.parquet')) "
                                      "TO 'tidbfs:///output/fail.parquet' (FORMAT PARQUET);", ok=False)
                self.assertNotIn("/output/fail.parquet", self.fixture.files)
                self.assertTrue(self.fixture.uploads)
                self.assertTrue(all(upload.get("aborted") for upload in self.fixture.uploads.values()))
                self.assertNotIn("never-expose", error)

    def test_multipart_expiry_refresh(self):
        self.fixture.fault = "upload_expire"
        self.query("COPY (SELECT * FROM read_parquet('tidbfs:///data/orders.parquet')) "
                   "TO 'tidbfs:///output/refresh.parquet' (FORMAT PARQUET);")
        self.assertTrue(self.fixture.upload_expired)

    def test_query_error_discards_staging(self):
        self.query("COPY (SELECT CASE WHEN i = 99999 THEN error('stop') ELSE i END AS id "
                   "FROM range(100000) t(i)) TO 'tidbfs:///output/failed.csv' (FORMAT CSV);", ok=False)
        self.assertNotIn("/output/failed.csv", self.fixture.files)
        self.assertFalse(any(r["method"] == "PUT" for r in self.fixture.requests),
                         "aborting an asynchronous writer must discard its private staging")

    def test_failed_copy_preserves_existing_output(self):
        for compression in ("UNCOMPRESSED", "GZIP"):
            with self.subTest(compression=compression):
                original = self.fixture.files["/data/items.csv"]
                self.fixture.requests.clear()
                self.query("COPY (SELECT CASE WHEN i = 99999 THEN error('stop') ELSE i END AS id "
                           "FROM range(100000) t(i)) TO 'tidbfs:///data/items.csv' "
                           f"(FORMAT CSV, COMPRESSION {compression});", ok=False)
                self.assertEqual(self.fixture.files["/data/items.csv"], original)
                self.assertFalse(any(r["method"] in ("PUT", "DELETE") for r in self.fixture.requests))

    def test_failed_copy_cleanup_guard_expires_before_next_query(self):
        self.fixture.files["/dataset/old.csv"] = b"id\n7\n"
        script = (f"LOAD {sql_string(EXTENSION)}; CREATE SECRET fs (TYPE tidbfs, "
                  f"ENDPOINT {sql_string(self.fixture.endpoint)}, TOKEN 'fixture-token');\n"
                  "COPY (SELECT error('expected abort') AS id) TO 'tidbfs:///dataset/old.csv' (FORMAT CSV);\n"
                  "COPY (SELECT 1 AS id, 42 AS value) TO 'tidbfs:///dataset' (FORMAT CSV, PARTITION_BY(id), OVERWRITE);\n"
                  "SELECT count(*) FROM read_csv('tidbfs:///dataset/**/*.csv', hive_partitioning=true);\n")
        result = subprocess.run([DUCKDB, "-unsigned", "-csv", "-noheader"],
                                input=script, text=True, capture_output=True, timeout=45)
        self.assertIn("expected abort", result.stderr)
        self.assertNotIn("/dataset/old.csv", list(self.fixture.files))
        self.assertTrue(result.stdout.strip().endswith("1"), result.stdout + result.stderr)

    def test_lost_publication_ack_preserves_committed_generation(self):
        self.fixture.fault = "write_drop_ack"
        _, error = self.query("COPY (SELECT 42 AS id) TO 'tidbfs:///output/uncertain.csv' (FORMAT CSV);", ok=False)
        self.assertIn("transport", error)
        self.assertEqual(self.fixture.files.get("/output/uncertain.csv"), b"id\n42\n")
        self.assertEqual(sum(r["method"] == "PUT" for r in self.fixture.requests), 1)
        self.assertFalse(any(r["method"] == "DELETE" for r in self.fixture.requests))
        out, _ = self.query("SELECT id FROM read_csv('tidbfs:///output/uncertain.csv');")
        self.assertEqual(out, "42")

    def test_failed_open_does_not_delete_concurrent_publisher(self):
        self.fixture.fault = "open_race"
        _, error = self.query("COPY (SELECT 42 AS id) TO 'tidbfs:///output/open-race.csv' (FORMAT CSV);", ok=False)
        self.assertIn("HTTP 503", error)
        self.assertEqual(self.fixture.files.get("/output/open-race.csv"), b"id\n77\n")
        self.assertFalse(any(r["method"] == "DELETE" for r in self.fixture.requests))

    def test_temporary_output_and_empty_file(self):
        self.query("COPY (SELECT 7 AS id) TO 'tidbfs:///output/temp.csv' (FORMAT CSV, USE_TMP_FILE true);")
        out, _ = self.query("SELECT id FROM read_csv('tidbfs:///output/temp.csv');")
        self.assertEqual(out, "7")
        self.query("COPY (SELECT 1 AS id WHERE false) TO 'tidbfs:///output/empty.csv' (FORMAT CSV, HEADER false);")
        self.assertEqual(self.fixture.files["/output/empty.csv"], b"")
        # DuckDB's generic JoinPath strips URI-root slashes for temp siblings.
        self.query("COPY (SELECT 8 AS id) TO 'tidbfs:///root.csv' (FORMAT CSV, USE_TMP_FILE true);")
        out, _ = self.query("SELECT id FROM read_csv('tidbfs:///root.csv');")
        self.assertEqual(out, "8")

    def test_compressed_csv_roundtrip(self):
        self.query("COPY (SELECT i AS id FROM range(100) t(i)) TO 'tidbfs:///output/data.csv.gz' "
                   "(FORMAT CSV, COMPRESSION GZIP);")
        out, _ = self.query("SELECT sum(id) FROM read_csv('tidbfs:///output/data.csv.gz');")
        self.assertEqual(out, "4950")

    def test_exact_threshold_and_upload_limit(self):
        self.fixture.inline_threshold = 4
        self.query("COPY (SELECT 'abc' AS value) TO 'tidbfs:///output/boundary.csv' (FORMAT CSV, HEADER false);")
        self.assertEqual(self.fixture.files["/output/boundary.csv"], b"abc\n")
        self.assertTrue(any(upload.get("complete") for upload in self.fixture.uploads.values()))
        self.fixture.max_upload_bytes = 2
        _, error = self.query("COPY (SELECT 'abc' AS value) TO 'tidbfs:///output/limit.csv' (FORMAT CSV, HEADER false);", ok=False)
        self.assertIn("upload limit", error)
        self.assertNotIn("/output/limit.csv", list(self.fixture.files))

    def test_expired_object_url_refresh(self):
        # Expire after the first range succeeds, so this exercises reuse/refresh.
        self.fixture.fault = "expire_once"
        out, _ = self.query("SELECT sum(length(payload)) FROM read_parquet('tidbfs:///redirect/orders.parquet');")
        self.assertEqual(out, "9600000")
        self.assertTrue(self.fixture.object_expired)
        self.fixture.fault = "always_expired"
        _, error = self.query("SELECT count(*) FROM read_parquet('tidbfs:///redirect/orders.parquet');", ok=False)
        self.assertIn("HTTP 403", error)  # newly issued invalid URLs are not retried forever

    def test_protocol_failures(self):
        for fault, expected in [("changed", "changed"), ("wrong_range", "byte-range"), ("short", "byte-range"),
                                ("encoded", "byte-range"), ("ignore_range", "transport"),
                                ("legacy", "Upgrade fs"), ("extent", "extent")]:
            with self.subTest(fault=fault):
                self.fixture.fault = fault
                _, error = self.query("SELECT sum(id) FROM read_parquet('tidbfs:///data/orders.parquet');", ok=False)
                self.assertIn(expected, error)

    def test_unsafe_redirect_and_token_redaction(self):
        self.fixture.fault = "unsafe_redirect"
        _, error = self.query("SELECT count(*) FROM read_parquet('tidbfs:///redirect/orders.parquet');", ok=False)
        self.assertNotIn("must-not-leak", error)
        self.assertIn("Unsafe tidbfs object redirect", error)
        out, _ = self.query("SELECT secret_string FROM duckdb_secrets() WHERE type='tidbfs';")
        self.assertNotIn("fixture-token", out)

    def test_tail_prefetch_reduces_requests(self):
        sql = "SELECT count(*) FROM read_parquet('tidbfs:///data/footer.parquet');"
        self.query(sql, prefetch=0)
        without = len([r for r in self.fixture.requests if r["range"]])
        self.fixture.requests.clear()
        self.query(sql)
        with_prefetch = len([r for r in self.fixture.requests if r["range"]])
        self.assertLess(with_prefetch, without)
        print(f"\nFooter requests: no prefetch={without}, prefetch={with_prefetch}")


if __name__ == "__main__":
    unittest.main()
