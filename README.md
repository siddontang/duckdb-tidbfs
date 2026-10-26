# duckdb-tidbfs

[![CI](https://github.com/siddontang/duckdb-tidbfs/actions/workflows/ci.yml/badge.svg)](https://github.com/siddontang/duckdb-tidbfs/actions/workflows/ci.yml)

`tidbfs` is a C++ DuckDB filesystem extension for reading and writing analytics files over the
TiDB Filesystem HTTP data plane, with experimental native `.duckdb` storage. It works in the DuckDB CLI and does not require
Python, fsspec, FUSE, or downloading whole Parquet files. Python is used only by
the build and test scripts.

## Build and query

The current target is DuckDB **v2.0.0**, with macOS arm64 and Linux amd64 validation. C++ extensions
must be built against the exact DuckDB version and platform that loads them.
Install the matching DuckDB C++ headers, a C++17 compiler, CMake, OpenSSL and libcurl development libraries.
The extension links the public [drive9 C++ SDK](https://github.com/mem9-ai/drive9/tree/42a316a20a71d3d7b789053ed020b45ba633aedc/clients/drive9-cpp), pinned to its merged revision in `cpp-sdk-revision.txt`.
It calls the existing HTTP APIs directly and includes no Go bridge or Go runtime.
For Homebrew DuckDB on Apple Silicon:

```sh
git clone https://github.com/siddontang/duckdb-tidbfs.git
cd duckdb-tidbfs
make build
duckdb -unsigned
```

For another matching SDK/CLI, set `DUCKDB_INCLUDE` and `DUCKDB`; `CXX` and
`CXXFLAGS` are also supported. The script downloads nlohmann/json 3.11.3 with a
pinned SHA-256, creates the DuckDB extension metadata, and verifies `LOAD`.
On Linux, install CMake and set `DUCKDB_SOURCE` to a matching DuckDB source
checkout. The script uses DuckDB's CMake build to statically link the extension
and its dependencies; the official CLI does not export every C++ filesystem
symbol, and the release SDK's static archive omits its third-party libraries.
The first build takes several minutes. `BUILD_JOBS` controls parallelism (default
2), and `CMAKE_ARGS` accepts additional CMake settings. The
[CI workflow](.github/workflows/ci.yml) shows the complete Linux setup with a
checksummed DuckDB CLI. macOS uses the faster installed-SDK build.

```sql
LOAD 'build/tidbfs.duckdb_extension';

CREATE TEMPORARY SECRET fs (
    TYPE tidbfs,
    ENDPOINT 'https://YOUR_FILESYSTEM_ENDPOINT',
    TOKEN 'YOUR_FILESYSTEM_API_KEY',
    SCOPE 'tidbfs:///'
);

SELECT region, sum(amount_cents)
FROM read_parquet('tidbfs:///warehouse/day=*/*.parquet', hive_partitioning=true)
WHERE day = '2026-10-17'
GROUP BY region;

COPY (SELECT * FROM range(100) t(id))
TO 'tidbfs:///results/output.parquet' (FORMAT PARQUET);

COPY (SELECT i AS id, i % 4 AS bucket FROM range(100) t(i))
TO 'tidbfs:///results/partitioned' (FORMAT PARQUET, PARTITION_BY(bucket));

EXPORT DATABASE 'tidbfs:///backups/run-001' (FORMAT PARQUET);
-- In a fresh local/in-memory database with the extension and secret loaded:
IMPORT DATABASE 'tidbfs:///backups/run-001';
```

`ENDPOINT` is the filesystem data-plane base URL; `/v1/fs` is appended by the
extension. The URI contains an absolute filesystem path, not a hostname.
Use separate scoped secrets to map different path prefixes to different
filesystems. Tokens are redacted by `duckdb_secrets()`. In automation, send SQL
through stdin rather than placing credentials in process arguments.

The build is unsigned and is not published to DuckDB's extension repository.
`INSTALL tidbfs` from a public repository is not available yet.

Both Linux and macOS builds produce a single `tidbfs.duckdb_extension` file.
Use SQL `INSTALL` or `python3 scripts/install.py` to install it; no adjacent
runtime library is required. OpenSSL and libcurl remain system dependencies.
Go is used only to build the independent filesystem server in integration CI.

## Write support and compatibility

For measured native database handoffs, see the
[multi-GiB benchmark](docs/benchmarks/handoff-2026-10-18.md), including separate
flush/close, worker startup, reopen, and client-cold/warm query timings.

| Operation | Support |
| --- | --- |
| Read Parquet, CSV, JSON; glob/Hive pruning | Supported |
| `COPY TO` Parquet, CSV, JSON; overwrite an output file | Supported |
| Partitioned `COPY`, `APPEND`, `OVERWRITE` | Supported; APPEND adds dataset files |
| `COPY ... USE_TMP_FILE true` | Supported through server rename |
| `EXPORT DATABASE` / `IMPORT DATABASE` | Supported; data and SQL scripts are remote |
| SQL DDL, DML and transactions in a local/in-memory database | Normal DuckDB behavior; import/export connects it to remote files |
| Writable `ATTACH 'tidbfs+db:///database.duckdb'` | Experimental: native database, WAL, checkpoint and recovery over existing extent storage |
| Writable native database on ordinary `tidbfs:///` | Use `tidbfs+db:///` to select extent I/O and database locking; both address the same filesystem |
| Multi-file atomic export, rollback of external files | Not provided |

Writable handles stage bytes in a private, unlinked local temporary file. This
supports seek, offset writes and truncation without retaining the entire output
in memory, but requires local scratch space proportional to open output files.
On explicit close/sync, the extension publishes a whole generation. Small files
use a conditional PUT; large files use V2 multipart uploads directly to object
storage, without forwarding filesystem credentials. Expired part URLs get one
refresh; failed multipart uploads are aborted on a best-effort basis.

DuckDB's EXPORT script writers destroy handles without calling close. Their
pending output is published at successful query end, with publication errors
returned to the caller. Failed queries discard unpublished staging; files
already published by a multi-file operation remain, as do directory changes.
An ambiguous commit response is reported as failure and is not blindly retried.

Writes carry the destination's expected revision (`0` for creation); conflicting
revisions fail with HTTP 409. This is optimistic publication, not a lifetime file
lock. The current write API compares revisions, not inode identity: a concurrent
delete/recreate with the same revision is not fenced. Do not use concurrent
namespace replacement as a database locking protocol. See the
[compatibility matrix](docs/compatibility.md) for the implemented native mode and remaining gaps.

## Native database mode (experimental)

`tidbfs+db:///warehouse.duckdb` runs DuckDB's storage engine on the existing
extent data plane. It addresses `/warehouse.duckdb` in the **same filesystem**
as `/v1/fs/warehouse.duckdb`; its WAL is `/warehouse.duckdb.wal`. There is no
separate volume, hidden `/data` mapping, new server endpoint or schema migration.
Ordinary listing and metadata APIs see these files.

The adapter links the pinned public drive9 C++ SDK directly. Metadata uses
`/v1/extent/meta`, credentials use `/v1/data-credential`, and binary bytes use
the existing object store. It reuses sessions, advisory locks, open references
and immutable object layout; there is no Go bridge or language boundary.
Use an unscoped filesystem token, as required by the existing extent API.

```sql
CREATE SECRET fs (TYPE tidbfs, ENDPOINT 'https://YOUR_FILESYSTEM_ENDPOINT',
                  TOKEN 'YOUR_FILESYSTEM_API_KEY');
ATTACH 'tidbfs+db:///warehouse.duckdb' AS warehouse;
CREATE TABLE warehouse.items AS SELECT * FROM range(100);
CHECKPOINT warehouse;
```

Ordinary `tidbfs:///` analytical opens also recognize existing extent files and
route their I/O through this adapter. New ordinary analytical output still uses
revision-checked PUT/multipart. Native mode accepts arbitrary filenames; the
`.duckdb` suffix is a convention, not a server rule.

Locks are the existing **advisory inode locks**: one database writer or shared
readers, with cooperating clients. Ordinary clients that ignore locks must not
replace a live database or its WAL. The adapter stops after an ambiguous metadata
response or a missed session deadline; it never reacquires ownership under an
already-open DuckDB instance. Metadata requests are bounded and late successful
responses cannot revive expired sessions. The shared extent transport also carries
the existing session ID; servers advertising `session_guard: true` validate it in
the same transaction as each metadata mutation, rejecting delayed old-session
writes after cleanup. Older servers retain legacy behavior. Advisory locks still
require cooperating clients.
Each write uploads immutable objects and waits for the existing metadata
publication to succeed before acknowledging it. Sync waits for in-flight writes.
Directory mutations negotiate the generic `directory_projection` capability
and uses optional `mknod.create_projection`; its default preserves existing
Go/FUSE clients. See [setup, recovery behavior and live tests](docs/native-database.md).
The live harness includes disposable RustFS tests for upload/download failures,
lost responses, truncated reads, failed COMMIT and checkpoint crash recovery.
The [interrupted-handoff demo](docs/demos/interrupted-handoff/README.md) disconnects
a running writer from metadata and S3, waits for another worker to take over,
then reconnects the old writer and verifies its COMMIT is rejected. It includes
measured timing, full-row checksums, a GIF/MP4 and the reproducible fault harness.
For shorter failure handoffs, use `SET GLOBAL tidbfs_heartbeat_ms=1000` before first extent
access in a fresh DuckDB instance (~5 s session lifetime; default ~60 s).
See [timing configuration and tradeoffs](docs/native-database.md#tuning-interrupted-handoff).
The [local tuning benchmark](docs/benchmarks/handoff-tuning-2026-10-18.md) measured
4.75 s median takeover with 1 s heartbeats, versus 57.68 s with the default.
A [fresh reconnect test](docs/demos/reconnect-test-2026-10-18/README.md) records
5.33 s takeover, rejected stale COMMIT and full-row validation, with a GIF and raw evidence.

The value is twofold: analytical files let DuckDB's pruning reach remote byte
ranges, while native mode lets a process leave SQL state that another process
can recover, using the filesystem's existing storage machinery. Queries execute
locally. This adds neither distributed SQL execution nor concurrent database
writer processes. Extent performance and failure behavior still require broader
production validation; it is not a claim of complete DuckDB compatibility.

## Quack SQL clients over shared storage

DuckDB 2.0 workers can serve the attached filesystem database with the official
Quack extension. Multiple remote clients then execute concurrent transactions
inside one worker, while tidbfs persists its database and WAL. Replacement
workers reopen the same shared URI; client SQL sessions are not migrated.
See [Quack setup, concurrency, failure tests and takeover](docs/quack.md),
and [pure C++ client validation and performance](docs/cpp-client-performance.md).
The [earlier Go read-path benchmark](docs/read-performance.md) remains available
as historical evidence.

## Read-path optimizations

Existing extent analytical files on either URI use one shared POSIX lock and
open inode per query, pinned across row-group handles and URI aliases until the
query finishes or fails. Cooperating extent overwrites are excluded during that
time. Both URI routes share one client registry and heartbeat configuration
within the DuckDB instance.

For ordinary inline and single-object files:

* One metadata lookup per path/endpoint/credential **per query**, shared across
  DuckDB's row-group readers. Query end discards these entries; later queries
  revalidate. The selected file version is pinned across those readers.
* Reuse the selected object's presigned URL across readers. Direct object
  requests carry neither the filesystem bearer token nor its ETag. A backing
  object ETag pins subsequent requests; expired URLs get one conditional
  refresh through fs.
* Keep-alive HTTP connections per handle, with independent handles able to read
  concurrently. A handle retains at most 1 MiB of range data; HTTP read buffers
  are capped at 8 MiB. Directory response bodies are capped at 16 MiB.
* A 64 KiB tail/read-ahead window can combine Parquet trailer and footer reads.
  `SET tidbfs_read_ahead = 0` disables prefetch; allowed values are 0–1048576.
  Cached bytes remain handle-local and are released on close.
* Prefix-directed `*`, `?`, `[]`, and `**` globbing supplies ordinary file paths
  to DuckDB, preserving its Hive partition, column, and row-group pruning.

The matching **fs server changes are required**: strong inode/revision/size
ETags on HEAD and GET, conditional GET (`If-Match`), exact inline HTTP ranges,
and SQL `SUBSTRING`/`RIGHT` projections. Inline ranges retrieve only selected
blob bytes from TiDB while size and revision come from the same SQL statement.
The SQL projection reduces TiDB-to-server transfer and Go allocation; it does
not imply fewer TiKV storage-page reads. Large single-object content continues
to use object-store range GETs. SQL predicates still execute in DuckDB. See the
[server compatibility contract](docs/server-contract.md) before connecting an
existing deployment; this repository contains the client extension only.

## Verify

```sh
make test
```

The tests execute actual DuckDB CLI queries against an instrumented HTTP fixture:
column/row-group pruning, Hive and recursive globs, encoded paths, query cache
expiry, signed URL reuse/refresh, credential stripping, invalid ranges, short
responses, concurrent reads, changed revisions, and database-lock rejection.
The write tests cover format roundtrips, partition append/overwrite, temporary
rename, empty files, multipart upload/expiry/abort, concurrent publication,
deferred EXPORT failures, and remote export/import of tables and views.
The real TiDB smoke also verifies native COPY uploads, partitioned output,
temporary-file replacement and export/import following INSERT/UPDATE/DELETE.
The wide-footer fixture verifies that prefetch reduces two range requests to one.

For the real TiDB-backed inline path, start the updated fs server in local mode
with `DRIVE9_INLINE_THRESHOLD=16777216`, provision a disposable filesystem, then:

```sh
export DRIVE9_BASE=http://127.0.0.1:19009
# Supply the provisioned token via DRIVE9_API_KEY in the environment.
python3 scripts/smoke.py
```

This creates and deletes one unique `/duckdb-smoke/<uuid>/` directory. A local
counting proxy records actual HTTP body bytes, excluding headers/TLS. Each query
uses a fresh DuckDB process and verifies its SQL result. A 2026-10-17 local run
with 100,000 synthetic rows and a 4,211,473-byte inline Parquet file measured:

| Query | HTTP body bytes | HEAD | Range GET |
| --- | ---: | ---: | ---: |
| `sum(id) WHERE id < 2048` | 131,072 | 1 | 2 |
| `sum(length(payload))` | 3,859,261 | 1 | 11 |

These queries intentionally read different columns/row groups, demonstrating
that DuckDB pruning reaches the remote byte path. They are not a latency
benchmark or evidence of speedup over S3. The smoke's 16 MiB inline threshold
is an explicit test setting; the server's default remains unchanged.

## Boundaries

The analytical `tidbfs:///` path supports **read/write analytics files**,
including existing extent-layout files. Select `tidbfs+db:///` for native
database access; it selects the extent adapter within the same filesystem. No dataset-wide
transaction/snapshot is implied for analytical files. Inline changes
fail with HTTP 412; single-object reads can finish from their pinned immutable
generation, or fail if that generation is garbage-collected. Query-level
caching does not provide a transaction-wide snapshot across SQL statements.

Backing object stores must return a strong object ETag and exact 206 ranges.
The current local S3 mock lacks object ETags, so the real local smoke explicitly
uses TiDB-inline content; signed-object behavior is covered by the HTTP fixture.
Filesystem and object validators are different: clients must not automatically
forward filesystem conditional headers to a redirect target. HTTPS is required
except for same-origin loopback HTTP in local development. Transport errors
exclude tokens, signed URLs, and response bodies.

Reference: DuckDB's [filesystem interface](https://github.com/duckdb/duckdb/blob/v2.0.0/src/include/duckdb/common/file_system.hpp)
and [C++ extension entry point](https://github.com/duckdb/duckdb/blob/v2.0.0/src/include/duckdb/main/extension/extension_loader.hpp).

## License

[MIT](LICENSE). The build downloads nlohmann/json (MIT); DuckDB is MIT-licensed
and libcurl uses the curl license. Their respective licenses remain applicable.
