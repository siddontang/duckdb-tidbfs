# DuckDB compatibility

SQL, catalog, WAL, recovery and transaction execution remain inside unmodified
DuckDB 2.0.0. This is an experimental filesystem integration, not a complete
DuckDB or POSIX compatibility claim.

| Workflow | `tidbfs:///` | `tidbfs+db:///` |
| --- | --- | --- |
| Parquet/CSV/JSON, Hive/globs, partition COPY | Supported | Extent I/O; use analytical URI for globs |
| New analytical output | Revision-checked PUT/multipart | Existing extent creation |
| Existing extent analytical files | Automatically routed, query-lifetime shared lock | Same query-lifetime shared lock |
| Writable native ATTACH, catalog and DML | Use explicit database URI | Existing extent files |
| COMMIT/ROLLBACK, checkpoint and reopen | Local engine or attached database | Native WAL and pages on extent storage |
| One writer or shared readers | CAS publication for ordinary output | Existing advisory POSIX locks |
| Remote spill files | Whole-generation ordinary output | Extent random I/O and DuckDB cleanup |
| Metadata/listing through `/v1/fs` | Yes | Yes, same paths and namespace |
| Additional server tables/endpoints | None | None |
| Quack multi-client SQL and transactions | Filesystem data is read by the worker | Multiple clients through one database worker; [integration guide](quack.md) |

## Verification

`make test` builds and loads the extension and runs 31 analytical/HTTP CLI fixtures.
`scripts/test_native_live.py` runs C++ SDK conformance, native DuckDB tests and
client/server restart checks against disposable TiDB and filesystem services.
See [the native adapter](native-database.md) for the exact failure scenarios.
Server-side range validators, binary projections, shared-schema isolation and
existing extent regressions run with `--fs-source`.
Public CI runs native compatibility, temporary-file replacement across both
namespace views, cross-route locking/session regressions, ambiguous publication
and transient S3 fault cases on the public upstream server and disposable RustFS;
the complete recovery suite runs against current `tidbcloud/fs`, whose checkout
is private. The harness defaults to the complete suite, including failure tests.
`--quack` adds the separate-process concurrency, conflict, snapshot, crash,
partition, ambiguous-write and worker-handoff suite described in [Quack](quack.md).

## Boundaries

- Native mode is single writer or shared read-only processes. It does not add
  simultaneous database writer processes or distributed query execution.
- Existing locks are advisory. Other writers must cooperate; ordinary API
  clients must not replace a live database or its WAL.
- The client fails closed after transport ambiguity, metadata timeout, or missed
  session renewal. Late acknowledgements cannot revive expired sessions. The
  shared extent transport carries `caller_sid`; servers advertising
  `session_guard: true` validate it atomically with metadata mutations and reject
  old-session requests after cleanup. Older servers lack this guarantee.
  Advisory locks remain advisory; production TiKV/object-store failover coverage
  is incomplete.
- fsync uses the existing extent durability path. The first version syncs every
  write, trading performance for simple acknowledgement semantics.
- The extension uses the standalone C++ HTTP SDK, OpenSSL and libcurl. It has no
  Go runtime or companion shared library. Extent payloads pass through typed
  C++ buffers; JSON/base64 remains only in the existing metadata protocol.
- Native mode inherits existing extent backend/permission limits, including
  rejection of subtree-scoped server tokens. It adds no 1 GiB database cap.
- Named spill files can remain after a crash. Normal DuckDB cleanup removes
  them; there is no new server-side temporary-file reclamation policy.
- Native URI globs and automatic conversion of inline/object files into extent
  databases are unsupported. Use analytical globs and explicit export/import.
- S3 faults are tested using real RustFS and a loopback fault proxy: upload and
  download 503, lost upload response, truncated download, persistent upload
  failure, and checkpoint crash after upload. AWS/GCS production credentials,
  service-specific failures, full checkpoint-boundary sweeps, SQLLogicTest and
  broader platform/version coverage remain open.

Analytical publication uses existing revision CAS. Individual file publication
is conditional; a multi-file export is not atomic and is not rolled back with
SQL. Existing backend APIs also have inode-aware conditions, but the ordinary
PUT/multipart contract used here only exposes revision. Query-local read pinning
is not a transaction-wide or dataset-wide snapshot.
