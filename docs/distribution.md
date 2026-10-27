# Community extension distribution

The initial publication targets DuckDB 2.0.0 on `linux_amd64` (glibc) and
`osx_arm64`. Linux arm64/musl, macOS Intel, Windows and WebAssembly are excluded.
The native database and Quack worker integration retains the limitations in
[compatibility.md](compatibility.md); publishing the extension does not change
its session, advisory-lock or durability contract.

## Reproducible build

The repository pins DuckDB and `extension-ci-tools` as Git submodules. The
distribution workflow uses DuckDB's reusable extension distribution workflow,
the same vcpkg revision as the community builder, and static OpenSSL/libcurl
dependencies declared in `vcpkg.json`. The public C++ SDK remains pinned by
`cpp-sdk-revision.txt`; its downloaded dependencies have SHA-256 checks.
See [third-party notices](../THIRD_PARTY_NOTICES.md) for dependency licenses.
The Makefile defaults `DUCKDB_VERSION` to `v2.0.0` so local release artifacts
carry the same ABI version as the official CLI, rather than a development hash.

```sh
git clone --recurse-submodules https://github.com/siddontang/duckdb-tidbfs.git
cd duckdb-tidbfs
# Install a C++17 compiler, CMake, make, pkg-config and Python 3.10+ first.
make setup-vcpkg
export VCPKG_TOOLCHAIN_PATH="$PWD/vcpkg/scripts/buildsystems/vcpkg.cmake"
# Use x64-linux-release on Linux amd64.
export VCPKG_TARGET_TRIPLET=arm64-osx-release
export VCPKG_HOST_TRIPLET="$VCPKG_TARGET_TRIPLET"
CMAKE_BUILD_PARALLEL_LEVEL=4 make release
make test_release
```

The distributable is `build/release/extension/tidbfs/tidbfs.duckdb_extension`.
Developer builds made with `make build` remain available; those can use system
OpenSSL/libcurl and are not the community distribution artifact.

## Publication tests

The community builder invokes `make test_release`. That target runs:

- A runtime dependency audit that rejects Homebrew paths and non-system shared
  libraries, including external OpenSSL, libcurl and Go runtimes.
- An explicit `LOAD` of the final artifact copied into an otherwise empty
  directory, checking extension registration and its version. This fails even
  if SQLLogicTest would skip a missing extension through `require`.
- Local `INSTALL` into a disposable extension directory, followed by `LOAD tidbfs`
  and checks of the installed state and version.
- All 31 existing HTTP regression tests using that copied artifact and the
  matching DuckDB CLI, with a disposable loopback fixture and no credentials.
- SQLLogicTest coverage for extension registration, secret redaction and
  validation, default settings and heartbeat bounds.

The existing native/RustFS/Quack CI remains separate and must also pass. The
distribution workflow disables reduced CI so both supported architectures are
actually built and tested on each PR. Its artifacts are unsigned until the
community repository builds and signs an accepted submission.

## Community descriptor

Submit `extensions/tidbfs/description.yml` to
<https://github.com/duckdb/community-extensions>. It records the MIT license,
maintainer `siddontang`, version `0.4.0`, the supported platforms and a fixed
source commit from this public repository. Use the same exclusions and vcpkg
revision as `.github/workflows/distribution.yml`. Include configuration examples
and the experimental native database/advisory-lock limits in its documentation.

After the submission is accepted and deployed, verify from the stock CLI:

```sql
INSTALL tidbfs FROM community;
LOAD tidbfs;
SELECT extension_version, installed_from
FROM duckdb_extensions() WHERE extension_name = 'tidbfs';
```

Future releases update the descriptor's version and source commit through a
community PR. A GitHub Release alone does not update the community artifact.
