#!/usr/bin/env python3
"""Build against a matching DuckDB 2.0 C++ SDK and CLI."""

import argparse
import hashlib
import os
from pathlib import Path
import platform
import shlex
import shutil
import subprocess
import urllib.request
from fetch_cpp import source as cpp_source

ROOT = Path(__file__).resolve().parents[1]
JSON_SHA256 = "9bea4c8066ef4a1c206b2be5a36302f8926f7fdc6087af5d20b417d0cf103ea6"


def build_linux(args, version, target, build):
    source = Path(args.source).resolve()
    if not (source / "src/include/duckdb/common/file_system.hpp").is_file():
        raise RuntimeError("--source must point to the matching DuckDB source checkout")
    cmake_build = build / "duckdb"
    flags = os.environ.get("CXXFLAGS", "")
    if target.endswith("_gcc4"):
        flags += " -D_GLIBCXX_USE_CXX11_ABI=0"
    commit = subprocess.check_output([args.duckdb, "-csv", "-noheader", "-c",
                                      "SELECT source_id FROM pragma_version()"], text=True).strip()
    command = ["cmake", "-S", str(source), "-B", str(cmake_build),
               "-DCMAKE_BUILD_TYPE=Release", "-DBUILD_UNITTESTS=OFF", "-DBUILD_SHELL=OFF",
               "-DSTATICALLY_LINK_EXTENSIONS=core_functions", "-DEXTENSION_STATIC_BUILD=ON",
               "-DDUCKDB_EXPLICIT_VERSION=" + version, "-DGIT_COMMIT_HASH=" + commit,
               "-DDUCKDB_EXTENSION_CONFIGS=" + str(ROOT / "extension_config.cmake"),
               "-DCMAKE_CXX_COMPILER=" + os.environ.get("CXX", "c++"),
               "-DCMAKE_CXX_FLAGS=" + flags]
    command += shlex.split(os.environ.get("CMAKE_ARGS", ""))
    subprocess.run(command, check=True)
    subprocess.run(["cmake", "--build", str(cmake_build), "--target", "tidbfs_loadable_extension",
                    "--parallel", os.environ.get("BUILD_JOBS", "2")], check=True)
    output = build / "tidbfs.duckdb_extension"
    shutil.copyfile(cmake_build / "extension/tidbfs/tidbfs.duckdb_extension", output)
    verify(args.duckdb, output, version, target)


def verify(duckdb, output, version, target):
    subprocess.run([duckdb, "-unsigned", "-c", "LOAD '" + str(output).replace("'", "''") + "';"], check=True)
    print(f"Built and loaded {output} ({version}, {target})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", default=os.environ.get("DUCKDB", "duckdb"))
    parser.add_argument("--include", default=os.environ.get("DUCKDB_INCLUDE", "/opt/homebrew/opt/duckdb/include"))
    parser.add_argument("--source", default=os.environ.get("DUCKDB_SOURCE"),
                        help="matching DuckDB source checkout (required on Linux)")
    args = parser.parse_args()
    includes = Path(args.include).resolve()
    system = platform.system()
    if system == "Darwin" and not (includes / "duckdb/common/file_system.hpp").is_file():
        parser.error("--include must point to the matching DuckDB C++ SDK (or DuckDB src/include)")
    if system == "Linux" and not args.source:
        parser.error("Linux requires --source /path/to/matching/duckdb and CMake")
    if system not in ("Darwin", "Linux"):
        parser.error("This build script supports macOS and Linux")
    version = subprocess.check_output([args.duckdb, "-csv", "-noheader", "-c", "SELECT version()"], text=True).strip()
    target = subprocess.check_output([args.duckdb, "-csv", "-noheader", "-c", "PRAGMA platform"], text=True).strip()
    build = ROOT / "build"
    if system == "Linux":
        build_linux(args, version, target, build)
        return
    deps = build / "deps"
    deps.mkdir(parents=True, exist_ok=True)
    header = deps / "json.hpp"
    if not header.exists():
        with urllib.request.urlopen("https://raw.githubusercontent.com/nlohmann/json/v3.11.3/single_include/nlohmann/json.hpp", timeout=60) as response:
            data = response.read()
        if hashlib.sha256(data).hexdigest() != JSON_SHA256:
            raise RuntimeError("JSON dependency checksum mismatch")
        header.write_bytes(data)
    if hashlib.sha256(header.read_bytes()).hexdigest() != JSON_SHA256:
        raise RuntimeError("JSON dependency checksum mismatch")
    output = build / "tidbfs.duckdb_extension"
    sdk = cpp_source()
    sdk_build = build / "cpp-sdk-build" / hashlib.sha256(str(sdk).encode()).hexdigest()[:12]
    cmake = os.environ.get("CMAKE", "cmake")
    configuration = [cmake, "-S", str(sdk), "-B", str(sdk_build), "-DCMAKE_BUILD_TYPE=Release",
                     "-DCMAKE_CXX_FLAGS=" + os.environ.get("CXXFLAGS", "")]
    configuration += shlex.split(os.environ.get("CMAKE_ARGS", ""))
    subprocess.run(configuration, check=True)
    subprocess.run([cmake, "--build", str(sdk_build), "--parallel", os.environ.get("BUILD_JOBS", "2")], check=True)
    subprocess.run([os.environ.get("CTEST", "ctest"), "--test-dir", str(sdk_build), "--output-on-failure"], check=True)
    openssl = Path(os.environ.get("OPENSSL_ROOT_DIR", "/opt/homebrew/opt/openssl@3"))
    command = [os.environ.get("CXX", "c++"), "-std=c++17", "-O2", "-DNDEBUG", "-fPIC", "-Wall", "-Wextra",
               "-Wno-unused-parameter", "-Wno-deprecated-declarations", "-I" + str(includes), "-I" + str(deps), "-I" + str(sdk / "include")]
    command += ["-dynamiclib", "-undefined", "dynamic_lookup"]
    command += shlex.split(os.environ.get("CXXFLAGS", ""))
    command += [str(ROOT / "src/tidbfs_extension.cpp"), "-o", str(output)]
    command += [str(sdk_build / "libdrive9_cpp.a"), str(sdk_build / "_deps/utf8proc-build/libutf8proc.a"),
                "-L" + str(openssl / "lib"), "-lssl", "-lcrypto", "-lcurl", "-pthread",
                "-framework", "CoreFoundation", "-framework", "Security"]
    subprocess.run(command, check=True)
    # DuckDB 2.0 scripts/append_metadata.cmake: 8 reversed 32-byte fields,
    # then 256 bytes reserved for a signature. The local build is unsigned.
    fields = ["4", target, version, "0.4.0", "CPP", "", "", ""]
    if any(len(field.encode()) > 32 for field in fields):
        raise RuntimeError("DuckDB metadata field exceeds 32 bytes")
    with output.open("ab") as handle:
        handle.write(b"\x00\x93\x04\x10duckdb_signature\x80\x04")
        for field in reversed(fields):
            handle.write(field.encode().ljust(32, b"\x00"))
        handle.write(bytes(256))
    verify(args.duckdb, output, version, target)


if __name__ == "__main__":
    main()
