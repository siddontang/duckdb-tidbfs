# Third-party notices

The tidbfs adapter is MIT licensed; see [LICENSE](LICENSE). The distribution
artifact also incorporates the following separately licensed components.
Dependency revisions are pinned by the submodules, `cpp-sdk-revision.txt`,
the SDK's checked downloads and the distribution workflow's vcpkg revision.
Copies of the principal dependency license texts are in
[third_party/licenses](third_party/licenses/); DuckDB's bundled component
notices are retained in its pinned source submodule.

| Component | License | Source and license notices |
| --- | --- | --- |
| DuckDB 2.0.0 and its bundled components | MIT and bundled third-party licenses | [Pinned source](https://github.com/duckdb/duckdb/tree/d54cbbe3df17e8e5ce68312f483b4155836112aa), including `LICENSE` and `third_party/` notices |
| drive9 C++ SDK | Apache-2.0 | [Pinned source](https://github.com/mem9-ai/drive9/tree/42a316a20a71d3d7b789053ed020b45ba633aedc/clients/drive9-cpp); copyright 2026 mem9-ai |
| AWS partition metadata used by the SDK | Apache-2.0 | Copyright Amazon.com, Inc. or its affiliates; [SDK license copy](https://github.com/mem9-ai/drive9/blob/42a316a20a71d3d7b789053ed020b45ba633aedc/clients/drive9-cpp/third_party/AWS-SDK-LICENSE.txt) |
| cpp-httplib 0.53.1 | MIT | [License](https://github.com/yhirose/cpp-httplib/blob/v0.53.1/LICENSE) |
| nlohmann/json 3.11.3 | MIT | [License](https://github.com/nlohmann/json/blob/v3.11.3/LICENSE.MIT) |
| utf8proc 2.10.0 and Unicode data | MIT and Unicode data license | [License](https://github.com/JuliaStrings/utf8proc/blob/v2.10.0/LICENSE.md) |
| OpenSSL | Apache-2.0 | [vcpkg port](https://github.com/microsoft/vcpkg/tree/cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3/ports/openssl) |
| libcurl | curl and accompanying notices | [vcpkg port](https://github.com/microsoft/vcpkg/tree/cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3/ports/curl) |
| zlib | Zlib | [vcpkg port](https://github.com/microsoft/vcpkg/tree/cd61e1e26a038e82d6550a3ebbe0fbbfe7da78e3/ports/zlib) |

The SDK applies a checked patch to cpp-httplib's reconnect path to prevent
implicit replay of metadata mutations. The full upstream source and license
remain available at the pinned links above. Built vcpkg dependencies retain
their full license texts under `build/release/vcpkg_installed/*/share/*/copyright`.

The upstream drive9 NOTICE reads:

> drive9
> Copyright 2026 mem9-ai
>
> This product includes software developed by Juicedata, Inc. (JuiceFS),
> licensed under the Apache License, Version 2.0.
> See https://github.com/juicedata/juicefs and the Apache License 2.0
> text distributed with that project.
>
> The drive9 JuiceFS meta engine (pkg/meta in the mornyx/juicefs fork)
> is a modification of JuiceFS, used under the terms of Apache-2.0.

The plugin links the C++ SDK; the Go meta engine is not part of its binary.
