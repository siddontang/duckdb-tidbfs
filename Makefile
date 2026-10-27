PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXT_NAME := tidbfs
EXT_CONFIG := $(PROJ_DIR)extension_config.cmake
.DEFAULT_GOAL := release

include extension-ci-tools/makefiles/duckdb_extension.Makefile
include extension-ci-tools/makefiles/vcpkg.Makefile

.PHONY: build test-local distribution-test-release distribution-test-debug distribution-test-reldebug
PYTHON ?= python3
# Keep the existing installed-SDK developer build available.
build:
	$(PYTHON) scripts/build.py
test-local: build
	$(PYTHON) -m unittest discover -s test -v

# The community builder invokes these same targets. Test the actual loadable
# artifact in a fresh CLI as well as the official SQLLogicTest suite.
test_release: distribution-test-release
test_debug: distribution-test-debug
test_reldebug: distribution-test-reldebug

distribution-test-release distribution-test-debug distribution-test-reldebug: distribution-test-%:
	@if [ "$(SKIP_TESTS)" != "1" ]; then \
		$(PYTHON) scripts/test_distribution.py --build-dir build/$*; \
	fi
