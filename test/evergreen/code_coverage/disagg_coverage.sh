#!/usr/bin/env bash
#
# Public Domain 2014-present MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# disagg_coverage.sh
#
# Build WiredTiger with gcov instrumentation, run all disagg test types,
# and generate a gcovr HTML + JSON coverage report for the src/ tree.
#
# Usage:
#   ./test/evergreen/code_coverage/disagg_coverage.sh [options]
#
# Options:
#   -j N              Number of parallel test workers (default: nproc)
#   -b DIR_BASE       Base name for parallel build directories (default: build_disagg_cov_)
#   --skip-format     Skip format stress tests (faster, less coverage)
#   --skip-ctest      Skip checkpoint ctest with check_disagg label
#   --skip-python     Skip parallel Python / catch2 tests
#   --report-only     Skip all tests; just (re)generate the gcovr report
#   -h / --help       Show this message
#
# Run from the WiredTiger repository root. Expects:
#   - mongodbtoolchain at /opt/mongodbtoolchain/v5/  OR cmake/ctest on PATH
#   - python3, virtualenv
#   - gcovr 5.0 (installed into venv automatically)
#
# See analysis/disagg_testing/HOW_TO_RUN_COVERAGE.md for full instructions.

set -o errexit
set -o pipefail

############################
# Argument parsing
############################
NUM_JOBS=""
BUILD_DIR_BASE="build_disagg_cov_"
SKIP_FORMAT=false
SKIP_CTEST=false
SKIP_PYTHON=false
REPORT_ONLY=false

usage() {
    grep '^#' "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -j) NUM_JOBS="$2"; shift 2 ;;
        -b) BUILD_DIR_BASE="$2"; shift 2 ;;
        --skip-format) SKIP_FORMAT=true; shift ;;
        --skip-ctest)  SKIP_CTEST=true;  shift ;;
        --skip-python) SKIP_PYTHON=true; shift ;;
        --report-only) REPORT_ONLY=true; shift ;;
        -h|--help) usage ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ -z "$NUM_JOBS" ]]; then
    NUM_JOBS=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4)
fi

############################
# Locate cmake and ctest
############################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

find_cmake_ctest() {
    if [[ -f /opt/mongodbtoolchain/v5/bin/cmake ]]; then
        CMAKE=/opt/mongodbtoolchain/v5/bin/cmake
        CTEST=/opt/mongodbtoolchain/v5/bin/ctest
        # Make the toolchain available to subprocesses (needed for cmake in setup_actions).
        export PATH="/opt/mongodbtoolchain/v5/bin:$PATH"
    elif command -v cmake &>/dev/null; then
        CMAKE=cmake
        CTEST=ctest
    else
        echo "ERROR: cmake not found. Install mongodbtoolchain or cmake." >&2
        exit 1
    fi
    export CMAKE CTEST
    echo "cmake: $CMAKE"
    echo "ctest: $CTEST"
}

find_cmake_ctest

############################
# Helper: set up GCOV env
############################
# Point GCOV_PREFIX to a specific build directory so that .gcda files from
# binaries run outside of parallel_code_coverage.py land in the right place.
# build_dir must be the absolute path to the target build directory.
setup_gcov_prefix() {
    local build_dir="$1"
    local path_depth
    path_depth=$(echo "$build_dir" | tr -cd '/' | wc -c)
    export GCOV_PREFIX_STRIP="$path_depth"
    export GCOV_PREFIX="$build_dir"
    echo "GCOV_PREFIX=$GCOV_PREFIX  GCOV_PREFIX_STRIP=$GCOV_PREFIX_STRIP"
}

unset_gcov_prefix() {
    unset GCOV_PREFIX GCOV_PREFIX_STRIP
}

############################
# Phase 1: Python + catch2
############################
run_python_tests() {
    echo ""
    echo "=== Phase 1: Python / catch2 disagg tests (parallel) ==="

    cd "$WT_ROOT"

    # parallel_code_coverage.py -s builds build_0/ from the JSON setup_actions,
    # then copies it to build_1/ ... build_N/.  Each worker process changes to
    # its own build_N/ directory and sets GCOV_PREFIX so that .gcda files land
    # in the correct copy.
    #
    # Python tests find libwiredtiger via sys.path (run.py adds build_N/lang/python)
    # and find PALite via WiredTigerTestCase.findExtension() scanning build_N/ext/.
    # No explicit LD_LIBRARY_PATH is needed for Python; the .so uses rpath.
    python3 test/evergreen/code_coverage/parallel_code_coverage.py \
        -c test/evergreen/code_coverage/code_coverage_config_disagg.json \
        -b "${WT_ROOT}/${BUILD_DIR_BASE}" \
        -j "$NUM_JOBS" \
        -s \
        -v

    echo "Phase 1 done."
}

############################
# Phase 2: Format stress
############################
run_format_tests() {
    echo ""
    echo "=== Phase 2: Format stress tests with CONFIG.disagg ==="

    # Format tests are run from the base build directory (build_0) so that .gcda
    # files accumulate there and are picked up by gcovr.
    # The format binary locates PALite via the compile-time BUILDDIR constant
    # (set to build_0 during cmake), so no EXT or LD_LIBRARY_PATH is needed.
    # GCOV_PREFIX is set so coverage data from this phase goes to build_0.
    local base_build="${WT_ROOT}/${BUILD_DIR_BASE}0"
    setup_gcov_prefix "$base_build"

    local format_bin="$base_build/test/format/t"
    local config_disagg="$WT_ROOT/test/format/CONFIG.disagg"

    if [[ ! -x "$format_bin" ]]; then
        echo "WARNING: format test binary not found at $format_bin, skipping format tests."
        unset_gcov_prefix
        return
    fi

    # Leader mode — short run appropriate for coverage measurement.
    # -h sets the home (RUNDIR) directory; keep it inside base_build so gcovr
    # can find any .gcda files created there alongside the binary's own .gcda files.
    echo "--- Format leader mode ---"
    local format_home_leader="$base_build/RUNDIR_disagg_cov_leader"
    rm -rf "$format_home_leader"

    "$format_bin" \
        -h "$format_home_leader" \
        -c "$config_disagg" \
        disagg.mode=leader \
        runs.rows=10000 \
        runs.ops=50000 \
        runs.timer=2:5 \
        || echo "WARNING: format leader run failed (partial coverage still recorded)"

    # Reopen run (-R) exercises crash-recovery and checkpoint-replay code paths.
    echo "--- Format leader reopen (-R) ---"
    "$format_bin" -R -h "$format_home_leader" \
        || echo "WARNING: format leader reopen failed (partial coverage still recorded)"

    # Follower mode: in a standalone run, no leader checkpoint data is available,
    # so this covers follower startup/shutdown code paths only.
    echo "--- Format follower mode ---"
    local format_home_follower="$base_build/RUNDIR_disagg_cov_follower"
    rm -rf "$format_home_follower"

    "$format_bin" \
        -h "$format_home_follower" \
        -c "$config_disagg" \
        disagg.mode=follower \
        runs.rows=10000 \
        runs.ops=50000 \
        runs.timer=2:5 \
        || echo "WARNING: format follower run failed (partial coverage still recorded)"

    unset_gcov_prefix
}

############################
# Phase 3: Checkpoint ctest
############################
run_checkpoint_ctest() {
    echo ""
    echo "=== Phase 3: Checkpoint tests with check_disagg ctest label ==="

    local base_build="${WT_ROOT}/${BUILD_DIR_BASE}0"

    if [[ ! -d "$base_build" ]]; then
        echo "WARNING: build directory $base_build not found, skipping ctest."
        return
    fi

    # GCOV_PREFIX directs .gcda files from the ctest binaries into base_build.
    setup_gcov_prefix "$base_build"

    # ctest must run from the cmake build directory so it can locate CTestTestfile.cmake.
    cd "$base_build"
    "$CTEST" -L "check_disagg" --output-on-failure \
        || echo "WARNING: some check_disagg ctest tests failed (partial coverage recorded)"
    cd "$WT_ROOT"

    unset_gcov_prefix
}

############################
# Phase 4: Coverage report
############################
run_gcovr() {
    echo ""
    echo "=== Phase 4: Generating gcovr HTML + JSON report ==="

    cd "$WT_ROOT"

    # code_coverage_analysis.sh reads time.txt (start/end epoch seconds) to
    # report total test duration.  Create it if it doesn't exist.
    if [[ ! -f time.txt ]]; then
        echo "0" > time.txt
        echo "0" >> time.txt
    fi

    ./test/evergreen/code_coverage_analysis.sh \
        src \
        "$NUM_JOBS" \
        python3

    echo ""
    echo "Report written to: $WT_ROOT/coverage_report/"
    echo "  HTML:    coverage_report/2_coverage_report.html"
    echo "  Summary: coverage_report/1_coverage_report_summary.json"
    echo "  Full:    coverage_report/full_coverage_report.json"
}

############################
# Main
############################
cd "$WT_ROOT"

echo "Disagg Coverage Runner"
echo "======================"
echo "  Repo root:        $WT_ROOT"
echo "  Build dir base:   ${BUILD_DIR_BASE}"
echo "  Parallel workers: $NUM_JOBS"
echo "  Skip format:      $SKIP_FORMAT"
echo "  Skip ctest:       $SKIP_CTEST"
echo "  Skip python:      $SKIP_PYTHON"
echo "  Report only:      $REPORT_ONLY"
echo ""

if [[ "$REPORT_ONLY" != "true" ]]; then
    if [[ "$SKIP_PYTHON" != "true" ]]; then
        run_python_tests
    fi

    if [[ "$SKIP_FORMAT" != "true" ]]; then
        run_format_tests
    fi

    if [[ "$SKIP_CTEST" != "true" ]]; then
        run_checkpoint_ctest
    fi
fi

run_gcovr
