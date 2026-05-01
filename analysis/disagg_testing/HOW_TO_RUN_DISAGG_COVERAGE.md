# How to Run Disagg Coverage

> Goal: Build WiredTiger with gcov instrumentation, run all disagg test types,
> and generate an HTML/JSON coverage report showing which `src/` code paths are
> exercised by disagg testing.

---

## Quick Start (Local)

Run from the **repository root**:

```bash
test/evergreen/code_coverage/disagg_coverage.sh -j $(nproc)
```

When finished, open `coverage_report/2_coverage_report.html` in a browser.

---

## What the Script Does

`test/evergreen/code_coverage/disagg_coverage.sh` runs four phases:

| Phase | What runs | Config / binary |
|---|---|---|
| 1 | Python + catch2 tests (parallel) | `code_coverage_config_disagg.json` via `parallel_code_coverage.py` |
| 2 | Format stress tests with `CONFIG.disagg` | `test/format/t` in leader and follower modes |
| 3 | Checkpoint ctest with `check_disagg` label | `ctest -L check_disagg` |
| 4 | gcovr report generation | `code_coverage_analysis.sh` → `gcovr 5.0` |

### Phase 1 — Python tests covered

- **test_layered\*.py** (103 files) — native layered table tests
- **test_disagg\*.py** (10 files) — disagg-specific tests including checkpoint-size and key-provider
- **hook leader** — all `test_*.py` files not in `hook_disagg.fail`, run via `--hook disagg=(role=leader)`
- **hook follower** — `base01` group run via `--hook disagg=(role=follower)`
- **catch2-unittests** — includes `test_disagg_meta_config` (metadata parsing unit tests)

### Phase 2 — Format stress covered

- **Leader mode**: `disagg.mode=leader runs.rows=10000 runs.ops=50000 runs.timer=2:5`
- **Leader reopen** (`-R`): exercises crash-recovery code paths
- **Follower mode**: `disagg.mode=follower` (standalone; covers startup/shutdown paths)

> **Note on follower coverage**: In production, the format follower reads from a
> running leader's page log.  Running standalone gives only startup/shutdown
> coverage.  Full follower code-path coverage requires running the multi-instance
> `format.sh` wrapper (used by the pull-request CI tasks).  This is not yet
> automated in the coverage script.

### Phase 3 — Checkpoint ctest covered

Runs all ctest tests labeled `check_disagg` in the `test/checkpoint/` suite.
These are excluded from the normal `checkpoint-test` task by `-LE check_disagg`.

---

## Prerequisites

| Requirement | Notes |
|---|---|
| **GCC with gcov** | Provided by mongodbtoolchain v5 (`/opt/mongodbtoolchain/v5/bin/gcc`) |
| **Ninja** | Also in mongodbtoolchain |
| **python3, virtualenv** | gcovr is installed into a virtualenv automatically |
| **cmake / ctest** | Script searches standard locations (mongodbtoolchain first) |

On a developer laptop without mongodbtoolchain, make sure `cmake`, `ctest`, `ninja`,
and a GCC with gcov support are on `$PATH`.

---

## Script Options

```
test/evergreen/code_coverage/disagg_coverage.sh [options]

  -j N              Parallel worker count (default: nproc)
  -b DIR_BASE       Build directory base name (default: build_disagg_cov_)
  --skip-format     Skip format stress tests
  --skip-ctest      Skip checkpoint ctest
  --skip-python     Skip Python / catch2 parallel tests
  --report-only     Regenerate report from existing .gcda files (no tests run)
```

### Examples

```bash
# Full run on an 8-core machine
test/evergreen/code_coverage/disagg_coverage.sh -j 8

# Just the Python tests (fastest meaningful run)
test/evergreen/code_coverage/disagg_coverage.sh --skip-format --skip-ctest

# Only re-generate the HTML report (all tests already ran)
test/evergreen/code_coverage/disagg_coverage.sh --report-only

# Use a different build dir name to keep results separate
test/evergreen/code_coverage/disagg_coverage.sh -b build_my_disagg_cov_
```

---

## Build Directories

The script calls `parallel_code_coverage.py -s`, which:

1. Creates `${BUILD_DIR_BASE}0/` and compiles from source inside it (takes ~10 min)
2. Copies it N times to `${BUILD_DIR_BASE}1/`, `${BUILD_DIR_BASE}2/`, ... (one per worker)
3. Runs tests across the copies in parallel using `GCOV_PREFIX` / `GCOV_PREFIX_STRIP`

After the run you will find:
```
build_disagg_cov_0/   ← base build + .gcda files from this worker's tests + format/ctest phases
build_disagg_cov_1/   ← copy + .gcda files from this worker's tests
...
coverage_report/      ← gcovr output (HTML + JSON)
```

To clean up after a run:
```bash
rm -rf build_disagg_cov_*/
rm -rf coverage_report/
```

---

## Running in Evergreen CI

The task `coverage-report-disagg` is defined in `test/evergreen.yml` and wired
to the `code-statistics` build variant in `test/evergreen_develop.yml`.

It is set **`activate: false`** by default (too slow for every commit) and must
be triggered manually.

### Triggering manually

In the Evergreen UI:
1. Navigate to a build for the `code-statistics` variant.
2. Click **coverage-report-disagg** → **Run task**.

Or with the Evergreen CLI:
```bash
evergreen patch -p wiredtiger -v ubuntu2004-arm64 --task coverage-report-disagg
```

### What the Evergreen task does

```yaml
- get project          # clone the source tree
- disagg_coverage.sh   # all four phases (Python, format, ctest, gcovr)
- publish HTML report  # uploaded to S3 under coverage_report/
```

The task runs on `ubuntu2004-arm64-large` (same as the other coverage tasks) to
give it enough memory for the parallel build copies.

---

## Files Involved

| File | Purpose |
|---|---|
| `test/evergreen/code_coverage/code_coverage_config_disagg.json` | Python + catch2 test list for `parallel_code_coverage.py` |
| `test/evergreen/code_coverage/disagg_coverage.sh` | Orchestration: build → test → report |
| `test/evergreen.yml` | Task definition (`coverage-report-disagg`) |
| `test/evergreen_develop.yml` | Task wired to `code-statistics` build variant |
| `test/evergreen/code_coverage_analysis.sh` | gcovr invocation (shared with normal coverage) |

---

## Coverage Gaps (Not Yet Automated)

The following disagg test types are **not** yet included in the automated coverage run:

| Test type | Why excluded | How to add |
|---|---|---|
| Full follower format run (multi-instance) | Requires running leader and follower simultaneously via `format.sh` | Implement multi-process wrapper in disagg_coverage.sh |
| Model checker with disagg (`model-test-long-disagg`) | Very long (1 hr), needs special test binary | Add as a separate `--skip-model` flag phase |
| timestamp_abort with disagg (`-G -s`) | Expected to fail (in `wiredtiger-disagg` project) | Add as optional `--include-failure-expected` phase |
| Switch / multi mode format tests | Expected to fail (in `wiredtiger-disagg` project) | Same |
| cppsuite disagg failover perf | Performance test, not correctness coverage | Run separately with coverage flags |
| Disagg hook follower (full test suite) | Many tests skip as follower; CI only runs `base01` | Consider adding more groups if follower coverage improves |

---

## Interpreting the Report

Focus on **`src/block_disagg/`** first — this is the primary disagg-specific source
directory and should have near-complete coverage from Phase 1.

Key directories to check:
- `src/block_disagg/` — disagg block manager (should be well covered)
- `src/tiered/` — layered table implementation (covered by test_layered*) 
- `src/btree/` — btree accessed through disagg paths (covered by hook tests)
- `src/checkpoint/` — checkpoint paths (covered by format + ctest phases)
- `src/reconcile/` — reconciliation code (covered by write workloads in all phases)
- `ext/page_log/palite/` — PALite (covered by all Python and format tests)

Lines or branches with zero coverage in these directories indicate code paths
that none of the current disagg tests exercise — these are the gaps to investigate.
