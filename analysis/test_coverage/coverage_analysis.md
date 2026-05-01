# WiredTiger Code Coverage Analysis

> Generated: 2026-05-01 from `test/evergreen.yml` and `test/evergreen_develop.yml`

---

## Overview

WiredTiger uses **gcov/gcovr** (GCC instrumentation) to measure line and branch coverage of C source code under `src/`. There are five distinct coverage CI tasks, plus one compile-only check. Coverage is reported as HTML to S3 and as JSON to MongoDB Atlas for trend tracking.

---

## Coverage Tasks in CI

### 1. `coverage-report-python` (task, `test/evergreen.yml` line 4231)

| Property | Value |
|---|---|
| Tag | `pull_request_code_statistics` |
| Build variant | `code-statistics` (see `evergreen_develop.yml`) |
| Host | ubuntu2004-arm64-large |
| Scope | All of `src/` |
| Test bucket | Python tests only (`--bucket python`) |

**Pipeline:**
1. `parallel_code_coverage.py -c code_coverage_config.json --bucket python`
2. `code_coverage_analysis.sh src` (gcovr, generates HTML + JSON)
3. Uploads artifact `coverage-report-python.tgz` for downstream combine step

---

### 2. `coverage-report-other` (task, line 4260)

| Property | Value |
|---|---|
| Tag | `pull_request_code_statistics` |
| Build variant | `code-statistics` |
| Host | ubuntu2004-arm64-large |
| Scope | All of `src/` |
| Test bucket | Non-python tests (`--bucket other`) |

**Pipeline:** Same as above but `--bucket other`.

---

### 3. `generate-coverage-report` (task, line 4289)

| Property | Value |
|---|---|
| Tag | `pull_request_code_statistics` |
| Depends on | `coverage-report-python`, `coverage-report-other` |
| Build variant | `code-statistics` |

**Pipeline:**
1. Downloads both `.tgz` artifacts from the two parallel tasks
2. `code_coverage_analysis.sh` with `combine_coverage_report=true` — merges two gcovr JSON tracefiles with `--add-tracefile`
3. Uploads combined stats to Atlas (`WTCodeStatisticsDB`, collection `CodeCoverage`)
4. Publishes HTML to S3 as public artifact

This is the authoritative combined coverage report for every develop commit and every PR.

---

### 4. `coverage-report-catch2` (task, line 4341)

| Property | Value |
|---|---|
| Tag | `pull_request_code_statistics` |
| Build variant | `code-statistics` |
| Scope | All of `src/` |
| Config file | `code_coverage_config_catch2.json` |
| num_jobs | 1 (serial) |
| `--check_errors` | Yes — task fails on any test error |

**Purpose:** Isolated view of coverage contributed solely by the Catch2 unit test suite. Useful for understanding how much the unit tests alone cover vs. the full test set.

**Test run:** `test/catch2/catch2-unittests` (only this one binary).

---

### 5. `coverage-report-per-test` (task, line 4320)

| Property | Value |
|---|---|
| Build variant | `code-statistics` |
| Host | ubuntu2004-arm64-large |
| Activation | **Disabled by default** (`activate: false`) — must be triggered manually |

**Purpose:** Per-test granularity — which test covers which code paths. Uses the same `code_coverage_config.json` test list but captures a gcovr snapshot after each test individually, then runs gcovr on each copy.

**Additional work (in patch builds):**
- Runs Metrix++ on `src/` to collect cyclomatic complexity
- Generates a git diff of the patch
- Produces `per_test_code_coverage_report.py` — a mapping of changed lines to which tests reach them

---

### 6. `code-change-report` (task, line 4358)

| Property | Value |
|---|---|
| Tag | `pull_request_code_statistics` |
| Depends on | `generate-coverage-report` |
| Build variant | `code-statistics` |

**Purpose:** Annotates a PR with how much of the changed code is covered. For PRs it posts a GitHub comment with a link to the report.

**Pipeline:**
1. Downloads `full_coverage_report.json` from `generate-coverage-report`
2. (Patch build) Generates git diff and Metrix++ complexity metrics for both current and previous commit
3. `coverage-report.sh` → `code_change_info.py` + `code_change_report.py`
4. Publishes HTML report to S3

---

### 7. `model-test-long-with-coverage` (task, line 5243)

| Property | Value |
|---|---|
| Tag | `model_checking` |
| Build variants | `ubuntu2004`, `ubuntu2004-arm64`, `amazon2023-arm64` |
| Frequency | Daily (`batchtime: 1440`) |
| Coverage scope | **`src/rollback_to_stable` only** (not all of `src/`) |
| Build flags | `CMAKE_BUILD_TYPE=Coverage`, `CODE_COVERAGE_MEASUREMENT=1`, `INLINE_FUNCTIONS_INSTEAD_OF_MACROS=1`, `HAVE_DIAGNOSTIC=0` |

**Purpose:** Focused coverage measurement for the rollback-to-stable component, driven by the model checker (not the Python/C test suite).

**Test run:** `model_test -l 2000-3000 -t 3600` (runs for 60 minutes with a randomised workload range).

---

### Compile Check Only: `compile-uncommon-build-flags` (task, line 1905)

| Property | Value |
|---|---|
| Tags | `pull_request`, `pull_request_compilers` |

Compiles with `CMAKE_BUILD_TYPE=Coverage` to catch compile errors specific to the coverage build (which enables macros→inline substitutions). **Does not run tests or collect coverage data.**

---

## Build Configuration for Coverage

All coverage builds use:

```
cmake -DHAVE_UNITTEST=1 \
      -DHAVE_DIAGNOSTIC=0 \
      -DCODE_COVERAGE_MEASUREMENT=1 \
      -DINLINE_FUNCTIONS_INSTEAD_OF_MACROS=1 \
      -DCMAKE_BUILD_TYPE=Coverage \
      -DCMAKE_TOOLCHAIN_FILE=../cmake/toolchains/mongodbtoolchain_stable_gcc.cmake \
      -G Ninja
```

- `CMAKE_BUILD_TYPE=Coverage` disables optimisations and enables `-fprofile-arcs -ftest-coverage` (gcov instrumentation)
- `INLINE_FUNCTIONS_INSTEAD_OF_MACROS=1` changes some performance-sensitive macros into real functions, making them show up in coverage
- `HAVE_DIAGNOSTIC=0` avoids the overhead of diagnostic builds

---

## Parallelism Strategy

Tests in `code_coverage_config.json` are split across **N build directories** (N = `num_jobs`):

1. A base build dir is compiled once, then `shutil.copytree`'d to create N copies
2. `GCOV_PREFIX` and `GCOV_PREFIX_STRIP` environment variables redirect `.gcda` runtime data into each copy's directory
3. Tests run concurrently via `concurrent.futures.ProcessPoolExecutor` — each worker operates in its own build directory
4. `gcovr` is run once across all build directories and automatically aggregates `.gcda` from all of them

The split python/other approach runs two machines in parallel, each doing roughly half the test list, cutting total time roughly in half.

---

## Test List: `code_coverage_config.json`

This config drives `coverage-report-python`, `coverage-report-other`, and `coverage-report-per-test`.

### Setup actions (run in each build dir before tests)

```
cmake ...  (coverage build)
ninja -j 16
mkdir WT_HOME_COVERAGE
examples/c/ex_hello/ex_hello
./wt -h WT_HOME_COVERAGE load -j -f test/evergreen/code_coverage/test_table.json
```

### Python tests (routed to `coverage-report-python`)

| Test | Notes |
|---|---|
| `test/suite/run.py test_overwrite` | |
| `test/suite/run.py test_salvage01` | |
| `test/suite/run.py cursor_random` | |
| `test/suite/run.py test_timestamp10` | |
| `test/suite/run.py test_alter02` | |
| `test/suite/run.py test_checkpoint_snapshot03` | |
| `test/suite/run.py test_rollback_to_stable22.py -s 1` | |
| `test/suite/run.py test_util19` | |
| `test/suite/run.py test_util02` | |
| `test/suite/run.py test_prefetch02` | |
| `test/suite/run.py test_tiered08` | |
| `test/suite/run.py test_bulk01` | |
| `test/suite/run.py test_sweep01` | |
| `test/suite/run.py test_shared_cache01` | (appears twice in config) |
| `test/suite/run.py test_cursor_bound10` | |
| `test/suite/run.py log` | |
| `test/suite/run.py test_log03` | |
| `test/suite/run.py test_tiered04` | |
| `test/suite/run.py test_txn15` | |
| `test/suite/run.py cursor_pin` | |
| `test/suite/run.py test_backup06` | |
| `test/suite/run.py test_reserve` | |
| `test/suite/run.py test_encrypt04` | |
| `test/suite/run.py test_intpack` | |
| `test/suite/run.py test_prepare_hs03` | |
| `test/suite/run.py test_reconfig03` | |
| `test/suite/run.py test_cursor_compare` | |
| `test/suite/run.py test_hs06` | |
| `test/suite/run.py test_util13` | |
| `test/suite/run.py test_colgap` | |
| `test/suite/run.py test_truncate24` | |
| `test/suite/run.py test_drop` | |
| `test/suite/run.py test_util18` | |
| `test/suite/run.py test_util01` | |
| `test/suite/run.py test_prepare01` | |
| `test/suite/run.py test_durable_rollback_to_stable` | |
| `test/suite/run.py test_durability01` | |
| `test/suite/run.py test_truncate15` | |
| `test/suite/run.py test_truncate02` | |
| `test/suite/run.py test_cc04` | |
| `test/suite/run.py cursor_compare` | |
| `test/suite/run.py test_stat_log01` | |
| `test/suite/run.py test_util12` | |
| `test/suite/run.py test_durable_ts02` | |
| `test/suite/run.py test_stat06` | |
| `test/suite/run.py test_rollback_to_stable43.py -s 3` | |
| `test/suite/run.py test_calc_modify` | |
| `test/suite/run.py test_util03` | |
| `test/suite/run.py test_util14` | |
| `test/suite/run.py test_jsondump01` | |
| `test/suite/run.py test_rollback_to_stable13.py -s 1` | |
| `test/suite/run.py cursor_bound -s 1` | |
| `test/suite/run.py test_util11` | |
| `test/suite/run.py test_prepare15` | |
| `test/suite/run.py test_rollback_to_stable02.py -s 1` | |
| `test/suite/run.py test_checkpoint05` | |
| `test/suite/run.py test_util09` | |
| `test/suite/run.py test_backup13` | |
| `test/suite/run.py test_metadata_cursor02` | |
| `test/suite/run.py cursor_bound_fuzz` | |
| `test/suite/run.py test_rename` | |
| `test/suite/run.py test_compact04` | |
| `test/suite/run.py test_util07` | |
| `test/suite/run.py test_rollback_to_stable34.py -s 1` | |
| `test/suite/run.py test_prepare04` | |
| `test/suite/run.py test_rollback_to_stable04.py -s 1` | |
| `test/suite/run.py test_dupc` | |
| `test/suite/run.py test_rollback_to_stable36.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable08.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable07.py -s 1` | |
| `test/suite/run.py test_util15` | |
| `test/suite/run.py test_util21` | |
| `test/suite/run.py test_rollback_to_stable01.py -s 1` | |
| `test/suite/run.py test_util16` | |
| `test/suite/run.py test_util17` | |
| `test/suite/run.py test_rollback_to_stable06.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable29.py -s 1` | |
| `test/suite/run.py test_verbose02` | |
| `test/suite/run.py test_rollback_to_stable23.py -s 1` | |
| `test/suite/run.py test_search_near02` | |
| `test/suite/run.py test_rollback_to_stable18.py -s 1` | |
| `test/suite/run.py test_inmem02` | |
| `test/suite/run.py test_cursor08` | |
| `test/suite/run.py test_rollback_to_stable15.py -s 1` | |
| `test/suite/run.py test_prefetch01` | |
| `test/suite/run.py test_rollback_to_stable32.py -s 1` | |
| `test/suite/run.py test_export01` | |
| `test/suite/run.py test_import01` | |
| `test/suite/run.py test_util04` | |
| `test/suite/run.py test_autoclose` | |
| `test/suite/run.py test_rollback_to_stable40.py -s 1` | |
| `test/suite/run.py test_assert06` | |
| `test/suite/run.py test_import08` | |
| `test/suite/run.py test_index02` | |
| `test/suite/run.py test_prepare03` | |
| `test/suite/run.py test_rollback_to_stable05.py -s 1` | |
| `test/suite/run.py test_base04` | |
| `test/suite/run.py test_pack` | |
| `test/suite/run.py test_rollback_to_stable19.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable03.py -s 1` | |
| `test/suite/run.py test_config06` | |
| `test/suite/run.py test_collator` | |
| `test/suite/run.py test_rollback_to_stable11.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable42.py -s 1` | |
| `test/suite/run.py test_prepare_cursor01` | |
| `test/suite/run.py test_rollback_to_stable16.py -s 1` | |
| `test/suite/run.py test_backup24` | |
| `test/suite/run.py test_rollback_to_stable17.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable24.py -s 1` | |
| `test/suite/run.py test_join08` | |
| `test/suite/run.py test_excl` | |
| `test/suite/run.py test_empty_value` | |
| `test/suite/run.py test_env01` | |
| `test/suite/run.py test_rollback_to_stable41.py -s 1` | |
| `test/suite/run.py test_verify2` | |
| `test/suite/run.py test_rollback_to_stable30.py -s 1` | |
| `test/suite/run.py test_hazard` | |
| `test/suite/run.py test_drop_create` | |
| `test/suite/run.py test_home` | |
| `test/suite/run.py test_rollback_to_stable33.py -s 1` | |
| `test/suite/run.py test_rollback_to_stable09.py -s 1` | |
| `test/suite/run.py test_debug_mode03` | |
| `test/suite/run.py test_isolation01` | |
| `test/suite/run.py test_rollback_to_stable31.py -s 1` | |
| `test/suite/run.py test_unicode01` | |
| `test/suite/run.py test_empty` | |
| `test/suite/run.py test_turtle01` | |
| `test/suite/run.py test_rollback_to_stable27.py -s 1` | |
| `test/suite/run.py test_split` | |
| `test/suite/run.py -p test_prepare02` | |
| `test/suite/run.py test_readonly03` | |
| `test/suite/run.py test_bug005` | |
| `test/suite/run.py test_baseconfig` | |
| `test/suite/run.py test_util08` | |
| `test/suite/run.py test_version` | |
| `test/suite/run.py test_rollback_to_stable25.py -s 1` | |
| `test/suite/run.py test_dictionary` | |
| `test/suite/run.py test_sweep04` | |
| `test/suite/run.py test_util10` | |
| `test/suite/run.py test_util06` | |
| `test/suite/run.py test_cursor_tracker` | |
| `test/suite/run.py test_util20` | |
| `test/suite/run.py test_rollback_to_stable28.py -s 1` | |
| `test/suite/run.py test_util05` | |
| `test/suite/run.py test_schema07` | |
| `test/suite/run.py test_compress02` | |
| `test/suite/run.py test_dump04` | |
| `test/suite/run.py test_live_restore01` | |
| `test/suite/run.py test_key_provider_disagg01.py` | |
| `test/suite/run.py test_key_provider_disagg02.py` | |
| `test/suite/run.py --hook disagg --skip-tests-in-file ../test/suite/hook_disagg.fail base` | disagg hook |

### Non-python / "other" tests (routed to `coverage-report-other`)

| Test | Notes |
|---|---|
| `test/csuite/wt8659_reconstruct_database_from_logs/test_wt8659_reconstruct_database_from_logs` | |
| `test/cppsuite/run -t hs_cleanup -f test/cppsuite/configs/hs_cleanup_default.txt` | |
| `test/cppsuite/run -t bounded_cursor_stress -f test/cppsuite/configs/bounded_cursor_stress_default.txt` | |
| `test/cppsuite/run -t cache_resize -f test/cppsuite/configs/cache_resize_default.txt` | |
| `test/catch2/catch2-unittests` | |
| `test/csuite/incr_backup/test_incr_backup -S 123456789` | |
| `test/csuite/wt6185_modify_ts/test_wt6185_modify_ts` | |
| `test/csuite/wt2999_join_extractor/test_wt2999_join_extractor` | |
| `test/csuite/wt4105_large_doc_small_upd/test_wt4105_large_doc_small_upd` | |
| `examples/c/ex_all/ex_all` | |
| `test/csuite/incr_backup/test_incr_backup -S 0x9b1bde3f111fe316` | |
| `test/csuite/wt2695_checksum/test_wt2695_checksum` | |
| `test/csuite/wt10897_compact_quick_interrupt/test_wt10897_compact_quick_interrupt` | |
| `test/csuite/wt4156_metadata_salvage/test_wt4156_metadata_salvage` | (appears twice) |
| `test/csuite/wt2447_join_main_table/test_wt2447_join_main_table` | |
| `test/csuite/wt3135_search_near_collator/test_wt3135_search_near_collator` | |
| `test/csuite/wt1965_col_efficiency/test_wt1965_col_efficiency` | |
| `examples/c/ex_hello/ex_hello` | (setup + test bucket — may run twice) |
| `test/csuite/wt3874_pad_byte_collator/test_wt3874_pad_byte_collator` | |
| `test/csuite/wt4891_meta_ckptlist_get_alloc/test_wt4891_meta_ckptlist_get_alloc` | |
| `test/csuite/wt4699_json/test_wt4699_json` | |
| `test/csuite/wt3184_dup_index_collator/test_wt3184_dup_index_collator` | |
| `examples/c/ex_schema/ex_schema` | |
| `test/csuite/wt2592_join_schema/test_wt2592_join_schema` | |
| `test/csuite/wt3120_filesys/test_wt3120_filesys -b $(pwd)` | |
| `test/csuite/wt9937_parse_opts/test_wt9937_parse_opts` | |
| `test/csuite/wt3363_checkpoint_op_races/test_wt3363_checkpoint_op_races` | |
| `test/csuite/wt4117_checksum/test_wt4117_checksum` | |
| `ctest -R '(ex_\|filesys\|metadata_salvage\|col_efficiency\|...)` | Subset of ctest targets |
| `./wt -h WT_HOME_COVERAGE <various subcommands>` | Exercises wt CLI: create, list, dump, verify, load, backup, stat, write, read, alter, salvage, compact, rename, printlog, truncate, downgrade, copyright, loadtext, drop |

### Test list for `coverage-report-catch2`

| Test |
|---|
| `test/catch2/catch2-unittests` |

### Test for `model-test-long-with-coverage`

| Test | Scope |
|---|---|
| `model_test -l 2000-3000 -t 3600` | `src/rollback_to_stable` only |

---

## What Is NOT Covered in CI Coverage Runs

The following test types exist in the repo but are not included in any coverage task:

- **Format test** (`test/format/t`) — there is `test/format/CONFIG.coverage` (a coverage-friendly config) but no CI task uses it
- **Fuzz tests** — `test/fuzz/fuzz_coverage.sh` exists for manual local use; fuzz testing uses LLVM instrumentation (not gcov), so it's a different tool entirely
- **Full Python test suite** — the coverage config runs a curated subset (~130 specific test files), not the complete `test/suite/` suite
- **RTS tests 10, 12, 14, 20, 26, 35, 37, 38, 39** — explicitly excluded from the coverage config (too slow)
- **TSAN/ASAN/sanitizer builds** — have their own build variants, no coverage measurement
- **Performance tests** — perf variants run on `develop` branch only, no coverage
- **Disaggregated storage stress tests** — separate `evergreen_disagg.yml`, no coverage

---

## RTS Coverage Note

The `model-test-long-with-coverage` task uses a **narrowed coverage filter** (`src/rollback_to_stable`) to focus on that subsystem specifically. The `generate-coverage-report` uses the full `src` filter. This means RTS has two separate coverage data points: one from model-checker-driven testing and one from the broader test suite.

---

## Key Files Reference

| File | Role |
|---|---|
| [test/evergreen.yml](../evergreen.yml) | Main CI config — all tasks and functions defined here |
| [test/evergreen_develop.yml](../evergreen_develop.yml) | Develop-only CI; defines the `code-statistics` build variant |
| [test/evergreen/code_coverage/parallel_code_coverage.py](../evergreen/code_coverage/parallel_code_coverage.py) | Runs tests in parallel across multiple build directories |
| [test/evergreen/code_coverage/per_test_code_coverage.py](../evergreen/code_coverage/per_test_code_coverage.py) | Captures per-test coverage snapshots |
| [test/evergreen/code_coverage/code_coverage_utils.py](../evergreen/code_coverage/code_coverage_utils.py) | Shared: build dir setup/check, parallel executor |
| [test/evergreen/code_coverage_analysis.sh](../evergreen/code_coverage_analysis.sh) | Installs gcovr, runs it, optionally combines two tracefiles |
| [test/evergreen/code_coverage_analysis.py](../evergreen/code_coverage_analysis.py) | Processes gcovr JSON summary into Atlas format |
| [test/evergreen/coverage-report.sh](../evergreen/coverage-report.sh) | Code-change-report pipeline (diff + Metrix++ + report) |
| [test/evergreen/code_coverage/coverage-report-per-test.sh](../evergreen/code_coverage/coverage-report-per-test.sh) | Per-test coverage pipeline entry point |
| [test/evergreen/code_coverage/code_coverage_config.json](../evergreen/code_coverage/code_coverage_config.json) | Master test list for main coverage run |
| [test/evergreen/code_coverage/code_coverage_config_catch2.json](../evergreen/code_coverage/code_coverage_config_catch2.json) | Catch2-only test list |
| [test/fuzz/fuzz_coverage.sh](../fuzz/fuzz_coverage.sh) | Manual script for LLVM fuzz coverage (not in CI) |
| [test/format/CONFIG.coverage](../format/CONFIG.coverage) | Format test config tuned for coverage speed (not in CI) |
