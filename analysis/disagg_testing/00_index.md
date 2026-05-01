# WiredTiger Disaggregated Storage — CI Testing Analysis

> Generated: 2026-05-01 from `test/evergreen.yml`, `test/evergreen_disagg.yml`

---

## What Is "Disagg"?

"Disaggregated storage" (disagg) is a WiredTiger architecture where the btree storage layer is decoupled — a **leader** node writes checkpoints to a shared page log, and **follower** nodes read from that page log. The implementation uses a **layered table** type (URI prefix `layered:`) backed by two constituent btrees: a stable btree (from checkpoint) and an ingest btree (for new writes).

Disagg is distinct from tiered storage (S3/Azure/GCP blob backends). Disagg uses a page-log abstraction; the default implementation for testing is **PALite** (`palite`).

---

## Test Categories

| File | Category | Description |
|---|---|---|
| [01_python_hook_tests.md](01_python_hook_tests.md) | Python suite with disagg hook | Entire Python test suite re-run under `--hook disagg=(role=leader)` |
| [02_test_layered_tests.md](02_test_layered_tests.md) | Dedicated layered table tests | ~103 `test_layered*.py` files — disagg-native Python tests |
| [03_test_disagg_specific_tests.md](03_test_disagg_specific_tests.md) | Disagg-specific unit tests | `test_disagg*.py`, `test_verify_disagg*.py`, `test_leaf_delta_disagg*.py`, `test_key_provider_disagg*.py` |
| [04_format_stress_tests.md](04_format_stress_tests.md) | Format stress tests | `test/format/t` run with `CONFIG.disagg` in leader, follower, switch, and multi modes |
| [05_model_checker_tests.md](05_model_checker_tests.md) | Model checker tests | Formal model checking with disaggregated storage enabled |
| [06_checkpoint_and_csuite_tests.md](06_checkpoint_and_csuite_tests.md) | Checkpoint + csuite tests | ctest with `check_disagg` label; timestamp abort disagg |
| [07_cppsuite_and_catch2_tests.md](07_cppsuite_and_catch2_tests.md) | C++ suite and unit tests | Failover perf test (cppsuite), metadata config unit test (Catch2) |
| [08_failure_expected_tests.md](08_failure_expected_tests.md) | Failure-Expected tests | `evergreen_disagg.yml` — switch mode, multi mode, timestamp abort (known failures) |

---

## Quick Coverage Gap Summary

The following disagg test categories are **not included** in the code coverage tasks (`coverage-report-python`, `coverage-report-other`):

- All `test_layered*.py` tests (103 files)
- All `test_disagg*.py` tests except `test_key_provider_disagg01.py` and `test_key_provider_disagg02.py`
- All format stress tests with `CONFIG.disagg`
- Model checker disagg tests
- Checkpoint ctest disagg tests (`-L check_disagg`)
- cppsuite disagg failover perf test
- Failure-expected disagg tests (switch mode, multi mode)

See [../test_coverage/coverage_analysis.md](../test_coverage/coverage_analysis.md) for the coverage baseline.

---

## Summary Counts

| Category | Task Count | Test File Count |
|---|---|---|
| Python hook tests | 14 tasks (5 buckets × leader + table variants, follower, key_provider, TSAN, extra-long) | ~all test_suite files minus hook_disagg.fail |
| test_layered*.py | (via hook or direct run) | 103 files |
| test_disagg*.py specific | (via hook or direct run) | 13 files |
| Format stress tests | 14 tasks (in evergreen.yml) + 14 tasks in evergreen_disagg.yml | CONFIG.disagg |
| Model checker | 2 tasks | model_test binary |
| Checkpoint/csuite | 1 task (disagg ctest label) | ctest `check_disagg` label |
| cppsuite perf | 2 tasks | test_disagg_failover_perf |
| Catch2 unit test | (included in catch2-unittests) | test_disagg_meta_config.cpp |
| Failure-expected (disagg project) | 16 tasks | CONFIG.disagg switch/multi modes |
