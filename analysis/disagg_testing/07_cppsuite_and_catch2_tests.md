# Disagg CI Testing — cppsuite and Catch2 Tests

> Category: C++ disagg tests (performance and unit level)

---

## cppsuite: test_disagg_failover_perf

### Location
`test/cppsuite/tests/test_disagg_failover_perf.cpp`

### What It Tests
Measures how long a **disagg failover** (leader transition) takes on a system that already has ingested data. Captures the `step_up_time` statistic from WiredTiger and reports it as a performance metric.

**Test flow:**
1. Populate a set of collections with a configurable amount of data (as leader)
2. Optionally warm the cache to a specified percentage of the initial dataset
3. Optionally create a backup (`-C`) or reuse existing data (`-L`)
4. Restart as follower, pick up the latest checkpoint
5. Run a workload (append operations or update operations) for a period
6. Transition back to leader (step-up) and measure the time

### Command-Line Arguments

| Flag | Description | Default |
|---|---|---|
| `-c N` | Number of collections | 3 |
| `-g N` | Cache size in GB | 16 |
| `-h PATH` | Home directory | `WT_TEST` |
| `-i N` | Ingest size in MB | 1 |
| `-k N` | Key count per collection | 5000 |
| `-s N` | Key size in bytes | 10 |
| `-v N` | Value size in bytes | 1000 |
| `-V N` | Verbose level | 0 |
| `-w N` | Warm cache percentage (0-100%) | 0 |
| `-C` | Create a backup after populate | — |
| `-L` | Load skip: reuse existing database | — |
| `-S append\|updates` | Workload shape | `updates` |

### CI Tasks

| Task Name | Tag | Args | Description |
|---|---|---|---|
| `cppsuite-disagg-failover-perf-append` | `cppsuite-perf-test-arm` | `-c 10 -k 1000000 -i 3200 -S append -g 32 -s 10 -v 1014 -V 1` | 3.2GB across 10 1GB collections, append workload, 32GB cache |
| `cppsuite-disagg-failover-perf-updates` | `cppsuite-perf-test-arm` | `-c 10 -k 1000000 -i 3200 -S updates -g 32 -s 10 -v 1014 -V 1` | Same data, update workload |

Both tasks:
- Depend on `compile`
- Use `fetch artifacts` (not `get project` — reuse compiled binary)
- Submit results to the performance tracking system via `perf_submission.sh`

**Build variant:** Tagged `cppsuite-perf-test-arm`, which maps to ARM64 performance test build variants in `evergreen_develop.yml`. These are **develop-branch-only** tasks.

---

## Catch2 Unit Test: test_disagg_meta_config

### Location
`test/catch2/misc_tests/test_disagg_meta_config.cpp`

### What It Tests
Parsing and validation of **disaggregated checkpoint metadata** — the structured metadata that records which checkpoint was last applied, timestamps, key provider info, and version numbers.

### Test Cases

| Section | What Is Verified |
|---|---|
| **Parse metadata** | Parse checkpoint ID, stable timestamp, checkpoint timestamp, key_provider fields; handles missing optional fields (key_provider), missing required fields (EINVAL), null/empty metadata, length-limited buffers, version fields |
| **Parse crypt key metadata** | Extract page_id and LSN from encrypted key provider metadata; detects malformed data |
| **Legacy metadata format** | Backward compatibility with old newline-separated `checkpoint\ntimestamp` format |
| **Parse metadata with version** | Version field checking: valid version, incompatible version (ENOTSUP), missing version, default values |

### How It Runs in CI

`test_disagg_meta_config.cpp` is compiled into `test/catch2/catch2-unittests` along with all other Catch2 tests. It runs whenever `catch2-unittests` is run:
- In `coverage-report-catch2` (code coverage)
- In `coverage-report-other` (as part of the full test list in `code_coverage_config.json`)
- In any task that runs `ctest` covering the catch2 suite

**This is the only disagg test included in code coverage measurement** (as part of the catch2 binary). However, it only tests metadata parsing logic — not the full disagg storage path.

---

## Coverage Gap

- `test_disagg_failover_perf` — not in code coverage; develop-branch only, ARM64 perf variants only
- `test_disagg_meta_config` — **is** in coverage (via catch2-unittests), but only tests metadata parsing
