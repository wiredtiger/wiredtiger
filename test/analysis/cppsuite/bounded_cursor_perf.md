# bounded_cursor_perf — Benchmarks next()/prev() throughput of bounded vs. unbounded cursors

**File:** `test/cppsuite/tests/bounded_cursor_perf.cpp`
**Storage mode:** General
**Components under test:** Bounded cursor API (`cursor->bound()`), cursor traversal (`next()`, `prev()`), compiled configuration strings (`conn->compile_configuration()`)

## Overview

This test measures the wall-clock time of `next()` and `prev()` traversal on a single collection, comparing four cursor variants: bounded-next, bounded-prev, default-next, and default-prev. Bounds are set to encompass the full key range (so all records are returned by both cursor types), making it a pure throughput comparison rather than a correctness test. Additionally, it alternates between setting bounds using pre-compiled configuration strings and the standard string-based API to benchmark the overhead of compilation. Timing data is written to a perf output file.

## Configuration

**Config files:**
- `test/cppsuite/configs/bounded_cursor_perf_default.txt` — 10-second run, 1,000 keys, key_size=5
- `test/cppsuite/configs/bounded_cursor_perf_stress.txt` — 300-second run, 100,000 keys, key_size=100

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 10 | |
| `cache_size_mb` | 1000 | |
| `collection_count` | 1 | Required by the test |
| `key_count_per_collection` | 1,000 | |
| `key_size` | 5 | |
| `read_config.thread_count` | 1 | Single read thread required |
| `checkpoint_config.op_rate` | 20s | |
| `metrics_monitor.cache_hs_insert` | max=100M, postrun, save | |
| `metrics_monitor.cc_pages_removed` | max=10M, postrun, save | |

### Stress config key differences

| Parameter | Value |
|---|---|
| `duration_seconds` | 300 |
| `key_count_per_collection` | 100,000 |
| `key_size` | 100 |
| `read_config.op_rate` | 10ms |

## Test Scenarios

### Scenario: Full-collection traversal — bounded vs. default cursors
- **What it tests:** Relative throughput of `next()` and `prev()` when bounds are set to a range that encompasses all keys versus a cursor with no bounds. Both cursor types traverse the entire collection.
- **Components:** Bounded cursor B-tree traversal, default cursor B-tree traversal.
- **Notes:** Lower and upper bounds are set just outside the key space (`'0'-1` and `'9'+1`) so that bounded cursor traversal visits the same records as the unbounded cursor. An assertion verifies that both cursors reach `WT_NOTFOUND` at the same step.

### Scenario: Compiled vs. non-compiled bound configuration
- **What it tests:** The overhead of `cursor->bound()` when using a pre-compiled configuration string (via `conn->compile_configuration()`) compared to the standard runtime string.
- **Components:** `WT_CONNECTION::compile_configuration`, `WT_CURSOR::bound`.
- **Notes:** The test alternates between compiled and non-compiled bound setting on successive traversals. Timing for each variant is captured separately in `compiled_config_timer` and `regular_config_timer`.

## Key Observations

- The test requires exactly one read thread (`testutil_assert(tc->thread_count == 1)`) and exactly one collection.
- Because bounds encompass the full key range, this is not testing the correctness of bound filtering — it is purely a performance regression test for the overhead introduced by the bounded cursor machinery on traversal.
- The assertion that all four cursors reach `WT_NOTFOUND` simultaneously (`range_ret_next == ret_next == range_ret_prev == ret_prev`) is a lightweight correctness check embedded in the performance loop.
- Metrics for history store inserts and checkpoint cleanup pages are monitored as health indicators, not as pass/fail criteria for the benchmark itself.
- No insert, update, or remove threads run concurrently; the collection is read-only during measurement, giving stable and reproducible results.
