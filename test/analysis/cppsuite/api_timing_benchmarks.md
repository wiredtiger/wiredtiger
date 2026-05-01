# api_timing_benchmarks — Measures wall-clock execution time for core session API calls

**File:** `test/cppsuite/tests/api_timing_benchmarks.cpp`
**Storage mode:** General
**Components under test:** Session API (`begin_transaction`, `commit_transaction`, `rollback_transaction`, `timestamp_transaction_uint`), Cursor API (`reset`, `search`)

## Overview

This test benchmarks the wall-clock latency of frequently-called WiredTiger session API operations. Like its companion `api_instruction_count_benchmarks`, it runs in in-memory mode to suppress I/O and background threads. Instead of instruction counters it uses `execution_timer` objects to record elapsed time. Each measured code path is executed 1,000 times (or 100 times for commit) to reduce clock-resolution noise; average times are written to a perf output file.

## Configuration

**Config file:** `test/cppsuite/configs/api_timing_benchmarks_default.txt`

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 1 | Single-pass benchmark |
| `cache_size_mb` | 2000 | Avoids eviction pressure |
| `in_memory` | true | Disables I/O and background servers |
| `sweep_interval` | 1000 | Prevents sweep server from triggering |
| `validate` | false | No post-run validation |
| `collection_count` | 1 | Single collection required |
| `key_count_per_collection` | 1 | One seed key |
| `checkpoint_config.thread_count` | 0 | Checkpoint disabled |
| `operation_tracker.enabled` | false | Tracker disabled |

## Test Scenarios

### Scenario: begin_transaction + commit_transaction loop (100 iterations)
- **What it tests:** Latency of `begin_transaction` paired with `commit_transaction`. Each commit requires at least one modification, so a unique key is inserted per iteration.
- **Components:** Transaction begin/commit paths, MVCC, B-tree insert.
- **Notes:** Uses a loop counter of `_LOOP_COUNTER / 10` (100). If an insert fails the iteration is retried.

### Scenario: begin_transaction + rollback_transaction loop (1,000 iterations)
- **What it tests:** Latency of a no-modification transaction cycle: begin immediately followed by rollback.
- **Components:** Transaction begin/rollback paths.
- **Notes:** Uses the full 1,000-iteration loop. No data is written so this isolates the overhead of the transaction infrastructure itself.

### Scenario: timestamp_transaction_uint loop (1,000 iterations)
- **What it tests:** Latency of setting a commit timestamp on an already-open transaction.
- **Components:** Timestamp manager, transaction time-point.
- **Notes:** A single transaction is begun, and `timestamp_transaction_uint` is called 1,000 times with incrementing timestamps inside it. The transaction is rolled back at the end.

## Key Observations

- The test covers timing (wall-clock) rather than instruction counts; the two benchmark tests complement each other for identifying regressions in both CPU efficiency and scheduling/synchronization overhead.
- Cursor `reset` and `search` timers are declared but the test body does not appear to exercise them in the current implementation — they may be placeholders for future measurement.
- No stress configuration exists; only the default config is provided.
- The `operation_tracker` is disabled via `init_operation_tracker(nullptr)`, which is slightly different from the default initialisation path.
- Like `api_instruction_count_benchmarks`, this test asserts a single collection and cannot be meaningfully run with a standard (non-in-memory) configuration.
