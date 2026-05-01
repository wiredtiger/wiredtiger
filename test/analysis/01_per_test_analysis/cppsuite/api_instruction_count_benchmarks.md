# api_instruction_count_benchmarks — Measures CPU instruction counts for core cursor and session API calls

**File:** `test/cppsuite/tests/api_instruction_count_benchmarks.cpp`
**Storage mode:** General
**Components under test:** Cursor API (`search`, `reset`, `insert`, `update`, `modify`, `remove`), Session API (`begin_transaction`, `commit_transaction`, `rollback_transaction`, `timestamp_transaction_uint`, `open_cursor`)

## Overview

This test benchmarks the raw instruction-count cost of individual WiredTiger cursor and session API calls. It runs in in-memory mode with all background servers (sweep, log, checkpoint, background compact, capacity, eviction-driven work) deliberately suppressed to minimize measurement noise. A single custom-operation thread executes each API call once and records the CPU instruction count using an `instruction_counter` utility. Results are emitted to a perf output file for tracking over time.

## Configuration

**Config file:** `test/cppsuite/configs/api_instruction_count_benchmarks_default.txt`

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 1 | Benchmark is a single-pass; duration is a minimum |
| `cache_size_mb` | 2000 | Large enough to avoid eviction |
| `in_memory` | true | Disables I/O and most background servers |
| `sweep_interval` | 1000 | Prevents sweep server from firing during the run |
| `validate` | false | No post-run validation |
| `collection_count` | 1 | Single collection; required by the test |
| `key_count_per_collection` | 1 | One key; cursor is positioned before measurement |
| `checkpoint_config.thread_count` | 0 | Checkpoint disabled |
| `operation_tracker.enabled` | false | Tracker disabled |

## Test Scenarios

### Scenario: Cursor search and reset (positioned read)
- **What it tests:** Instruction count of `cursor->search()` followed by `cursor->reset()` on an existing key.
- **Components:** B-tree cursor, page lookup path.
- **Notes:** The cursor is pre-positioned using `set_key` with the last key in the collection. No transaction wraps the search.

### Scenario: Transaction begin and rollback
- **What it tests:** Instruction count of `session->begin_transaction()` and `session->rollback_transaction()`.
- **Components:** Transaction subsystem.
- **Notes:** Measured independently. Rollback path does not include any written data (no modification before rollback).

### Scenario: Timestamp assignment (`timestamp_transaction_uint`)
- **What it tests:** Instruction count for setting a commit timestamp on an open transaction.
- **Components:** Timestamp manager, transaction time-point.
- **Notes:** Measured inside a transaction that is immediately rolled back.

### Scenario: Cursor update (in-place update, pre-positioned)
- **What it tests:** Instruction count of `cursor->update()` with cursor pre-positioned by a prior `search()`.
- **Components:** B-tree update path.
- **Notes:** Pre-positioning avoids conflating `search` cost. Wrapped in a transaction that is subsequently committed (to satisfy the commit-path benchmark).

### Scenario: Transaction commit
- **What it tests:** Instruction count of `session->commit_transaction()`.
- **Components:** Transaction commit path, MVCC.
- **Notes:** Requires at least one modification in the transaction; the update benchmark above provides it.

### Scenario: Cursor modify (delta modification, pre-positioned)
- **What it tests:** Instruction count of `cursor->modify()`.
- **Components:** B-tree modify path, WT_MODIFY struct.
- **Notes:** Pre-positioned with `search()`. Transaction is rolled back after measurement.

### Scenario: Cursor insert (overwrite=true, pre-positioned)
- **What it tests:** Instruction count of `cursor->insert()` with `overwrite=true` and cursor pre-positioned.
- **Components:** B-tree insert path.
- **Notes:** The `overwrite` flag prevents an internal search. This is equivalent to an upsert on a known key.

### Scenario: Cursor remove (pre-positioned)
- **What it tests:** Instruction count of `cursor->remove()` with cursor pre-positioned by a prior `search()`.
- **Components:** B-tree remove path.
- **Notes:** Pre-positioning avoids a redundant internal search.

### Scenario: Open cursor (uncached vs. cached)
- **What it tests:** Instruction count of `session->open_cursor()` both when the cursor cache is disabled (cold open) and when a previously closed cursor is returned from the cache (warm open).
- **Components:** Cursor cache, session cursor lifecycle.
- **Notes:** The two variants (`open_cursor_uncached` and `open_cursor_cached`) are measured separately to quantify the benefit of cursor caching.

## Key Observations

- All background servers are suppressed to make measurements deterministic; this test is not a correctness test.
- Measurements are single-sample (not averaged over many iterations), which may introduce noise; the `instruction_counter` utility wraps `perf_event_open` or equivalent.
- The test asserts that `in_memory=true` is set, ensuring it cannot accidentally run against disk storage.
- No explicit stress configurations exist; this test always runs with the default config.
- The pre-positioning pattern (search before update/modify/remove) is intentional and documented inline to prevent future regressions where a benchmark inadvertently measures two operations.
