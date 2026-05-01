# operations_test — Base framework stress test exercising all standard database operations

**File:** `test/cppsuite/tests/operations_test.cpp`
**Storage mode:** General
**Components under test:** Framework default implementations of insert, update, remove, read, checkpoint, validate, background compact, operation tracker, timestamp manager, metrics monitor

## Overview

This is the "base test" of the cppsuite framework. It does not override any database operation method, so it runs entirely with the framework's default implementations of populate, insert, update, remove, read, checkpoint, and validate. It is the canonical example of using the framework as-is and serves as a general stress test for the WiredTiger core engine. The configurations range from a 60-second smoke run to a 10-minute stress run and an insert-heavy variant.

## Configuration

**Config files:**
- `test/cppsuite/configs/operations_test_default.txt` — 60-second default run
- `test/cppsuite/configs/operations_test_stress.txt` — 10-minute stress run
- `test/cppsuite/configs/operations_insert_heavy.txt` — 5-minute insert-heavy run

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 60 | |
| `cache_size_mb` | 200 | |
| `enable_logging` | true | |
| `timestamp_manager.oldest_lag` | 2 | |
| `timestamp_manager.stable_lag` | 2 | |
| `collection_count` | 100 | |
| `key_count_per_collection` | 50 | |
| `key_size` | 10 | |
| `value_size` | 20 | |
| `insert_config.thread_count` | 5 | |
| `read_config.thread_count` | 10 | |
| `remove_config.thread_count` | 1 | |
| `update_config.thread_count` | 10 | |
| `background_compact_config.thread_count` | 1 | |
| `background_compact_config.free_space_target_mb` | 20 | |
| `checkpoint_config.op_rate` | 5s | |
| `operation_tracker.op_rate` | 5s | Enabled with default settings |
| `metrics_monitor.*` | Various limits | cache_hs_insert, cc_pages_removed, stat_cache_size, stat_db_size |

### Stress config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 600 | |
| `cache_size_mb` | 2000 | |
| `compression_enabled` | true | |
| `collection_count` | 200 | |
| `key_count_per_collection` | 10,000 | |
| `key_size` | 100 | |
| `value_size` (populate) | 100,000 | |
| `value_size` (insert/update) | 1,000,000 | 1 MB |
| `insert_config.thread_count` | 40 | |
| `read_config.thread_count` | 40 | |
| `remove_config.thread_count` | 20 | |
| `update_config.thread_count` | 40 | |
| `operation_tracker.op_rate` | 40s | |

### Insert-heavy config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 300 | |
| `cache_size_mb` | 2000 | |
| `collection_count` | 100 | |
| `key_count_per_collection` | 500 | |
| `value_size` | 2000 | |
| `insert_config.thread_count` | 50 | Insert-dominant |
| `read_config.thread_count` | 10 | |
| Remove and update | not configured | Insert-only workload |
| `operation_tracker.enabled` | false | Disabled (cannot handle all rollbacks) |

## Test Scenarios

### Scenario: Populate — initial dataset creation
- **What it tests:** Default framework populate: fills each collection with `key_count_per_collection` unique sequential keys using distributed threads.
- **Components:** B-tree insert, parallel populate threads.

### Scenario: Concurrent mixed workload (insert + read + update + remove)
- **What it tests:** Default framework implementations of all four operations run concurrently:
  - **Insert:** Adds unique keys to assigned collections in transactions.
  - **Read:** Selects random collections and reads keys using a random cursor.
  - **Update:** Selects random keys via a random cursor and updates them.
  - **Remove:** Selects random keys and removes them.
- **Components:** B-tree insert/read/update/remove, MVCC, transaction management.
- **Notes:** All operations use the framework's default transaction management (try_begin, commit/rollback patterns).

### Scenario: Checkpoint
- **What it tests:** Default framework checkpoint thread runs every `op_rate` seconds.
- **Components:** Checkpoint subsystem.

### Scenario: Background compact
- **What it tests:** Background compaction runs as a framework-managed thread (default config only).
- **Components:** Background compaction server.
- **Notes:** Present in the default config but not in the stress or insert-heavy configs.

### Scenario: Operation tracking and validation
- **What it tests:** The operation tracker records all insert, update, and remove operations; the default validator compares the tracker's view of the database against the actual WiredTiger tables.
- **Components:** Operation tracker, default validator.
- **Notes:** Disabled in the insert-heavy config (the comment says "verification can't handle rollbacks").

### Scenario: Metrics monitoring
- **What it tests:** The metrics monitor checks that `cache_hs_insert`, `cc_pages_removed`, `stat_cache_size`, and `stat_db_size` stay within configured bounds during and after the run.
- **Components:** Metrics monitor, statistics.

## Key Observations

- This test is the framework's "reference implementation" — it validates that the default framework operations work correctly together.
- The three config variants cover different stress dimensions: balanced load (default), large-scale with compression (stress), and insert-dominant (insert-heavy).
- The operation tracker with default validation provides the primary correctness guarantee: if any mutation is lost or corrupted, the final validation will detect it.
- The insert-heavy config disables the operation tracker because the default validator cannot correctly handle the case where an insert is rolled back after being tracked.
- Background compaction is only enabled in the default config; the stress configs focus on raw throughput rather than space reclamation.
