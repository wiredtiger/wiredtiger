# burst_inserts — Stress test for bursty high-rate insert workloads simulating MongoDB bulk load

**File:** `test/cppsuite/tests/burst_inserts.cpp`
**Storage mode:** General
**Components under test:** B-tree insert path, eviction, cache management, history store, checkpoint, timestamp manager

## Overview

This test simulates a workload where a large volume of data is inserted in rapid bursts, mimicking a MongoDB instance under heavy bulk-load conditions. During each burst, insert threads write as fast as possible for `burst_duration` seconds with no throttling, then sleep for the configured `op_rate`. A parallel random-read cursor runs alongside each insert thread to generate cache pressure (simulating concurrent reads during bulk load). The test is designed to reproduce conditions from WT-7798 (a cache-stuck issue under bursty inserts).

## Configuration

**Config files:**
- `test/cppsuite/configs/burst_inserts_default.txt` — 5-second smoke run, default parameters
- `test/cppsuite/configs/burst_inserts_stress.txt` — 4-hour stress run, 1,000 collections, 100 threads

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 5 | Smoke test only |
| `cache_size_mb` | 250 | |
| (all other params) | framework defaults | No explicit workload config |

### Stress config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 14400 (4 hours) | |
| `burst_duration` | 90 | Seconds of unthrottled inserts per burst |
| `cache_size_mb` | 2048 | |
| `compression_enabled` | true | Prevents disk exhaustion |
| `timestamp_manager.oldest_lag` | 30 | |
| `timestamp_manager.stable_lag` | 30 | |
| `collection_count` | 1,000 | |
| `key_count_per_collection` | 1 | Seed only; inserts grow the dataset |
| `key_size` | 50 | |
| `value_size` | 1,000,000 | 1 MB values; stress eviction |
| `insert_config.thread_count` | 100 | |
| `insert_config.op_rate` | 10s | Sleep between bursts |
| `insert_config.ops_per_transaction` | max=30 | |
| `checkpoint_config.op_rate` | 120s | Infrequent checkpoints |
| `operation_tracker.enabled` | false | Disabled to avoid tracker table growing unbounded |

## Test Scenarios

### Scenario: Burst insert — unthrottled write phase
- **What it tests:** Each insert thread continuously inserts unique sequential keys into its assigned collection for `burst_duration` seconds without any `sleep()` call. Transactions are committed when `can_commit()` returns true (based on `ops_per_transaction`).
- **Components:** B-tree insert, transaction management, cache eviction pressure.
- **Notes:** The absence of throttling during the burst phase maximises write pressure on the cache and eviction subsystem. If a transaction fails (rollback), the key counter is reset and the burst continues.

### Scenario: Concurrent random read — cache pressure generation
- **What it tests:** Each insert thread simultaneously advances a `next_random=true` cursor on the same collection to generate cache pressure, forcing the eviction subsystem to manage both dirty (insert) and clean (read) pages concurrently.
- **Components:** Random cursor traversal, eviction, page pinning.
- **Notes:** `WT_NOTFOUND` from the read cursor causes a reset rather than an error. `WT_ROLLBACK` causes the current insert transaction to be rolled back as well.

### Scenario: Sleep between bursts
- **What it tests:** After each burst, the thread sleeps for `op_rate` (10 seconds in the stress config). During sleep, eviction and checkpointing can catch up. The next burst then begins from the updated key count.
- **Components:** Eviction, checkpoint, history store aging.
- **Notes:** This on/off pattern exercises the transition between high-dirty-cache and low-dirty-cache states.

## Key Observations

- The test was originally created to reproduce WT-7798 (cache stuck under bursty inserts with very large values).
- Compression is required in the stress config because 100 threads inserting 1 MB values would otherwise fill the host disk within minutes.
- The operation tracker is disabled to prevent the tracking table from growing unboundedly (since the test only inserts keys and never removes them).
- The default config is a minimal smoke test; the real coverage comes from the stress config which runs for 4 hours with extreme write pressure.
- No read, update, or remove threads beyond the embedded random-read cursor are configured, making this a write-dominant test.
- Infrequent checkpoints (every 2 minutes) are intentional to maximize dirty cache pressure.
