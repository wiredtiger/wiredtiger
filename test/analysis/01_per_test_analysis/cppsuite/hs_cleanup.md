# hs_cleanup — Exercises history store cleanup by aging out entire pages with high-timestamp updates

**File:** `test/cppsuite/tests/hs_cleanup.cpp`
**Storage mode:** General
**Components under test:** History store (HS) cleanup, MVCC, checkpoint cleanup (`cc_pages_removed`), metrics monitor (`cache_hs_insert`, `cc_pages_removed`, `stat_cache_size`, `stat_db_size`), update path, timestamp manager

## Overview

This test drives history store cleanup by continuously updating keys with incrementally advancing timestamps. Because each key is updated many times, old versions accumulate in the history store. As the stable and oldest timestamps advance, entire pages of old versions become globally visible (their stop time pair is visible to all readers), triggering the checkpoint cleanup subsystem to reclaim them. The test monitors several statistics to verify that HS entries are being inserted and that pages are actually being cleaned up.

## Configuration

**Config files:**
- `test/cppsuite/configs/hs_cleanup_default.txt` — 90-second run, 10 collections, 500 keys each
- `test/cppsuite/configs/hs_cleanup_stress.txt` — 1-hour run, 100 collections, 1,000 keys each, with reads

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 90 | |
| `cache_size_mb` | 200 | Deliberately constrained to create HS pressure |
| `enable_logging` | true | |
| `timestamp_manager.oldest_lag` | 5 | |
| `timestamp_manager.stable_lag` | 5 | |
| `timestamp_manager.op_rate` | 1s | |
| `collection_count` | 10 | |
| `key_count_per_collection` | 500 | |
| `key_size` | 100 | |
| `value_size` | 10,000 | Large values to grow HS quickly |
| `update_config.thread_count` | 10 | One per collection |
| `update_config.op_rate` | 10ms | |
| `update_config.ops_per_transaction` | max=20 | |
| `checkpoint_config.op_rate` | 20s | |
| `operation_tracker.enabled` | true | |
| `operation_tracker.op_rate` | 30s | |
| `metrics_monitor.cache_hs_insert` | min=10, max=100M, postrun, save | Verifies HS inserts occurred |
| `metrics_monitor.cc_pages_removed` | max=10M, postrun, save | |
| `metrics_monitor.stat_cache_size` | max=100, runtime | Cache usage percentage check |
| `metrics_monitor.stat_db_size` | max=10GB, runtime, save | |

### Stress config key differences

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 3600 | |
| `cache_size_mb` | 1536 | |
| `compression_enabled` | true | Needed for large value sizes |
| `collection_count` | 100 | |
| `key_count_per_collection` | 1,000 | |
| `key_size` | 50 | |
| `value_size` | 1,000,000 | 1 MB values |
| `update_config.thread_count` | 100 | |
| `update_config.op_rate` | 1ms | |
| `read_config.thread_count` | 20 | Added in stress config |
| `read_config.op_rate` | 5ms | |
| `timestamp_manager.oldest_lag` | 1 | Tighter lag to age data faster |
| `timestamp_manager.stable_lag` | 10 | |
| `operation_tracker.enabled` | false | Disabled at stress scale |

## Test Scenarios

### Scenario: Update operation — sequential key updates to age HS content
- **What it tests:** Each update thread is assigned one collection (`tc->id` maps to `coll.id`). It iterates through keys sequentially with `cursor->next()`, updating each key with a new random value at an advancing timestamp. As older versions accumulate in the history store and the stable/oldest timestamps advance, the checkpoint cleanup server should reclaim globally-visible old versions.
- **Components:** B-tree update path, history store, MVCC versioning, checkpoint cleanup.
- **Notes:** The test asserts `collection_count == thread_count` to guarantee one thread per collection. Sequential iteration (not random) ensures all keys are updated evenly, aging entire pages at once rather than spot-updating.

### Scenario: Metrics validation — HS inserts and page cleanup (default config)
- **What it tests:** The metrics monitor asserts post-run that `cache_hs_insert >= 10` (at least some data went to the HS) and that `cc_pages_removed` is within bounds (up to 10M). Runtime checks ensure the cache usage percentage stays below 100% and database size stays below 10 GB.
- **Components:** Metrics monitor, HS statistics, checkpoint cleanup statistics.
- **Notes:** The minimum `cache_hs_insert=10` check is the key correctness indicator: if no HS inserts occurred the test would not be exercising the intended code path.

### Scenario: Read operation (stress config only)
- **What it tests:** 20 concurrent read threads perform random reads at varying timestamps during the update workload, exercising MVCC visibility under concurrent HS cleanup.
- **Components:** Cursor read, MVCC snapshot management, history store read path.
- **Notes:** Not present in the default config; added in the stress config to increase contention.

## Key Observations

- The core mechanism is "update ranges of keys with increasing timestamps so old versions become globally visible and trigger cleanup." This is documented explicitly in the file header.
- The test uniquely exercises the path where checkpoint cleanup removes HS data at the page granularity (entire-page cleanup), not just individual record cleanup.
- The one-thread-per-collection constraint (`collection_count == thread_count`) is enforced at runtime; mismatched configs will assert.
- The stress config disables the operation tracker (too large at 100 collections × 1,000 keys × 1 MB values) and compresses data to avoid disk exhaustion.
- A known limitation comment (FIXME-WT-12931) in the stress config disables the `stat_cache_size` runtime check because cache management is not yet tight enough at that scale.
