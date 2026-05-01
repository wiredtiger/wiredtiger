# reverse_split — Stress test for the B-tree reverse split path under concurrent insert and truncate

**File:** `test/cppsuite/tests/reverse_split.cpp`
**Storage mode:** General
**Components under test:** B-tree split path (specifically reverse splits), `session->truncate()`, concurrent insert, checkpoint, split timing stresses (`split_3`, `split_4`)

## Overview

This test is designed to exercise the reverse split code path in the WiredTiger B-tree. It achieves this by combining two concurrent operations: (1) insert threads continuously append new keys at the end of each collection, growing the tree's rightmost pages, and (2) remove threads continuously truncate from the beginning of the collection, emptying pages at the start of the tree. This pattern — adding at the tail while removing from the head — causes pages at the start to become empty and triggers the reverse split mechanism that reclaims them. Additionally, the test randomly injects split timing stresses (`timing_stress_for_test=[split_3]` or `[split_4]`) to increase the probability of hitting race conditions in the split code.

## Configuration

**Config files:**
- `test/cppsuite/configs/reverse_split_default.txt` — 30-second run, 5 collections, 100 seed keys
- `test/cppsuite/configs/reverse_split_stress.txt` — 2-hour run, 5 collections, 10,000 seed keys

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 30 | |
| `cache_size_mb` | 256 | |
| `timestamp_manager.oldest_lag` | 30 | |
| `timestamp_manager.stable_lag` | 30 | |
| `collection_count` | 5 | |
| `key_count_per_collection` | 100 | Seed dataset |
| `key_size` | 50 | |
| `value_size` | 100,000 | Large values to force multi-page splits |
| `insert_config.thread_count` | 5 | One per collection |
| `insert_config.op_rate` | 2ms | |
| `insert_config.ops_per_transaction` | max=30 | |
| `remove_config.thread_count` | 5 | One per collection |
| `remove_config.op_rate` | 5s | |
| `checkpoint_config.op_rate` | 10s | |
| `operation_tracker.enabled` | false | |

### Stress config key differences

| Parameter | Value |
|---|---|
| `duration_seconds` | 7200 (2 hours) |
| `compression_enabled` | true |
| `key_count_per_collection` | 10,000 |
| `insert_config.op_rate` | 1ms |
| `remove_config.op_rate` | 45s |
| `checkpoint_config.op_rate` | 60s |

## Test Scenarios

### Scenario: Insert operation — append-only growth at collection tail
- **What it tests:** Default framework insert implementation appends new sequential keys at the end of each collection in multi-operation transactions. This continuously grows the rightmost pages of the B-tree.
- **Components:** B-tree insert, transaction management, sequential key ordering.
- **Notes:** Uses the default framework insert operation (not overridden). The combination with truncation at the head creates a sliding-window workload.

### Scenario: Remove operation — burst truncation from collection head
- **What it tests:** Each remove thread (one per collection, enforced by assertion) reads the current first key via `cursor->next()`, then truncates from that key to a randomly chosen end key that covers up to 83% of the live range. After truncation, threads synchronise with `tc->sync()` to ensure all truncation threads fire at roughly the same time (bursty truncation workload).
- **Components:** `session->truncate()`, B-tree cursor, B-tree reverse split path.
- **Notes:** The 83% truncation cap (`key_count / 1.2`) ensures the collection never becomes fully empty. The `tc->sync()` call is key to creating simultaneous truncation bursts, which increases reverse split activity.

### Scenario: Split timing stress injection
- **What it tests:** At construction time, if no `wt_open_config` is provided, the test randomly chooses between `timing_stress_for_test=[split_3]` and `timing_stress_for_test=[split_4]`. These internal timing stresses slow down specific split code paths to expose race conditions.
- **Components:** B-tree split state machine (internal).
- **Notes:** This is the only cppsuite test that injects split timing stresses. The random choice ensures both stress variants are exercised over multiple runs.

## Key Observations

- The test was designed specifically to reproduce and prevent regressions in reverse split logic, which handles the case where a B-tree page becomes empty from the head of the tree.
- The combination of large values (`value_size=100,000`), frequent inserts, and periodic large truncations maximises page fragmentation and reverse split triggers.
- Thread synchronisation (`tc->sync()`) for truncation threads is unusual in cppsuite tests; it intentionally creates artificial contention at the split layer.
- The one-thread-per-collection constraint for remove threads is enforced at runtime (`testutil_assert(db.get_collection_count() == tc->thread_count)`).
- The operation tracker is disabled; there is no post-run data validation, making this a stability/crash test rather than a correctness test.
- The stress config enables compression to prevent disk exhaustion during the 2-hour run with 100 KB values and frequent inserts.
