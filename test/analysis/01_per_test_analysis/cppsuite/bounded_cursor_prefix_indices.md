# bounded_cursor_prefix_indices — Verifies unique-index insertion semantics using bounded search_near

**File:** `test/cppsuite/tests/bounded_cursor_prefix_indices.cpp`
**Storage mode:** General
**Components under test:** Bounded cursor API (`cursor->bound()`, `cursor->search_near()`), unique index insertion pattern, B-tree visibility, operation tracker, timestamp manager

## Overview

This test validates the WiredTiger bounded `search_near` API in the context of a unique-index-insertion pattern. The pattern mimics how a database engine might enforce uniqueness: insert a prefix, remove the prefix, then use a bounded `search_near` on the prefix to check whether any matching record already exists. If the search finds a result, the key is considered taken and the full insertion is aborted. Insert threads continuously attempt to insert using existing prefixes (expecting the insertion to fail), while read threads verify that the collection sizes do not change.

## Configuration

**Config files:**
- `test/cppsuite/configs/bounded_cursor_prefix_indices_default.txt` — 60-second run, 10 collections, 100 keys each
- `test/cppsuite/configs/bounded_cursor_prefix_indices_stress.txt` — 30-minute run, 10 collections, 2,000 keys each (larger key/value sizes)

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 60 | |
| `cache_size_mb` | 500 | |
| `timestamp_manager.stable_lag` | 30 | |
| `collection_count` | 10 | |
| `key_count_per_collection` | 100 | |
| `key_size` | 5 | |
| `insert_config.thread_count` | 10 | |
| `insert_config.op_rate` | 100ms | |
| `read_config.thread_count` | 1 | |
| `read_config.op_rate` | 250ms | |

### Stress config key differences

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 1800 | |
| `key_count_per_collection` | 2,000 | |
| `key_size` | 30 | Larger keys to force cross-page splits |
| `value_size` | 40 | |
| `timestamp_manager.stable_lag` | 60 | |
| `insert_config.thread_count` | 15 | |
| `insert_config.op_rate` | 10ms | |

## Test Scenarios

### Scenario: Populate — unique index insertion
- **What it tests:** Creates collections and populates each one using the full unique-index protocol: insert prefix, remove prefix, bounded `search_near` on prefix, then insert the full composite key (prefix + thread_id). Spawns one populate thread per collection.
- **Components:** B-tree insert, remove, bounded `search_near`, transaction commit/rollback.
- **Notes:** After populate finishes, all inserted prefixes are stored in a 2D `prefixes_map` vector indexed by collection ID. This map is used by both insert and read threads during the test phase.

### Scenario: Insert operation — expected-failure unique index insertions
- **What it tests:** Each insert thread picks a random existing prefix from `prefixes_map` and runs the full unique-index insertion protocol on it. Because the prefix already exists (committed during populate), the bounded `search_near` should always find a matching record and the full insertion should be rejected. The test asserts the insertion fails every time.
- **Components:** Bounded `search_near`, B-tree insert, transaction rollback.
- **Notes:** This exercises the common "optimistic insert" failure path of the unique-index protocol under concurrent reads and timestamp visibility.

### Scenario: Read operation — collection size invariant
- **What it tests:** A single read thread iterates over all collections counting records and verifies the total matches `prefixes_map.size() * prefixes_map[0].size()`. Because all insert threads are expected to fail, the count should never change.
- **Components:** Cursor traversal (`next()`), collection count tracking.
- **Notes:** The test wraps the scan in a single long transaction (begun once, never committed) to get a stable snapshot.

## Key Observations

- The core correctness property is that bounded `search_near` correctly detects the presence of an existing prefix even when concurrent inserts/removes are happening in the same transaction.
- The test specifically checks that `exact_prefix == 1` when the search finds a key (meaning the cursor landed on a key lexicographically after the prefix, confirming prefix match).
- The stress config uses larger keys (`key_size=30`) to increase the likelihood of page splits and cross-page prefix searches.
- The `prefixes_map` is built after populate completes and is never modified during the test phase, making it safe to access from multiple threads without locking.
- There is a maximum rollback retry limit of 100 during populate to prevent infinite loops on conflict.
