# bounded_cursor_prefix_search_near — Validates correctness of prefix-bounded search_near under concurrent inserts

**File:** `test/cppsuite/tests/bounded_cursor_prefix_search_near.cpp`
**Storage mode:** General
**Components under test:** Bounded cursor API (`cursor->bound()`, `cursor->search_near()`), timestamp-based visibility, concurrent insert/read workload, MVCC

## Overview

This test verifies that `search_near` with prefix bounds set returns the correct key (or `WT_NOTFOUND`) compared to an unbounded `search_near` on the same collection at the same read timestamp. Insert threads continuously add random keys, while read threads perform bounded `search_near` calls using randomly generated prefixes and validate the result against the standard (unbounded) cursor behaviour. The validation logic covers all correctness cases: exact match, positioned-after, positioned-before, and not-found.

## Configuration

**Config files:**
- `test/cppsuite/configs/bounded_cursor_prefix_search_near_default.txt` — 60-second run, 10 collections, key_size=5
- `test/cppsuite/configs/bounded_cursor_prefix_search_near_stress.txt` — 30-minute run, 10 collections, smaller prefix key_size for reads

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 60 | |
| `cache_size_mb` | 500 | |
| `timestamp_manager.oldest_lag` | 10 | Wider read-timestamp range for visibility tests |
| `collection_count` | 10 | |
| `insert_config.key_size` | 5 | |
| `insert_config.op_rate` | 100ms | |
| `insert_config.thread_count` | 10 | |
| `read_config.key_size` | 5 | Used as max prefix length |
| `read_config.op_rate` | 250ms | |
| `read_config.thread_count` | 10 | |

### Stress config key differences

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 1800 | |
| `timestamp_manager.oldest_lag` | 50 | Larger lag for deeper visibility testing |
| `insert_config.op_rate` | 10ms | Higher insert rate |
| `read_config.key_size` | 3 | Shorter prefixes; more likely to match existing keys |
| `read_config.op_rate` | 10ms | |

## Test Scenarios

### Scenario: Populate — create empty collections
- **What it tests:** Creates the configured number of empty collections; no initial data is inserted. This forces insert threads to build the dataset from scratch during the test phase.
- **Components:** Collection creation.
- **Notes:** The populate override is minimal by design so that insert and read threads operate concurrently on a growing dataset from the start.

### Scenario: Insert operation — concurrent random key insertion
- **What it tests:** Each insert thread continuously inserts randomly generated string keys and values into its assigned collection. Transactions are committed when `can_commit()` returns true.
- **Components:** B-tree insert, transaction management, timestamp manager.
- **Notes:** Rollback retries are capped at 100 per transaction to detect stuck states. Threads are distributed across collections to reduce contention.

### Scenario: Read operation — bounded search_near correctness validation
- **What it tests:** Each read thread picks a random collection, generates a random prefix of random length (1 to `key_size` characters), applies it as a lower+upper bound set, calls `search_near` with the bounded cursor, then opens a second unbounded cursor and validates the result.
- **Components:** Bounded cursor `search_near`, unbounded cursor `search_near`, timestamp-based read transactions, `bound_set` helper.
- **Notes:** Reads use `roundup_timestamps=(read=true)` to handle oldest-timestamp advancement. The validation logic (`validate_prefix_search_near`) covers three sub-cases:

### Scenario / Phase: Validation sub-case A — both calls succeed (`exact_default >= 0`)
- **What it tests:** When both bounded and unbounded `search_near` return a key, verifies that: (1) the bounded result always has `exact >= 0` (never positions before the prefix), (2) the bounded key starts with the generated prefix, and (3) the unbounded cursor's subsequent `next()` lands on the same key as the bounded cursor when `exact_default < 0`.
- **Components:** Prefix containment check, cursor positioning consistency.
- **Notes:** Handles the case where a tombstone (invisible delete) causes the unbounded cursor to land before the prefix while the bounded cursor skips forward.

### Scenario / Phase: Validation sub-case B — bounded fails, unbounded succeeds
- **What it tests:** Verifies that when bounded `search_near` returns `WT_NOTFOUND`, no visible key with the searched prefix actually exists in the table. Uses `next()` / `prev()` on the unbounded cursor to confirm the gap.
- **Components:** B-tree traversal, lexicographic ordering.
- **Notes:** Confirms the bounded cursor's early-exit logic is not returning `WT_NOTFOUND` incorrectly.

## Key Observations

- The test's correctness guarantee is that a bounded `search_near` can never succeed when the unbounded equivalent would also fail, but the bounded version may return `WT_NOTFOUND` when the unbounded version succeeds (because all matching keys are outside the bounded range).
- Using `oldest_lag` deliberately creates a wide window of potentially invisible data, which exercises the MVCC interaction with bounded cursor early-exit logic.
- The stress config reduces read `key_size` to 3 (shorter than write `key_size` of 5), increasing the proportion of searches that find matching keys and thus exercising the success path more heavily.
- A single long-lived transaction per read thread (refreshed via `try_rollback` at the end) limits the visibility window; this is intentional.
