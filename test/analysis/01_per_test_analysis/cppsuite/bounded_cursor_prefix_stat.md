# bounded_cursor_prefix_stat — Validates that bounded search_near limits B-tree traversal via statistics

**File:** `test/cppsuite/tests/bounded_cursor_prefix_stat.cpp`
**Storage mode:** General
**Components under test:** Bounded cursor early-exit path (`WT_STAT_CONN_CURSOR_BOUNDS_NEXT_EARLY_EXIT`), cursor traversal statistics (`WT_STAT_CONN_CURSOR_NEXT_SKIP_LT_100`), MVCC visibility with timestamps

## Overview

This test verifies that when `search_near` is called with prefix bounds on a dataset where no keys are visible at the read timestamp, the engine's early-exit optimisation fires and limits the number of entries traversed. The dataset is structured with 3-character alphabetical prefix keys (aaa through zzz); all keys are committed at timestamp 100 but searches read at timestamp 10, making every key invisible. The test spawns multiple parallel `search_near` threads per read iteration and then asserts that the traversal statistics increment by no more than `expected_entries` per thread, and that the bounds early-exit counter increases by exactly the number of threads (minus edge-case "z-key" searches where early exit is not possible).

## Configuration

**Config files:**
- `test/cppsuite/configs/bounded_cursor_prefix_stat_default.txt` — 60-second run, 3 collections, 1 key per prefix
- `test/cppsuite/configs/bounded_cursor_prefix_stat_stress.txt` — 30-minute run, 3 collections, 100 keys per prefix

### Default config key parameters

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 60 | |
| `cache_size_mb` | 1000 | |
| `search_near_threads` | 10 | Number of parallel search_near threads per iteration |
| `timestamp_manager.enabled` | false | Timestamps managed manually by the test |
| `collection_count` | 3 | |
| `key_count_per_collection` | 1 | 1 key per (26×26) prefix combination |
| `key_size` | 5 | 3-char prefix + 2-char random suffix |
| `read_config.thread_count` | 1 | Exactly one read thread required |

### Stress config key differences

| Parameter | Value |
|---|---|
| `duration_seconds` | 1800 |
| `key_count_per_collection` | 100 |
| `value_size` | 30 |
| `read_config.op_rate` | 10ms |

## Test Scenarios

### Scenario: Populate — structured prefix dataset with forced eviction
- **What it tests:** 26 populate threads (one per first-character letter) insert keys with 3-character prefixes (all combinations of `aaa`–`zzz`) plus a random suffix, committing at timestamp 100. After insertion, all pages are force-evicted using a `debug=(release_evict=true)` cursor so that subsequent bounded `search_near` calls must traverse on-disk pages, making the early-exit path observable in statistics.
- **Components:** B-tree insert, force eviction, timestamp management.
- **Notes:** The `srchkey_len` (1, 2, or 3) is randomly chosen once during populate and reused for the entire read phase, controlling how specific the prefix search is.

### Scenario: Read operation — parallel bounded search_near with stat validation
- **What it tests:** In each iteration, spawns `search_near_threads` worker threads, each picking a random collection and calling bounded `search_near` at timestamp 10 (where all keys are invisible). Validates after each batch that:
  1. The number of entries skipped (`CURSOR_NEXT_SKIP_LT_100`) does not exceed `expected_entries * num_threads + small_buffer`.
  2. The bounds early-exit counter (`CURSOR_BOUNDS_NEXT_EARLY_EXIT`) increments by exactly `num_threads - z_key_searches`.
- **Components:** Bounded cursor early-exit path, statistics counters, thread management.
- **Notes:** "Z-key searches" are tracked separately because a search prefix of `"z"`, `"zz"`, or `"zzz"` matches the tail of the entire dataset, preventing early exit. The expected traversal count is `keys_per_prefix * 26^(3 - srchkey_len) * 2` (forward and backward passes).

### Scenario / Phase: Edge case — z-key search (no early exit)
- **What it tests:** When the search prefix is at the lexicographic maximum of the prefix space (`z*`), the early-exit optimisation cannot fire because every remaining key in the tree also starts with `z`. The test accounts for this by tracking `z_key_searches` and subtracting them from the expected early-exit count.
- **Components:** Bounded cursor bounds checking, B-tree traversal at dataset tail.
- **Notes:** This is a documented limitation of the prefix early-exit approach, not a bug.

## Key Observations

- The entire correctness property rests on statistics rather than returned data: the test checks that the internal traversal count is bounded, not just that the API returns the right answer.
- Force-evicting all pages after populate is essential; without eviction the search could short-circuit through the in-memory page cache without exercising the on-disk traversal path.
- Reading at timestamp 10 (below the commit timestamp of 100) ensures all keys are invisible, so `WT_NOTFOUND` is always expected. This isolates the early-exit path from the normal found-key path.
- The `timestamp_manager` is disabled because the test manages timestamps directly (populate commits at ts=100, reads use ts=10).
- Only one read thread is permitted; the read thread itself manages the lifecycle of the `search_near_threads` worker pool.
