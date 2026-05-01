# bounded_cursor_stress — Full stress test for the bounded cursor API under concurrent mixed workloads

**File:** `test/cppsuite/tests/bounded_cursor_stress.cpp`
**Storage mode:** General
**Components under test:** Bounded cursor API (`cursor->bound()`, `cursor->search_near()`, `cursor->search()`, `cursor->next()`, `cursor->prev()`), standard and reverse collator, MVCC, concurrent insert/update/remove/read/traverse

## Overview

This test exercises the correctness of the bounded cursor API under a high-concurrency mixed workload. Multiple thread types run simultaneously: inserters add new keys, updaters modify existing ones, removers delete keys, read threads perform bounded `search_near` and `search` calls, and custom threads traverse entire bounded ranges with `next()` and `prev()`. Every result from a bounded cursor call is validated against the expected behaviour derived from an unbounded cursor operating at the same read timestamp. The test also supports a reverse collator variant, where all bound comparisons are flipped.

## Configuration

**Config files (forward collator):**
- `test/cppsuite/configs/bounded_cursor_stress_default.txt` — 60-second run
- `test/cppsuite/configs/bounded_cursor_stress_stress.txt` — 1-hour run

**Config files (reverse collator):**
- `test/cppsuite/configs/bounded_cursor_stress_reverse_default.txt` — 60-second run with `reverse_collator=true`
- `test/cppsuite/configs/bounded_cursor_stress_reverse_stress.txt` — 1-hour run with `reverse_collator=true`

### Default config key parameters (forward collator)

| Parameter | Value |
|---|---|
| `duration_seconds` | 60 |
| `cache_size_mb` | 100 |
| `reverse_collator` | false |
| `timestamp_manager.stable_lag` | 2 |
| `timestamp_manager.oldest_lag` | 2 |
| `collection_count` | 10 |
| `key_count_per_collection` | 100 |
| `insert_config.thread_count` | 10 |
| `read_config.thread_count` | 10 |
| `remove_config.thread_count` | 5 |
| `update_config.thread_count` | 10 |
| `custom_config.thread_count` | 10 |
| `operation_tracker.enabled` | false |

### Stress config key differences (forward)

| Parameter | Value |
|---|---|
| `duration_seconds` | 3600 |
| `cache_size_mb` | 900 |
| `timestamp_manager.stable_lag` | 25 |
| `collection_count` | 100 |
| `key_count_per_collection` | 10,000 |
| `value_size` (populate/insert/update) | 100,000–1,000,000 |
| `insert_config.thread_count` | 15 |
| `read_config.thread_count` | 30 |

### Reverse collator configs

Mirror the forward collator configs with `reverse_collator=true`; the stress reverse config uses slightly higher `cache_size_mb` (1500 vs 900) and lower insert/update `op_rate`.

## Test Scenarios

### Scenario: Insert operation — random key/value insertion
- **What it tests:** Concurrent insertion of randomly generated string keys into random collections.
- **Components:** B-tree insert, transaction management.
- **Notes:** Standard framework insert pattern with rollback retry limit of 100.

### Scenario: Update operation — random key selection and value replacement
- **What it tests:** Uses a `next_random=true` cursor to select an existing key, then updates it with a new random value.
- **Components:** B-tree update, random cursor, MVCC.
- **Notes:** If no record is found (empty collection), the current transaction is committed and restarted.

### Scenario: Read operation — bounded search_near and search validation
- **What it tests:** For each iteration, randomly sets 0, 1, 2, or all bounds on a cached bounded cursor, then calls `search_near` with a randomly generated search key. The result is validated against an unbounded cursor. If `search_near` succeeds, also validates a subsequent bounded `search` on the found key.
- **Components:** Bounded cursor `search_near`, bounded cursor `search`, unbounded cursor, bound_set helper.
- **Notes:** Bound action is randomly chosen from: NO_BOUNDS, LOWER_BOUND_SET, UPPER_BOUND_SET, ALL_BOUNDS_SET. When ALL_BOUNDS_SET, the lower key's first character is one step below the upper key's first character (to guarantee non-overlapping bounds). Uses timestamp `roundup_timestamps=(read=true)` to handle oldest-timestamp drift.

### Scenario / Phase: search_near validation — result inside bounded range
- **What it tests:** When the search key falls inside the bounded range and a key is found, verifies `exact` is consistent with the found key's position relative to the search key, and that `prev()` / `next()` on an unbounded cursor at the found key is consistent with `exact`.
- **Components:** B-tree cursor comparison, `exact` return value semantics.

### Scenario / Phase: search_near validation — result outside bounded range
- **What it tests:** When the search key is outside the bounded range and a key is returned, verifies the returned key is the first or last record in the bounded range.
- **Components:** Boundary edge detection.

### Scenario / Phase: search_near validation — WT_NOTFOUND
- **What it tests:** When bounded `search_near` returns `WT_NOTFOUND`, traverses the range on an unbounded cursor to confirm no visible keys exist within the bounds.
- **Components:** B-tree traversal, visibility check.

### Scenario: Custom operation — bounded next()/prev() traversal validation
- **What it tests:** Applies random bounds to a cached bounded cursor, then walks the entire bounded range forward with `next()` and backward with `prev()`, comparing each step against an unbounded cursor that is manually positioned at the bound edges.
- **Components:** Bounded cursor traversal, cursor range walking, bound edge positioning.
- **Notes:** `cursor_traversal` positions the unbounded cursor at the lower/upper bound via `search_near` and then walks the rest of the range in lock-step with the bounded cursor. Every key returned by the bounded cursor must match the unbounded cursor's key.

### Scenario: Reverse collator variant
- **What it tests:** All of the above scenarios with `reverse_collator=true`, which inverts the lexicographic ordering used for bounds comparison. The `custom_lexicographical_compare` helper wraps all comparisons to respect the collator direction.
- **Components:** Custom comparator integration with bounded cursor API.
- **Notes:** Bound generation is also adjusted: when both bounds are set, the lower key's first character is one step above the upper key's first character (order is flipped).

## Key Observations

- This is the most comprehensive bounded cursor correctness test; it combines all cursor operations with the full set of thread types.
- The four config variants (default/stress × forward/reverse) provide coverage of both standard and reverse-collated B-trees.
- The test does not use the operation tracker (disabled in all configs), so validation is purely online (checked immediately after each call).
- The `bound_action::NO_BOUNDS` case (clearing all bounds) is included to ensure the API correctly handles an unbounded call on a cursor that previously had bounds set.
- Stress configs use very large values (up to 1 MB) to create eviction pressure and increase the likelihood of MVCC conflicts and rollbacks.
