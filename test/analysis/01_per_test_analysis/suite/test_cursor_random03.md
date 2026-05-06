# test_cursor_random03 — Regression: next_random must not produce identical key streams from two cursors (WT-12225)

**File:** `test/suite/test_cursor_random03.py`
**Storage mode:** General
**Components under test:** cursor next_random, random number generator seeding (`__wt_random`)

## Test Cases

### `test_cursor_random03.test_cursor_random_bug`
- **What it tests:** Regression test for WT-12225. Inserts exactly 2135 records (chosen so that the skip insert-list random estimate is a power of 2, acting as a bitmask). Opens two `next_random=true` cursors close together in time. Reads 100 keys from the first cursor, closes it, opens a second cursor, and verifies that the second cursor's key stream differs from the first cursor's at least once in the first 100 results. Tests that `__wt_random` is seeded differently for each cursor open (time-based seed), ensuring two concurrently opened cursors do not return identical streams.
- **Components:** `src/cursor/cur_std.c`, `src/support/random.c`
- **Notes:** Runs 5000 iterations to maximize the chance of catching a regression. Table URI only. `leaf_page_max=100MB` to avoid page splits (all data in insert list). The specific record count (2135) is required for the bug to manifest.
