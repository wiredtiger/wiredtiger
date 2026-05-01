# test_cursor13 — Cursor caching and sweep (cache_cursors, reopen, drop, sweep stats)

**File:** `test/suite/test_cursor13.py`
**Storage mode:** General
**Components under test:** cursor caching, cursor sweep, cursor reopen stats, cache_cursors config

## Test Cases

### `test_cursor13_01` / `test_cursor13_02` / `test_cursor13_03` (inherit from cursor01/02/03 with caching)
- **What it tests:** Runs the standard cursor iteration/insert/remove tests from test_cursor01-03 with `cache_cursors=true` enabled at the connection level, verifying that caching does not break correctness.
- **Components:** `src/cursor/cur_std.c`, `src/session/session_api.c`

### `test_cursor13_ckpt01` through `test_cursor13_ckpt06` / `test_cursor13_ckpt2` (inherit checkpoint tests)
- **What it tests:** Runs checkpoint-based cursor tests with cursor caching enabled.
- **Components:** `src/checkpoint/`, `src/cursor/cur_std.c`
- **Notes:** Skipped for tiered and disagg hooks.

### `test_cursor13_reopens.test_cursor13_reopens`
- **What it tests:** Tests that `cache_cursors` can be enabled/disabled at both connection and session level via reconfigure. Verifies that cursor.close() with caching enabled stores the cursor in a pool, and cursor open on same URI retrieves it (incrementing `cursor_reopen` stat). Tests reconfigure disabling caching drains the pool.
- **Components:** `src/cursor/cur_std.c`, `src/session/session_api.c`
- **Notes:** Skipped for tiered hook. Checks `cursor_cache` and `cursor_reopen` statistics.

### `test_cursor13_drops.test_cursor13_drops`
- **What it tests:** Verifies that cached cursors do not prevent table drops. After caching cursors on a URI, `session.drop(uri)` must succeed. Also tests index cursor caching and drop.
- **Components:** `src/cursor/cur_std.c`, `src/schema/schema_drop.c`
- **Notes:** Skipped for disagg hook.

### `test_cursor13_big.test_cursor13_big`
- **What it tests:** 500,000 random cursor open/close operations across 100 URIs with 3 nested cached cursor levels. Verifies all cursor reopens come from the cache (zero cache misses).
- **Components:** `src/cursor/cur_std.c`, `src/session/`

### `test_cursor13_sweep.test_cursor13_sweep`
- **What it tests:** Long-running test: 5 rounds of opening cursors on half the URIs, sleeping to trigger cursor sweep, then verifying the sweep stat increments. Verifies idle cached cursors are reclaimed.
- **Components:** `src/cursor/cur_std.c`, `src/session/`, `src/conn/conn_sweep.c`
- **Notes:** Long test only.

### `cursor13_dup.test_cursor13_dup`
- **What it tests:** Caches 100 duplicate cursors (opened from a positioned source cursor) and verifies they are reopened from cache on subsequent open calls.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Skipped for tiered and disagg hooks.
