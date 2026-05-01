# test_prefetch02 — Prefetch statistics correctness during cursor traversal

**File:** `test/suite/test_prefetch02.py`
**Storage mode:** General
**Components under test:** prefetch background I/O, cursor traversal, statistics

## Test Cases

### `test_prefetch02.test_prefetch_scenarios`
- **What it tests:** Verifies that prefetch statistics (`prefetch_pages_queued`, `prefetch_attempts`, `prefetch_pages_read`) increase during forward and backward cursor traversal when prefetch is enabled; verifies that `prefetch_skipped` increments when prefetch is configured as unavailable
- **Components:** `conn/conn_prefetch.c`, `cursor/cur_std.c`, `btree/bt_read.c`
- **Notes:** Scenarios: column-store-variable/integer-row × 4 config combinations (available×default) × traversal direction (forward/backward) or verify mode; inserts 1,000 entries across multiple checkpointed pages; checks stats before and after full cursor scan; unavailable+enabled scenario checks skipped stat rather than queued/read stats
