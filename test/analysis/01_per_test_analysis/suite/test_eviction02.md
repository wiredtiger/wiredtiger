# test_eviction02 — Clean eviction removes obsolete time window information (heuristic limits)

**File:** `test/suite/test_eviction02.py`
**Storage mode:** General
**Components under test:** eviction, time window cleanup, heuristic controls, statistics

## Test Cases

### `test_eviction02.test_evict`
- **What it tests:** In 10 iterations, inserts 10,000 rows per iteration with per-key timestamps, makes inserted data stable and checkpoints, bumps the oldest timestamp to make earlier data globally visible (obsolete time windows), then triggers clean eviction via a debug cursor and checks the `cache_eviction_dirty_obsolete_tw` statistic. Verifies that:
  - When `expected_cleanup=False` (either `obsolete_tw_btree_max=0` or `eviction_obsolete_tw_pages_dirty_max=0`), no pages with obsolete TW are cleaned.
  - When `expected_cleanup=True`, the number of pages cleaned per iteration does not exceed `obsolete_tw_max * 1.5` (threshold), and by end of test both btree-level and connection-level stats are > 0.
- **Components:** `src/evict/`, `src/reconcile/`, `src/stat/`
- **Notes:** Scenarios (via `heuristic_controls`):
  - `no_btrees`: `obsolete_tw_btree_max=0` → no cleanup
  - `no_pages`: `eviction_obsolete_tw_pages_dirty_max=0` → no cleanup
  - `50_pages`: max 50 pages per checkpoint → cleanup expected
  - `100_pages`: max 100 pages per checkpoint → cleanup expected
  - `500_pages`: max 500 pages per checkpoint → cleanup expected
  
  Value is `'k' * 1024`. Uses `eviction_util.evict_cursor_tw_cleanup`. Stats logged via `statistics_log=(json,wait=1,on_close=true)`.

### Eviction trigger
- Debug cursor with `release_evict` performs clean eviction. Oldest timestamp advancement makes TW info obsolete and eligible for removal during clean eviction.
