# test_layered67 — Page write for update-restore eviction path with page deltas disabled

**File:** `test/suite/test_layered67.py`
**Storage mode:** Disagg/Layered
**Components under test:** Eviction update-restore path, `cache_write` stat, page delta configuration

## Test Cases

### `test_layered67.test_uncommit_eviction`
- **What it tests:** With page deltas disabled (`internal_page_delta=false, leaf_page_delta=false`), populates 10 rows (ts=50, stable=50) and begins an uncommitted update on key=1 (value2, no commit). Forces page eviction via `debug=(release_evict_page)`. Verifies that `cache_write` increments after eviction, confirming the page is still written to the block manager (via the update-restore path) even though page deltas are disabled.
- **Components:** `src/btree/bt_evict.c`, `src/btree/bt_rec.c`, disagg block manager
- **Notes:** Uses `file:` URI with `block_manager=disagg`. The test guards against a regression where the disagg eviction path might skip writing a page that needs update-restore. Disagg-only; no follower connection.
