# test_checkpoint27 — Metadata page eviction while reading checkpoint

**File:** `test/suite/test_checkpoint27.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, metadata, eviction, page lifecycle

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that metadata pages can be safely evicted while a checkpoint cursor is actively reading data, without corrupting the cursor's view of the checkpoint.
- **Components:** `src/meta/`, `src/evict/evict_page.c`, `src/cursor/cur_btree.c`, `src/checkpoint/`
- **Notes:** Opens a checkpoint cursor and uses `debug=(release_evict_page=true)` to force-evict metadata pages during the read. Confirms the cursor continues to return correct values after page eviction and re-read. Tests memory-safety of metadata page lifecycle under concurrent checkpoint cursor usage.
