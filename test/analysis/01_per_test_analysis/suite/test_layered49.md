# test_layered49 — User tombstones in the ingest table are not removed before checkpoint inclusion

**File:** `test/suite/test_layered49.py`
**Storage mode:** Disagg/Layered
**Components under test:** ingest btree, cur_layered.c, eviction, checkpoint pick-up, tombstone lifecycle

## Test Cases

### `test_layered49.test_remove`
- **What it tests:** Verifies that tombstones (deletes) written by a follower into the ingest table are not discarded by eviction before they are included in a stable checkpoint. Inserts data on the leader, picks up the checkpoint on the follower, then the follower deletes all records one by one. The deletes are made globally visible (oldest_timestamp advanced). Force-evicts all pages using a debug cursor. Verifies that the deletions are still correctly visible (all keys return `WT_NOTFOUND`) after eviction.
- **Components:** ingest btree (tombstone retention during eviction), cur_layered.c (remove path), eviction (`debug=(release_evict_page=true)`), checkpoint pick-up
- **Notes:** 100 items. Uses two sessions: one for deletes (with timestamps), one for force-evict. Disagg-only.

### `test_layered49.test_truncate`
- **What it tests:** Same as `test_remove` but uses `session.truncate()` instead of individual `cursor.remove()` calls to delete all records. Verifies that truncation-generated tombstones are also preserved correctly in the ingest table after eviction.
- **Components:** ingest btree (truncate tombstone retention), eviction, checkpoint pick-up
- **Notes:** Uses a positioned cursor (`cursor.next()`) as the start argument to `session.truncate(None, cursor, None, None)`. 100 items. Disagg-only.
