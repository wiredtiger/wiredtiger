# test_layered14 — Layered table random cursor (next_random) on leader and follower

**File:** `test/suite/test_layered14.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** random cursor (`next_random`), layered cursor, stable btree, ingest btree, checkpoint, cur_layered.c

## Test Cases

### `test_layered14.test_layered_random_cursor`
- **What it tests:** Inserts 1000 records into a layered table (these go into the ingest btree), checkpoints, inserts another 1000 (now mix of stable and ingest). Opens a `next_random=true` cursor on the leader and calls `cursor.next()` — verifies it returns 0 (found a record). Then reopens the connection as follower (with checkpoint meta), opens the random cursor again, and verifies it still returns a record.
- **Components:** random cursor implementation for layered tables (`cur_layered.c`), stable btree (via checkpoint), ingest btree
- **Notes:** Parametrized by disagg_storage scenario. Tests that `next_random` works when data spans both stable and ingest btrees (leader), and also works on a follower that only has stable data (since follower picks up checkpoint, ingest data is not available). Would break if `next_random` is not implemented or returns `WT_NOTFOUND` when data exists.

### `test_layered14.test_empty_table`
- **What it tests:** Creates a layered table with no data. Opens a `next_random=true` cursor and asserts `cursor.next()` returns `WT_NOTFOUND`.
- **Components:** random cursor on empty table (`cur_layered.c`)
- **Notes:** Edge case: random cursor on a completely empty layered table must return `WT_NOTFOUND` rather than crashing or returning a spurious record.
