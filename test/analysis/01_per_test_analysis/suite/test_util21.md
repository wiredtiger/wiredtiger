# test_util21 — wt dump: obsolete history store data not removed on clean pages

**File:** `test/suite/test_util21.py`
**Storage mode:** General
**Components under test:** `wt dump` of `WiredTigerHS.wt`, history store cleanup, oldest timestamp interaction

## Test Cases

### `test_util21.test_dump_obsolete_data`
- **What it tests:** Inserts 4 keys at timestamps 2, 3, 5, and 7 (4 versions per key); checkpoints; dumps `file:WiredTigerHS.wt` to `before_oldest`; advances oldest_timestamp to 6 (making ts=2,3,5 obsolete); checkpoints again; dumps `WiredTigerHS.wt` to `after_oldest`; asserts both dumps are identical (obsolete data is NOT removed from history store on clean pages).
- **Components:** `util_dump.c`, `history_store.c`, `txn_timestamp.c`
- **Notes:** No parameterization. Verifies that advancing oldest_timestamp does not trigger eviction of clean pages, so obsolete history store entries survive a checkpoint and are still dumpable. Tests the specific scenario where wt dump must handle obsolete data gracefully.
