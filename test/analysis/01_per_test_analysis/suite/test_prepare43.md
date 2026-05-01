# test_prepare43 — Checkpoint cursor does not skip pages with prepared tombstones

**File:** `test/suite/test_prepare43.py`
**Storage mode:** General
**Components under test:** prepared transactions, tombstones, checkpoint cursor, page skip optimization

## Test Cases

### `test_prepare43.test_prepare43`
- **What it tests:** Creates 100 keys, commits 99 of them and prepares a tombstone (delete) on the 100th; opens a checkpoint cursor and verifies that it sees all 99 committed keys without incorrectly skipping pages that contain the prepared tombstone
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `cursor/cur_ckpt.c`, `btree/bt_walk.c`
- **Notes:** Scenarios: column/integer-row × fuzzy/precise checkpoint; the bug was that the cursor walk optimization that skips "clean" pages could incorrectly skip a page containing a prepared tombstone (because the page appeared clean from the prepared-update perspective), causing the checkpoint cursor to miss 99 committed keys on adjacent pages; verifies that all 99 committed keys are found by the checkpoint cursor
