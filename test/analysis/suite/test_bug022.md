# test_bug022 — cursor.modify on a tombstone returns WT_NOTFOUND

**File:** `test/suite/test_bug022.py`
**Storage mode:** General
**Components under test:** cursor modify, tombstone visibility, MVCC update chain

## Test Cases

### `test_bug022.test_apply_modifies_on_onpage_tombstone`
- **What it tests:** Verifies that `cursor.modify()` on a key whose most recent visible update is an on-page tombstone returns `WT_NOTFOUND` rather than applying the modify on top of a deleted record. Inserts 9999 records at timestamp 2, deletes all of them at timestamp 3, calls `session.checkpoint()` to push tombstones to disk, then tries to apply a `Modify('B', 0, 100)` at each key. Asserts each modify returns `WT_NOTFOUND`. Also confirms that a subsequent `cursor.search()` on every key returns `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_curwalk.c`
- **Notes:** Parametrized across `string-row` (`key_format=S`) and `column` (`key_format=r`). 50 MB cache. Sets `oldest_timestamp=1`.
