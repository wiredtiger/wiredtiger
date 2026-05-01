# test_bug032 — WT-11845: fast truncate incorrectly uses aggregated timestamp visibility

**File:** `test/suite/test_bug032.py`
**Storage mode:** General
**Components under test:** fast truncate, aggregated timestamp, transaction visibility

## Test Cases

### `test_bug032.test_bug032`
- **What it tests:** Reproduces WT-11845 where fast truncate incorrectly decided a page was fully visible by checking its aggregated max transaction ID (from the page header) rather than the actual per-key visibility. Scenario: populate 500 rows; remove key 32; start txn1 (inserts key 32, not committed yet); start and commit txn2 (inserts key 33, larger txn ID); start truncate transaction (snapshot sees txn2 but not txn1); commit txn1; force-evict the page (DS now contains updates from both txn1 and txn2, aggregated txn ID = txn2's ID); issue full truncate. Without the fix, the truncate sees the page's aggregated txn ID as txn2 (visible) and fast-truncates the whole page, incorrectly removing txn1's key. With the fix, the per-key check correctly identifies txn1 as not visible. Validates that key 32 is still present after the truncate.
- **Components:** `src/btree/bt_delete.c`, `src/reconcile/rec_write.c`
- **Notes:** Parametrized across `column` (`key_format=r`) and `string_row` (`key_format=S`). 512-byte values, `leaf_page_max=10KB`. Uses `debug=(release_evict)` cursor.
