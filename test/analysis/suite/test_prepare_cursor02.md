# test_prepare_cursor02 — Cursor search/next/prev on empty dataset with prepared insert

**File:** `test/suite/test_prepare_cursor02.py`
**Storage mode:** General
**Components under test:** prepared transactions, cursor navigation, prepare conflict, empty table

## Test Cases

### `test_prepare_cursor02.test_cursor_navigate_prepare_transaction`
- **What it tests:** Creates an empty table; prepares an insert of key 1; a concurrent session attempts search, next, and prev at a timestamp that would see the prepared key; verifies that all three operations return a prepare conflict error (not WT_NOTFOUND), confirming that the cursor correctly detects the prepared key even in an otherwise empty dataset
- **Components:** `txn/txn_prepare.c`, `cursor/cur_std.c`, `btree/bt_cursor.c`
- **Notes:** Scenarios: integer-row/column; tests the edge case where the only entry in the table is a prepared insert — without this, a naive implementation might return WT_NOTFOUND (no keys visible) instead of the correct prepare conflict
