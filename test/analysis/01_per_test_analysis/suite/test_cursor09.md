# test_cursor09 — Cursor key state after insert() (WT-2217)

**File:** `test/suite/test_cursor09.py`
**Storage mode:** General
**Components under test:** cursor insert, cursor position state, row-store, column-store

## Test Cases

### `test_cursor09.test_cursor09`
- **What it tests:** After `cursor.insert()` completes, the cursor has no key set. A subsequent `cursor.search()` without setting a new key must fail with "requires key be set". Regression test for WT-2217.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file-r, file-S, table-r, table-S, table-r-complex, table-S-complex. Verifies both that insert succeeds and that search on the same cursor without reset or set_key raises an error.
