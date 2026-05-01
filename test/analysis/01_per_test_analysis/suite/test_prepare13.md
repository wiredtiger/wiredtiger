# test_prepare13 — Fast-truncate returns prepare conflict for pages with prepared updates

**File:** `test/suite/test_prepare13.py`
**Storage mode:** General
**Components under test:** prepared transactions, fast-truncate, prepare conflict

## Test Cases

### `test_prepare13.test_prepare`
- **What it tests:** Creates a large table with small pages to maximize the fast-truncate code path; places a prepared (not-yet-committed) update on an evicted page; verifies that `session.truncate()` returns a prepare conflict error rather than incorrectly fast-truncating a page that contains a prepared update
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `cursor/cur_std.c`
- **Notes:** No scenarios; uses `allocation_size=512B,leaf_page_max=512B` to create many small pages; the prepared update is evicted to disk via a debug cursor; fast-truncate skips the page-by-page scan and instead marks entire pages as deleted, which would be incorrect if a page has an in-flight prepare; test verifies the conflict is detected before any truncation occurs
