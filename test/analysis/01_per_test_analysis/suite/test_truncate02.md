# test_truncate02 — Fast delete with overflow pages and commit/rollback isolation

**File:** `test/suite/test_truncate02.py`
**Storage mode:** General
**Components under test:** fast delete (address-deleted cells), overflow pages, read/write before and after truncate

## Test Cases

### `test_truncate_fast_delete.test_truncate`
- **What it tests:** Inserts 10,000 entries with optional overflow key/value pages; performs a cursor-range truncate then verifies reads and writes (insert/delete) before and after the truncated range are correct. Commits or rolls back the truncate transaction and verifies visibility changes accordingly.
- **Components:** `btree.c`, `cursor.c`, `schema.c`, `txn.c`, `ovfl.c`
- **Notes:** Parameterized over string-row/column format × overflow key × overflow value × reads before truncate × writes before truncate × commit/rollback. Tests the fast delete code path (address-deleted pages not immediately freed) and confirms that overflow pages are correctly handled. Rollback restores all 10,000 entries; commit makes them invisible.
