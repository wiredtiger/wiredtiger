# test_index03 — Cursors cannot stay open while a new index is created or dropped

**File:** `test/suite/test_index03.py`
**Storage mode:** General
**Components under test:** schema/index, cursor lifecycle, exclusive locking

## Test Cases

### `test_index03.test_index_create`
- **What it tests:** Verifies that creating a new index while a cursor is open on the base table fails with `WiredTigerError`, with the expected error message `"Can't create an index for table"`. Also verifies that dropping an index while a cursor has active modifications fails.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/cursor/`, `src/session/session_api.c`
- **Notes:** Two-part test:
  1. Open `c1` on the table, attempt to create `index2` while `c1` is open → error. Close `c1`, then create `index2` successfully.
  2. Open `c1` again, insert 100 rows through it (making it "active"), attempt to drop `index2` → error. Close `c1`, then drop `index2` successfully via `dropUntilSuccess`.

  Covers the schema lock contention between concurrent cursor operations and DDL.
