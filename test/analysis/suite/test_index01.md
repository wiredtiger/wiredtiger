# test_index01 — Basic index operations

**File:** `test/suite/test_index01.py`
**Storage mode:** General
**Components under test:** schema/index, cursor, btree

## Test Cases

### `test_index01.test_empty`
- **What it tests:** Creates a table with 6 secondary indexes, searches for a nonexistent key, and verifies all index cursors return empty results.
- **Components:** `src/schema/schema_index.c`, `src/cursor/cur_index.c`
- **Notes:** Table has composite key `(name:S, ID:i)` and columns `(dept, job, salary, year)`. All 6 indexes cover different column combinations.

### `test_index01.test_insert`
- **What it tests:** Inserts one record and verifies all 6 index cursors return the correct projected values in the expected order.
- **Components:** `src/schema/schema_index.c`, `src/cursor/cur_index.c`, `src/btree/`
- **Notes:** Verifies the specific column projection returned by each index cursor (index values include the indexed columns plus all value columns as the value side). Checks exact string representations.

### `test_index01.test_update`
- **What it tests:** Inserts a record, updates it (changing job title and salary), and verifies all 6 indexes reflect the new values. Also verifies that updates to nonexistent keys return `WT_NOTFOUND`.
- **Components:** `src/schema/schema_index.c`, `src/cursor/cur_index.c`
- **Notes:** Tests case-sensitive key mismatch (e.g., `'smith'` vs `'Smith'`).

### `test_index01.test_insert_overwrite`
- **What it tests:** Uses `overwrite=true` to insert-overwrite an existing record and insert a brand-new record; verifies index state is consistent. Also verifies that `overwrite=false` rejects a duplicate with `WiredTigerError`.
- **Components:** `src/schema/schema_index.c`, `src/cursor/cur_index.c`
- **Notes:** Checks that overwrite properly updates all secondary indexes.

### `test_index01.test_insert_delete`
- **What it tests:** Inserts a record, confirms it exists, removes it, and verifies all 6 index cursors are empty again.
- **Components:** `src/schema/schema_index.c`, `src/cursor/cur_index.c`
- **Notes:** Confirms cascading delete propagation to all indexes.

### `test_index01.test_exclusive`
- **What it tests:** Verifies that non-exclusive index re-creation is allowed, but exclusive (`exclusive` flag) re-creation of an existing index fails with `WiredTigerError`.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_index.c`
- **Notes:** Edge case: `exclusive` flag semantics for index creation.
