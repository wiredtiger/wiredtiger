# test_base04 — Empty table reconciliation: insert and delete with optional connection reopen

**File:** `test/suite/test_base04.py`
**Storage mode:** General
**Components under test:** cursor API, reconciliation, schema (create/drop)

## Test Cases

### `test_base04.test_empty`
- **What it tests:** Creates a table and searches for a nonexistent key, asserting `WT_NOTFOUND`, then drops the table.
- **Components:** `src/btree/bt_search.c`

### `test_base04.test_insert`
- **What it tests:** Twice (with and without connection reopen after insert): creates a table, inserts `key1=value1`, then searches confirming the key is found (return 0).
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_walk.c`
- **Notes:** The `reconcile` flag triggers `reopen_conn()` after each insert/remove, forcing data to disk.

### `test_base04.test_insert_delete`
- **What it tests:** Twice: creates a table, inserts, confirms found, removes, confirms `WT_NOTFOUND`. Tests that after delete the btree is properly empty.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_walk.c`
- **Notes:** Non-parametrized. Key/value format is `S/S`.
