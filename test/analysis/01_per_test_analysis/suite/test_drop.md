# test_drop — Session-level drop operation correctness across object types and conditions

**File:** `test/suite/test_drop.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** schema drop, session drop, table/file/index/colgroup, transaction interaction

## Test Cases

### `test_drop.test_drop`
- **What it tests:** Exercises `session.drop()` across a matrix of conditions — with/without an open cursor, with/without reopening the connection, with/without an active transaction — for three dataset types: `SimpleDataSet` (file or table), `SimpleIndexDataSet` (table with index), and `ComplexDataSet` (multi-file table with column groups and indices). Verifies that:
  - An open cursor prevents drop (raises `WiredTigerError`).
  - An active transaction causes `EBUSY`.
  - After rollback or cursor close, drop succeeds.
  - After drop, the URI no longer exists (`confirm_does_not_exist`).
- **Components:** `src/schema/schema_drop.c`, `src/schema/schema_open.c`, `src/txn/`
- **Notes:** Scenarios: `file` (uri='file:') and `table` (uri='table:'). `SimpleIndexDataSet` and `ComplexDataSet` only tested for `table:` URI. The `drop_index` flag allows dropping an individual index URI rather than the whole table. Skipped for tiered storage hook.

### `test_drop.test_drop_dne`
- **What it tests:** Verifies that dropping a non-existent object with `force` succeeds silently, while dropping without `force` raises `WiredTigerError`. Covers table, colgroup, and index URIs.
- **Components:** `src/schema/schema_drop.c`
- **Notes:** Explicitly skipped for tiered storage hook (negative tests not compatible). Checks `table:`, `colgroup:`, and `index:` URI forms.
