# test_bug017 — WT-2987: opening cursor on incomplete table crashes

**File:** `test/suite/test_bug017.py`
**Storage mode:** General
**Components under test:** cursor open, column groups, schema validation

## Test Cases

### `test_bug017.test_bug017_run`
- **What it tests:** Reproduces WT-2987 where opening a cursor on a table whose column groups had not yet been fully created triggered a crash (NULL dereference). Creates a table with two column groups (`main` and `population`) but without creating the underlying column-group files. Then attempts to open a cursor with a column projection (`table:bug17(country)`), which must raise `WiredTigerError` with the message `column groups` instead of crashing.
- **Components:** `src/cursor/cur_table.c`, `src/schema/schema_open.c`
- **Notes:** Non-parametrized. The error message check is `/column groups/`.
