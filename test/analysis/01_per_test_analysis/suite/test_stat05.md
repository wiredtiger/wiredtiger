# test_stat05 — Size-only statistics cursor on various table types

**File:** `test/suite/test_stat05.py`
**Storage mode:** General
**Components under test:** statistics cursor (`statistics=(size)`), in-memory mode

## Test Cases

### `test_stat_cursor_config.test_stat_cursor_size`
- **What it tests:** Opens a `statistics=(size)` cursor on a populated table and walks all entries, verifying the cursor opens and iterates without error; repeated during bulk insertion at every 100th operation.
- **Components:** `stat.c`, `block_mgr.c`
- **Notes:** Parameterized over 7 scenarios: file-row, file-var, table-row, table-var, inmem-row (`in_memory=true`), inmem-var, complex-row. The `size` stat cursor avoids expensive tree traversal and is expected to always succeed regardless of database statistics setting (`fast` here).
