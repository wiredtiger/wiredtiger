# test_stat03 — Statistics cursor reset reloads values

**File:** `test/suite/test_stat03.py`
**Storage mode:** General
**Components under test:** statistics cursor, btree entry count

## Test Cases

### `test_stat_cursor_reset.test_stat_cursor_reset`
- **What it tests:** Verifies that after inserting one additional record, calling `statcursor.reset()` causes `btree_entries` to reflect the new count; also confirms that per-index and per-colgroup statistics cursors show the correct un-multiplied entry count for complex datasets.
- **Components:** `stat.c`, `btree`, `schema.c` (colgroups/indices)
- **Notes:** Parameterized over 6 scenarios: file-simple-row, file-simple-var, table-simple-row, table-simple-var, table-complex-row, table-complex-var. For `SimpleDataSet` the entry multiplier is 1; for `ComplexDataSet` it is `colgroup_count + index_count`.
