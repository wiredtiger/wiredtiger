# test_dupc — Cursor duplication via session.open_cursor(None, source_cursor, None)

**File:** `test/suite/test_dupc.py`
**Storage mode:** General
**Components under test:** cursor duplication, cursor compare, file/table cursors

## Test Cases

### `test_duplicate_cursor.test_duplicate_cursor`
- **What it tests:** Iterates through a populated dataset and at every record duplicates the current cursor with `session.open_cursor(None, cursor, None)`. Verifies that:
  - `cursor.compare(dupc) == 0` (both cursors are positioned at the same record).
  - `dupc.get_key()` returns the same key as the original cursor.
  - Iteration continues correctly after swapping `cursor = dupc`.
  - After full iteration, `next()` returns `WT_NOTFOUND` and the total count equals `nentries` (1000).
  Tests both `SimpleDataSet` (one file) and `ComplexDataSet` (multi-file table, table: URI only).
- **Components:** `src/cursor/cur_std.c`, `src/cursor/cur_btree.c`, `src/schema/schema_open.c`
- **Notes:** Scenarios: `file-r`, `file-S`, `table-r`, `table-S` (keyfmt=r or S, valfmt=S). Skipped for tiered storage. `ComplexDataSet` is only tested when `uri == "table:"`. Duplicate cursor shares the same transaction context and position.
