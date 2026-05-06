# test_cursor_random — next_random cursor: supported operations, empty table, insert list, on-disk, deleted records, invisible data

**File:** `test/suite/test_cursor_random.py`
**Storage mode:** General
**Components under test:** cursor next_random, random cursor, row-store, visibility

## Test Cases

### `test_cursor_random.test_cursor_random`
- **What it tests:** Opens a `next_random=true` cursor and verifies that only `next()`, `reconfigure()`, and `reset()` are supported; all other operations (`compare`, `insert`, `prev`, `remove`, `search`, `search_near`, `update`) return "Unsupported cursor" error. Also verifies `next()` returns `WT_NOTFOUND` on empty table.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Scenarios: file/table × sample (`next_random_sample_size=35`) / not-sample.

### `test_cursor_random.test_cursor_random_empty`
- **What it tests:** `next()` on an empty table returns `WT_NOTFOUND` repeatedly (5 times).
- **Components:** `src/cursor/cur_std.c`

### `test_cursor_random.test_cursor_random_single_record`
- **What it tests:** With a single record in the table, `next()` always returns that record (5 times).
- **Components:** `src/cursor/cur_std.c`

### `test_cursor_random.test_cursor_random_multiple_insert_records_small` / `test_cursor_random_multiple_insert_records_large`
- **What it tests:** With 2000 or 10000 records in the insert list (not yet reconciled to disk), calls `next()` 100 times and verifies that at least 80% unique keys are returned (distribution check).
- **Components:** `src/cursor/cur_std.c`, `src/btree/`
- **Notes:** Allocation size 512 bytes, leaf_page_max 512 bytes to exercise small pages.

### `test_cursor_random.test_cursor_random_multiple_page_records_reopen_small` / `_reopen_large` / `_small` / `_large`
- **What it tests:** With 2000 or 10000 records on disk-format pages (optionally after reopen to flush insert lists), calls `next()` 100 times and verifies 80%+ unique keys returned.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_read.c`

### `test_cursor_random.test_cursor_random_deleted_partial`
- **What it tests:** With 10000 records where the middle range (keys 10–9990) is truncated, calls `next()` 10 times and verifies the cursor finds the surviving records at the ends.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_delete.c`

### `test_cursor_random.test_cursor_random_deleted_all`
- **What it tests:** With all 10000 records deleted, `next()` returns `WT_NOTFOUND` on every call.
- **Components:** `src/cursor/cur_std.c`

### `test_cursor_random_column.test_cursor_random_column`
- **What it tests:** Opening a `next_random=true` cursor on a column-store table returns "not supported" error.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Scenarios: file/table × string value format.

### `test_cursor_random_invisible.test_cursor_random_invisible_all`
- **What it tests:** All records are inserted in an uncommitted transaction; a second session's random cursor sees `WT_NOTFOUND` (all records invisible).
- **Components:** `src/cursor/cur_std.c`, `src/txn/`

### `test_cursor_random_invisible.test_cursor_random_invisible_after`
- **What it tests:** One committed record + many uncommitted records; random cursor returns only the committed record.
- **Components:** `src/cursor/cur_std.c`, `src/txn/`

### `test_cursor_random_invisible.test_cursor_random_invisible_before`
- **What it tests:** One committed record (key 99) + many uncommitted records (keys 2–98); random cursor returns only the committed record (key 99).
- **Components:** `src/cursor/cur_std.c`, `src/txn/`
