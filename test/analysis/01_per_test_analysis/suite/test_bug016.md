# test_bug016 — WT-2757: cursor.get_key() after cursor.insert()

**File:** `test/suite/test_bug016.py`
**Storage mode:** General
**Components under test:** cursor insert, cursor get_key, column-store append

## Test Cases

### `test_bug016.test_simple_column_store_append`
- **What it tests:** Inserts into a simple column-store (`file:`) table opened with `append`. After `insert()`, `get_key()` must succeed and return 1.
- **Components:** `src/cursor/cur_file.c`

### `test_bug016.test_simple_column_store`
- **What it tests:** Inserts into a simple column-store table without append (`set_key(37)` first). After `insert()`, `get_key()` must fail with `requires key be set`.
- **Components:** `src/cursor/cur_file.c`

### `test_bug016.test_simple_row_store`
- **What it tests:** Inserts into a simple row-store (`file:`) table. After `insert()`, `get_key()` must fail with `requires key be set`.
- **Components:** `src/cursor/cur_file.c`

### `test_bug016.test_complex_column_store_append`
- **What it tests:** Same as `test_simple_column_store_append` but using a `table:` URI with named columns. `get_key()` must return 1 after append insert.
- **Components:** `src/cursor/cur_table.c`

### `test_bug016.test_complex_column_store`
- **What it tests:** Same as `test_simple_column_store` but using a `table:` URI. `get_key()` must fail with `requires key be set`.
- **Components:** `src/cursor/cur_table.c`

### `test_bug016.test_complex_row_store`
- **What it tests:** Same as `test_simple_row_store` but using a `table:` URI. `get_key()` must fail with `requires key be set`.
- **Components:** `src/cursor/cur_table.c`

**Notes:** Non-parametrized. The bug was that `get_key()` always failed after `insert()` even for the column-store append case (where the assigned recno is a new output).
