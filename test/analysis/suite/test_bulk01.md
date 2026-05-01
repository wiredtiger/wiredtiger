# test_bulk01 — Bulk-load smoke tests across formats

**File:** `test/suite/test_bulk01.py`
**Storage mode:** General
**Components under test:** bulk cursor, column-store RLE, bulk append, out-of-order detection, statistics

## Test Cases

### `test_bulk_load.test_bulk_load`
- **What it tests:** Basic bulk-load smoke test: creates a URI, verifies `cursor_bulk_count` stat is 0 before opening the bulk cursor, opens `bulk` cursor, inserts 999 key/value pairs, closes it, and verifies `cursor_bulk_count` returns to 0.
- **Components:** `src/cursor/cur_bulk.c`, `src/conn/conn_stat.c`

### `test_bulk_load.test_bulk_load_var_rle`
- **What it tests:** VLCS (`keyfmt=r`) only. Inserts 999 records where the value is `i // 7` — creating runs of identical values. Verifies no diagnostic assertion fires, which would indicate that RLE compression was expected but skipped.
- **Components:** `src/cursor/cur_bulk.c`, `src/btree/col_modify.c`

### `test_bulk_load.test_bulk_load_var_append`
- **What it tests:** VLCS only. Opens bulk cursor with `bulk,append`, inserts 999 records all with key=37 (ignored in append mode). Closes and re-reads with a normal cursor to confirm records were assigned sequential keys 1–999.
- **Components:** `src/cursor/cur_bulk.c`

### `test_bulk_load.test_bulk_load_col_delete`
- **What it tests:** VLCS only. Inserts only every 7th key (skipping the rest) via bulk cursor. Then inserts one past the gap to force creation of missing records. Re-reads with a normal cursor to verify that keys that were inserted exist, and keys that were skipped return `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_bulk.c`, `src/btree/col_modify.c`

### `test_bulk_load.test_bulk_load_col_big`
- **What it tests:** VLCS only. Inserts records 1–9, then inserts a very large record number (18446744073709551606 — near `UINT64_MAX`). Verifies the large-recno record can be read back without hanging.
- **Components:** `src/cursor/cur_bulk.c`

### `test_bulk_load.test_bulk_load_order_check`
- **What it tests:** Verifies that bulk-load rejects out-of-order key insertion. Inserts key 10, then attempts to insert keys 1, 9, and 10; each must raise `WiredTigerError` with `less than or equal to the previously inserted key`. Then inserts key 11 (in order) and confirms no error.
- **Components:** `src/cursor/cur_bulk.c`

### `test_bulk_load.test_bulk_load_row_order_nocheck`
- **What it tests:** Row-store `bulk,skip_sort_check` fast path. Currently skipped (`self.skipTest`) because the error return was changed to an assertion. Historically tested that out-of-order keys with `skip_sort_check` triggered `are incorrectly sorted` on `conn.close()`.
- **Components:** `src/cursor/cur_bulk.c`
- **Notes:** Skipped unconditionally.

### `test_bulk_load.test_bulk_load_not_empty`
- **What it tests:** Verifies that bulk-load is rejected on a table that already has data. Inserts one record, checkpoints, then attempts to open a bulk cursor — must raise `WiredTigerError` with `bulk-load is only supported on newly created objects`.
- **Components:** `src/cursor/cur_bulk.c`

### `test_bulk_load.test_bulk_load_busy`
- **What it tests:** Verifies that opening a bulk cursor fails with `WiredTigerError` when another cursor is already open on the table (EBUSY).
- **Components:** `src/cursor/cur_bulk.c`

**Notes:** Parametrized across: `types` (file, table) × `keyfmt` (integer i, recno r, string S) × `valfmt` (integer i, string S) = 12 combinations. Column-store-only tests short-circuit when `keyfmt != 'r'`.
