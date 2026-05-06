# test_colgap — Column-store gap handling and near-maximum record number behavior

**File:** `test/suite/test_colgap.py`
**Storage mode:** General
**Components under test:** column-store btree, sparse record numbers, cursor traversal, near-max recno

## Test Cases

### `test_column_store_gap.test_column_store_gap`
- **What it tests:** Verifies that column-store tables with large gaps in record numbers (sparse inserts at widely separated recnos) can be traversed correctly with forward and backward cursors, and that gap records are returned as empty/zero values.
- **Components:** `src/btree/col_modify.c`, `src/cursor/cur_col.c`, `src/btree/bt_walk.c`
- **Notes:** Inserts at non-contiguous recnos (e.g., 1, 1000, 1000000) and iterates the entire table. Verifies that gap records between inserted values are returned with empty/null values and that cursor next/prev correctly traverses the entire logical record number space.

### `test_colmax.test_colmax_*` (multiple scenarios)
- **What it tests:** Verifies that operations near the maximum possible record number (close to `UINT64_MAX` for column stores) do not cause overflow, incorrect behavior, or crashes.
- **Components:** `src/btree/col_modify.c`, `src/cursor/cur_col.c`, `src/btree/`
- **Notes:** Scenarios generated via `make_scenarios` covering combinations of key_format (`r`), value types, and near-max recno values. Tests insert, search, next, and prev operations near the record number upper bound. Verifies that recno arithmetic does not overflow and that boundary conditions are handled safely.
