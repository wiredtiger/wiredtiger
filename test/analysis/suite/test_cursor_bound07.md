# test_cursor_bound07 — Cursor bound with column-store: RLE records, deleted ranges, insert list

**File:** `test/suite/test_cursor_bound07.py`
**Storage mode:** General
**Components under test:** cursor bound API, column-store (VLCS), RLE records, fast truncate

## Test Cases

### `test_cursor_bound07.test_bound_next_scenario`
- **What it tests:** Column-store specific bound tests: RLE (run-length encoded) records within bounds, deleted record ranges at boundaries, insert-list records (not yet reconciled) at boundaries, and the boundary between RLE records and normal records. Verifies that `next()`/`prev()` with bounds correctly handles all column-store page formats.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_col.c`, `src/btree/bt_cursor.c`
- **Notes:** Column-store only (`key_format=r`). Scenarios: file/table × prev/next × evict × RLE record combinations. Tests include: deleted ranges at lower bound, deleted ranges at upper bound, insert-list records at both ends of the bound, and mixed RLE/non-RLE pages.
