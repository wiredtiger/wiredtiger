# test_cursor21 — Cursor reposition debug mode and evict_reposition timing stress

**File:** `test/suite/test_cursor21.py`
**Storage mode:** General
**Components under test:** cursor reposition, debug_mode, timing stress, cursor stats

## Test Cases

### `test_cursor21.test_cursor21`
- **What it tests:** With `debug_mode=[cursor_reposition=true]` and `timing_stress_for_test=(evict_reposition)`, inserts 9999 records and then exercises `next()`, `prev()`, `search()`, and `search_near()` operations. Verifies that the `cursor_reposition` statistic increments for each operation group when reposition mode is active, and stays at 0 when disabled.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`, `src/evict/`
- **Notes:** Scenarios: column (`key_format=r`) and row_integer (`key_format=i`) × no_reposition/reposition. The reposition stat counts how many times a cursor had to re-establish its position after the underlying page was evicted between operations.
