# test_cursor_compare — Cursor compare() and equals() API

**File:** `test/suite/test_cursor_compare.py`
**Storage mode:** General
**Components under test:** cursor compare, cursor equals, index cursor comparison

## Test Cases

### `test_cursor_comparison.test_cursor_comparison`
- **What it tests:** Exercises `cursor.compare(other)`: (1) unpositioned cursor raises error "requires key be set"; (2) cursors from different data sources raise error "cursors must reference the same object"; (3) comparisons before and after positioning via `next()`/`prev()` — verifies ordering (negative/zero/positive return); (4) index cursor comparison (two cursors on the same index).
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Scenarios: file/table × integer/recno/string key formats.

### `test_cursor_comparison.test_cursor_equality`
- **What it tests:** Exercises `cursor.equals(other)`: verifies returns `True` when both cursors are positioned on the same key, `False` when on different keys. Also tests the error conditions (unpositioned, different objects).
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Same scenario matrix.
