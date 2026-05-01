# test_cursor_bound02 — Cursor bound overlap and ordering validation (lower > upper, reset, clear)

**File:** `test/suite/test_cursor_bound02.py`
**Storage mode:** General
**Components under test:** cursor bound API, bound validation, bound reset, bound clear

## Test Cases

### `test_cursor_bound02.test_bound_api`
- **What it tests:** Tests bounds overlap validation: setting upper bound below lower bound returns error; setting lower bound above upper bound returns error; equal bounds require both inclusive flags set; validates all 7 key formats (S, r, i, u, SSS, iS, iSru) and 2 value formats with inclusive/non-inclusive combinations.
- **Components:** `src/cursor/cur_bound.c`
- **Notes:** Scenarios: file/table/colgroup × 7 key formats × 2 value formats × inclusive/no-inclusive.

### `test_cursor_bound02.test_bound_api_reset`
- **What it tests:** `cursor.reset()` clears any active bounds; verifies that after reset, the cursor can traverse the full key range again.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`

### `test_cursor_bound02.test_bound_api_clear`
- **What it tests:** `cursor.bound("action=clear")` removes lower or upper bound independently; verifies that clearing one bound does not affect the other.
- **Components:** `src/cursor/cur_bound.c`
