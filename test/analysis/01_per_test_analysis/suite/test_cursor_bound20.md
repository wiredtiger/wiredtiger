# test_cursor_bound20 — Cursor bound edge cases: max fixed-string and max byte-array as upper bound

**File:** `test/suite/test_cursor_bound20.py`
**Storage mode:** General
**Components under test:** cursor bound API, index cursor, increment_bounds_array, fixed-length string, byte array key formats

## Test Cases

### `test_cursor_bound20.test_cursor_index_bounds_fixed`
- **What it tests:** Sets the upper bound to the maximum possible fixed-length string value (`chr(127)*4` for `value_format='4s'`) on an index cursor. Verifies that `increment_bounds_array` handles the max fixed-string case without overflow: traversal still finds all keys, `search_near()` returns exact match, and an exclusive lower bound at the max value returns `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_index.c`
- **Notes:** `key_format=S`, `value_format='4s'`. MAX_FIXED_STRING = `chr(127)+chr(127)+chr(127)+chr(127)`.

### `test_cursor_bound20.test_cursor_index_bounds_byte`
- **What it tests:** Sets the upper bound to the maximum byte array value (`0xFFFF` as 2-byte array) on an index cursor. Verifies that `increment_bounds_array` handles the max byte-array case without overflow: traversal finds all keys, `search_near()` returns exact match, and exclusive lower bound at max value returns `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_index.c`
- **Notes:** `key_format=S`, `value_format='u'`. MAX_BYTE_ARRAY = `bytes([0xFF, 0xFF])`.
