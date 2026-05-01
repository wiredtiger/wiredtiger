# test_cursor22 — cursor.get_raw_key_value() atomic key+value read

**File:** `test/suite/test_cursor22.py`
**Storage mode:** General
**Components under test:** cursor get_raw_key_value API

## Test Cases

### `test_cursor22.test_cursor22`
- **What it tests:** Calls `cursor.get_raw_key_value()` and verifies the returned `(key, value)` tuple matches individual `cursor.get_key()` / `cursor.get_value()` calls. Also tests partial destructuring (ignoring one of the return values) and ignoring the entire result.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Tests the atomicity guarantee: both key and value are fetched in a single call. Uses a simple file URI with string key/value format.
