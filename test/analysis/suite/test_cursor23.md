# test_cursor23 — get_raw_key_value() with simple schema (SimpleDataSet)

**File:** `test/suite/test_cursor23.py`
**Storage mode:** General
**Components under test:** cursor get_raw_key_value API, simple schema, table schema

## Test Cases

### `test_cursor23.test_cursor23`
- **What it tests:** Calls `cursor.get_raw_key_value()` on a `SimpleDataSet` table (file and table URIs). Verifies that it works for simple schemas. Notes that complex table schemas (with column groups or indices) are not supported by `get_raw_key_value()`.
- **Components:** `src/cursor/cur_std.c`
- **Notes:** Scenarios: file-S and table-S. Uses `SimpleDataSet` from `wtdataset`. Complex schema (multi-column) would return an error.
