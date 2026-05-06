# test_cursor_get_raw_key_value — Cursor get_raw_key_value API tests

**File:** `test/catch2/cursors/unit/test_cursor_get_raw_key_value.cpp`
**Storage mode:** General
**Components under test:** `cursor->get_raw_key_value()`, `cursor->get_key()`, `cursor->get_value()`
**Test type:** API contract

## TEST_CASE: "Cursor get_raw_key_value" [cursor_get_raw_key_value]
### SECTION: "get_key and get_value"
- **What it tests:** The traditional `cursor->get_key()` and `cursor->get_value()` return the same data as the corresponding raw key/value items.
- **Components:** `cursor->get_key`, `cursor->get_value`
- **Notes:** Baseline comparison against `get_raw_key_value`.

### SECTION: "get_raw_key_value"
- **What it tests:** `cursor->get_raw_key_value()` populates `WT_ITEM` structs for the key and value; passing NULL for key or value skips that field without error.
- **Components:** `cursor->get_raw_key_value`
- **Notes:** Tests all three combinations: both non-null, key-only (null value), value-only (null key).

### SECTION: "unsupported cursor type (version cursor)"
- **What it tests:** Calling `get_raw_key_value` on a version cursor returns `ENOTSUP`.
- **Components:** `cursor->get_raw_key_value`, version cursor
- **Notes:** Verifies that not all cursor types implement the method.
