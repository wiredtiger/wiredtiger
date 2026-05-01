# test_bounds_restore — Cursor bounds save and restore flag tests

**File:** `test/catch2/cursors/unit/test_bounds_restore.cpp`
**Storage mode:** General
**Components under test:** `__wt_cursor_bounds_save`, `__wt_cursor_bounds_restore`
**Test type:** Unit

## TEST_CASE: "Cursor bounds save and restore" [cursor_bounds]
### SECTION: "non-inclusive bounds"
- **What it tests:** Saving a cursor with non-inclusive lower and upper bounds and restoring them produces a cursor with the same bounds and the `WT_CURSTD_BOUND_LOWER`/`WT_CURSTD_BOUND_UPPER` flags set but without `WT_CURSTD_BOUND_LOWER_INCLUSIVE`/`WT_CURSTD_BOUND_UPPER_INCLUSIVE`.
- **Components:** `__wt_cursor_bounds_save`, `__wt_cursor_bounds_restore`, cursor flags
- **Notes:** Verifies the flag bitmask is preserved exactly across save/restore.

### SECTION: "inclusive bounds"
- **What it tests:** Saving and restoring cursor bounds that are inclusive sets both the bound flag and the inclusive flag in the restored cursor.
- **Components:** `__wt_cursor_bounds_save`, `__wt_cursor_bounds_restore`, cursor flags
- **Notes:** Checks that both `WT_CURSTD_BOUND_LOWER_INCLUSIVE` and `WT_CURSTD_BOUND_UPPER_INCLUSIVE` survive the round-trip.
