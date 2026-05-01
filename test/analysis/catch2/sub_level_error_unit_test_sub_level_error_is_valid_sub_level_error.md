# test_sub_level_error_is_valid_sub_level_error — Sub-level error code range validation tests

**File:** `test/catch2/sub_level_error/unit/test_sub_level_error_is_valid_sub_level_error.cpp`
**Storage mode:** General
**Components under test:** `__wt_is_valid_sub_level_error`
**Test type:** Unit

## TEST_CASE: "Test that helper function __wt_is_valid_sub_level_error validates sub level error codes correctly" [sub_level_error_is_valid_sub_level_error, sub_level_error]
- **What it tests:**
  - Normal WiredTiger error codes (`WT_ROLLBACK`, `WT_DUPLICATE_KEY`, `WT_ERROR`, `WT_NOTFOUND`, `WT_PANIC`, `WT_RESTART`, `WT_RUN_RECOVERY`, `WT_CACHE_FULL`, `WT_PREPARE_CONFLICT`, `WT_TRY_SALVAGE`) are **not** valid sub-level error codes.
  - `WT_NONE` is a valid sub-level error code.
  - Boundary values: -31999 is invalid, -32000 is valid, -32001 is valid, -32199 is valid, -32200 is invalid, -32201 is invalid.
- **Components:** `__wt_is_valid_sub_level_error`
- **Notes:** Sub-level error codes occupy the range [-32000, -32199] inclusive (200 codes total). This test pins the range boundaries to prevent accidental expansion or contraction.
