# test_live_restore_fh_truncate — Live restore file handle truncate and bitmap update tests

**File:** `test/catch2/live_restore/api/test_live_restore_fh_truncate.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fh_truncate` on `WTI_LIVE_RESTORE_FILE_HANDLE`, migration bitmap
**Test type:** API contract

## TEST_CASE: "Live restore file handle: truncate" [live_restore_fh]
- **What it tests:**
  - Truncating to the same size as current: bitmap is unchanged and operation succeeds.
  - Shrinking the file: bits in the truncated region are set (marking them as migrated/no longer from source).
  - Growing the file within the current bitmap: no bitmap change; new region is blank.
  - Growing the file beyond the current bitmap boundary: bitmap is extended and new bits are initialized correctly.
  - Truncating with a completely clean (all-zero) bitmap: operates correctly.
- **Components:** `fh_truncate`, `WTI_LIVE_RESTORE_FILE_HANDLE`, migration bitmap
- **Notes:** No named sections; all sub-scenarios are sequential assertions within the test body. Bitmap state is inspected after each truncate operation.
