# test_sub_level_error_strerror — wiredtiger_strerror sub-level error code tests

**File:** `test/catch2/sub_level_error/api/test_sub_level_error_strerror.cpp`
**Storage mode:** General
**Components under test:** `wiredtiger_strerror()` for sub-level error codes
**Test type:** API contract

## TEST_CASE: "Test wiredtiger_strerror() for sub-level error codes" [sub_level_error_strerror, sub_level_error]
### SECTION: "all sub-level error codes have string representations"
- **What it tests:** `wiredtiger_strerror()` returns a non-null, non-empty descriptive string for each of the 14 sub-level error codes:
  - `WT_NONE`
  - `WT_BACKGROUND_COMPACT_ALREADY_RUNNING`
  - `WT_CACHE_OVERFLOW`
  - `WT_WRITE_CONFLICT`
  - `WT_OLDEST_FOR_EVICTION`
  - `WT_CONFLICT_BACKUP`
  - `WT_CONFLICT_DHANDLE`
  - `WT_CONFLICT_SCHEMA_LOCK`
  - `WT_UNCOMMITTED_DATA`
  - `WT_DIRTY_DATA`
  - `WT_CONFLICT_TABLE_LOCK`
  - `WT_CONFLICT_CHECKPOINT_LOCK`
  - `WT_CONFLICT_LIVE_RESTORE`
  - `WT_CONFLICT_DISAGG`
- **Components:** `wiredtiger_strerror`, sub-level error code enumeration
- **Notes:** Ensures that every sub-level error code has a human-readable message. Catches any new codes added without a corresponding string.
