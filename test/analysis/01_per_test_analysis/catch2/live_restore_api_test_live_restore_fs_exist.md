# test_live_restore_fs_exist — Live restore filesystem file existence tests

**File:** `test/catch2/live_restore/api/test_live_restore_fs_exist.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fs_exist` on `WTI_LIVE_RESTORE_FS`
**Test type:** API contract

## TEST_CASE: "Live restore filesystem: file existence" [live_restore_fs]
- **What it tests:** `fs_exist` returns the correct existence boolean for all 16 combinations of:
  - File present in destination (yes/no)
  - File present in source (yes/no)
  - Migrating flag set (yes/no)
  - Stop file (tombstone) present (yes/no)
- **Components:** `fs_exist`, `WTI_LIVE_RESTORE_FS`, tombstone files
- **Notes:** Exhaustive 16-case inline test. Key rules: a stop file (`.stop`) means the file is considered deleted even if the destination file exists. A source-only file is visible if no tombstone blocks it.
