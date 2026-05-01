# test_live_restore_fs_size — Live restore filesystem file size tests

**File:** `test/catch2/live_restore/api/test_live_restore_fs_size.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fs_size` on `WTI_LIVE_RESTORE_FS`
**Test type:** API contract

## TEST_CASE: "Live restore filesystem: file size" [live_restore_fs]
- **What it tests:** `fs_size` returns the correct size for all 16 combinations of {dest present, source present, migrating flag, stop file present}:
  - When the destination file exists: always returns `DEST_FILE_SIZE = 10`.
  - When only the source exists and the migrating flag is set: returns `SOURCE_FILE_SIZE = 100`.
  - When neither destination nor migrating source exists (or a tombstone blocks access): returns ENOENT.
- **Components:** `fs_size`, `WTI_LIVE_RESTORE_FS`, tombstone files
- **Notes:** Exhaustive 16-case test. Distinguishes between the destination size (always authoritative when dest exists) and the source size (used only when destination is not yet created and migration is active).
