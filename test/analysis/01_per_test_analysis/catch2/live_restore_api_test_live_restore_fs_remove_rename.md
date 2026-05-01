# test_live_restore_fs_remove_rename — Live restore filesystem remove and rename tests

**File:** `test/catch2/live_restore/api/test_live_restore_fs_remove_rename.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fs_remove`, `fs_rename` on `WTI_LIVE_RESTORE_FS`
**Test type:** API contract

## TEST_CASE: "Live restore filesystem: remove" [live_restore_fs]
- **What it tests:**
  - Removing a file that exists in the destination creates a tombstone (`.stop`) file.
  - Removing a file not present in the destination (or source) returns ENOENT.
  - When live restore is in `COMPLETE` state, remove deletes the file without creating a tombstone.
- **Components:** `fs_remove`, tombstone files, `WTI_LIVE_RESTORE_FS` state
- **Notes:** Tombstone creation is critical so that source files are masked from `fs_exist` and `fs_directory_list`.

## TEST_CASE: "Live restore filesystem: rename" [live_restore_fs]
- **What it tests:**
  - Renaming a file creates a tombstone for the old name and removes the old destination file.
  - Renaming a source-only file returns EINVAL (source files cannot be renamed directly).
  - Renaming to a name that already exists overwrites the destination file.
  - When live restore is in `COMPLETE` state, rename performs a direct rename without creating a tombstone.
- **Components:** `fs_rename`, tombstone files, `WTI_LIVE_RESTORE_FS` state
- **Notes:** Tombstone for the old name prevents the source file from becoming visible again after rename.
