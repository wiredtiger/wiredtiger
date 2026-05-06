# test_live_restore_fs_open_file — Live restore filesystem file open tests

**File:** `test/catch2/live_restore/api/test_live_restore_fs_open_file.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fs_open_file` on `WTI_LIVE_RESTORE_FS`
**Test type:** API contract

## TEST_CASE: "Live restore filesystem: open file" [live_restore_fs]
### SECTION: "File"
- **What it tests:**
  - Opening a nonexistent file without the CREATE flag returns ENOENT.
  - Opening with the CREATE flag creates the file in the destination.
  - Opening a source-only file copies it to the destination before returning the handle.
  - Opening a file that has a tombstone (`.stop`) returns ENOENT even if the source file exists.
- **Components:** `fs_open_file`, `WTI_LIVE_RESTORE_FS`, destination file copy, tombstone
- **Notes:** Source-only-file copy behavior is central to the live restore migration path.

### SECTION: "Directory"
- **What it tests:**
  - Opening a directory handle on the destination path succeeds.
  - Directories are never opened from the source; source-only directories are not copied.
  - Nonexistent directories return ENOENT.
- **Components:** `fs_open_file` with `WT_FS_OPEN_FILE_TYPE_DIRECTORY`
- **Notes:** Directory handles are used by WiredTiger for lock files and fsync of directory entries.
