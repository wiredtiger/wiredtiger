# test_live_restore_fh_read_write — Live restore file handle read/write tests

**File:** `test/catch2/live_restore/api/test_live_restore_fh_read_write.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fh_read`, `fh_write` on `WTI_LIVE_RESTORE_FILE_HANDLE`, migration bitmap
**Test type:** API contract

## TEST_CASE: "Live restore file handle: read and write" [live_restore_fh]
### SECTION: "with source file (background migration simulation)"
- **What it tests:** Reading from a region not yet migrated triggers a copy from the source file and sets the corresponding bitmap bits. Subsequent reads come from the destination.
- **Components:** `fh_read`, migration bitmap, source file handle, destination file handle
- **Notes:** Simulates the incremental background migration path. Bitmap bits are checked before and after the read to confirm migration tracking.

### SECTION: "without source file"
- **What it tests:** When no source file exists, reads come entirely from the destination and no bitmap operations occur.
- **Components:** `fh_read`, `fh_write`
- **Notes:** Post-migration state where all data is in the destination.

### SECTION: "writes beyond bitmap range"
- **What it tests:** Writing data past the current bitmap boundary does not corrupt the bitmap or cause errors.
- **Components:** `fh_write`, migration bitmap bounds
- **Notes:** Ensures the bitmap is not accessed out-of-bounds during writes to new regions.
