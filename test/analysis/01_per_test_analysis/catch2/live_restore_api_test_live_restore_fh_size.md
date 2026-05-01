# test_live_restore_fh_size — Live restore file handle size tests

**File:** `test/catch2/live_restore/api/test_live_restore_fh_size.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fh_size` on `WTI_LIVE_RESTORE_FILE_HANDLE`
**Test type:** API contract

## TEST_CASE: "Live restore file handle: size" [live_restore_fh]
- **What it tests:** `fh_size` always returns the destination file size (`DEST_FILE_SIZE = 10`) regardless of whether a source file, migrating state, or stop file is present.
- **Components:** `fh_size`, `WTI_LIVE_RESTORE_FILE_HANDLE`
- **Notes:** Exhaustively tests all 8 permutations of {source present, migrating flag set, stop file present}. All 8 cases must return `DEST_FILE_SIZE`. This confirms that the live restore layer hides the source file size from callers.
