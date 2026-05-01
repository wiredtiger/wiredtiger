# test_live_restore_fh_lock_close_sync — Live restore file handle lock, sync, and close tests

**File:** `test/catch2/live_restore/api/test_live_restore_fh_lock_close_sync.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `fh_lock`, `fh_sync`, `fh_close` on `WTI_LIVE_RESTORE_FILE_HANDLE`
**Test type:** API contract

## TEST_CASE: "Live restore file handle: lock, sync, close" [live_restore_fh]
- **What it tests:**
  - `fh_lock`: Re-entrant locking is supported; locking and unlocking return 0.
  - `fh_sync`: Sync succeeds both before and after a write; calls through to the underlying destination file handle.
  - `fh_close`: Closing the live restore file handle releases resources and returns 0.
- **Components:** `WTI_LIVE_RESTORE_FILE_HANDLE`, `WTI_LIVE_RESTORE_FS`
- **Notes:** No sections — all sub-scenarios are sequential within the single test body. Tests that the wrapper layer correctly delegates to the underlying WT file system handle.
