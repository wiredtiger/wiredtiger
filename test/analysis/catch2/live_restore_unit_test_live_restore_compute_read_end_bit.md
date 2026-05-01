# test_live_restore_compute_read_end_bit — Live restore compute_read_end_bit tests

**File:** `test/catch2/live_restore/unit/test_live_restore_compute_read_end_bit.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `__ut_live_restore_compute_read_end_bit`
**Test type:** Unit

## TEST_CASE: "Live restore compute read end bit" [live_restore_bitmap]
- **What it tests:** `__ut_live_restore_compute_read_end_bit` returns the correct ending bit position for a read operation, considering:
  1. Clear length = 1 (read stops at the first migrated bit)
  2. Multiple consecutive cleared bits
  3. Read length longer than the buffer size (clamped to buffer size)
  4. Read extending to the end of the bitmap
  5. Buffer size equals allocation size (boundary condition)
  6. File size greater than bitmap coverage (trailing region handling)
  7. File size less than bitmap coverage (truncated file)
- **Components:** `__ut_live_restore_compute_read_end_bit`, migration bitmap
- **Notes:** This function determines how much data to fetch from the source file in one read pass before hitting already-migrated data. It is the boundary computation for the hole-filling algorithm.
