# test_live_restore_fill_hole — Live restore background migration fill_hole tests

**File:** `test/catch2/live_restore/unit/test_live_restore_fill_hole.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `__ut_live_restore_fill_hole`
**Test type:** Unit

## TEST_CASE: "Live restore fill hole" [live_restore_fill_hole]
### SECTION: "single call — 6 sub-scenarios"
- **What it tests:** A single call to `__ut_live_restore_fill_hole` correctly copies one unmigrated region (hole) from the source file to the destination and updates the bitmap. Sub-scenarios include:
  - Hole at the beginning of the file
  - Hole in the middle (already-migrated data on both sides)
  - Hole at the end of the file
  - Entire file is a hole
  - Source file does not exist (no operation needed)
  - Zero-size hole (no-op)
- **Components:** `__ut_live_restore_fill_hole`, migration bitmap, source file, destination file
- **Notes:** Each sub-scenario verifies that exactly the expected bitmap bits are set after the fill.

### SECTION: "multiple calls until fully migrated"
- **What it tests:** Calling `__ut_live_restore_fill_hole` repeatedly until it returns "migration complete" results in a fully-set bitmap (all bits 1).
- **Components:** `__ut_live_restore_fill_hole`, migration bitmap
- **Notes:** Samples 50 intermediate bitmap values and verifies they are monotonically increasing. At completion, all bits must be set and the function must signal migration done.
