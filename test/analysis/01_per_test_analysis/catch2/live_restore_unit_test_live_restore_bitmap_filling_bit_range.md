# test_live_restore_bitmap_filling_bit_range — Live restore migration bitmap bit-range fill tests

**File:** `test/catch2/live_restore/unit/test_live_restore_bitmap_filling_bit_range.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `__ut_live_restore_fh_fill_bit_range`
**Test type:** Unit

## TEST_CASE: "Live restore bitmap fill bit range" [live_restore_bitmap]
- **What it tests:** `__ut_live_restore_fh_fill_bit_range` sets bits in the migration bitmap for 11 test scenarios:
  1. Single bit at position 0
  2. Single bit at position in the middle
  3. Multiple consecutive bits (sub-byte range)
  4. Multiple bits spanning a byte boundary
  5. Range extending beyond the current bitmap size
  6. Entire bitmap set (all bits)
  7. Multiple overlapping ranges applied sequentially
  8. Range starting at the last valid bit
  9. Zero-length range (no-op)
  10. Range exactly one byte wide
  11. Range spanning all bytes of the bitmap
- **Components:** `__ut_live_restore_fh_fill_bit_range`, migration bitmap
- **Notes:** Each scenario verifies the exact resulting bitmap byte values. The function is the core mechanism for recording which file regions have been migrated to the destination.
