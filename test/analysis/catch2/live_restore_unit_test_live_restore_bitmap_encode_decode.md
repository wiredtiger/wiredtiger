# test_live_restore_bitmap_encode_decode — Live restore migration bitmap encode/decode tests

**File:** `test/catch2/live_restore/unit/test_live_restore_bitmap_encode_decode.cpp`
**Storage mode:** General (Live Restore)
**Components under test:** `__ut_live_restore_encode_bitmap`, `__ut_live_restore_decode_bitmap`
**Test type:** Unit

## TEST_CASE: "Live restore bitmap encode and decode" [live_restore_bitmap]
- **What it tests:** Encoding a byte array to a hex string and decoding that hex string back to a byte array produces the original data, for 9 test bitmap patterns including:
  - All zeros
  - All ones (0xFF)
  - Alternating patterns (0xAA, 0x55)
  - Mixed patterns
- **Components:** `__ut_live_restore_encode_bitmap`, `__ut_live_restore_decode_bitmap`
- **Notes:** The bitmap is stored as a hex string in WiredTiger metadata. This test verifies the serialization round-trip is lossless for all tested patterns.
