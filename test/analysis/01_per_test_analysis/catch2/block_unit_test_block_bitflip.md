# test_block_bitflip — Single-bit memory corruption detection unit tests

**File:** `test/catch2/block/unit/test_block_bitflip.cpp`
**Storage mode:** General
**Components under test:** Bit-flip detection (`__ut_block_bitflip_detect`, `WT_BITFLIP_MAX_SIZE`)
**Test type:** Unit

## TEST_CASE: "Detect single bit flip in first byte" [block_bitflip]
- **What it tests:** A single-bit flip in the very first byte of a buffer is correctly detected.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Flips each of the 8 bits in byte 0 and confirms detection.

## TEST_CASE: "Detect single bit flip in middle byte" [block_bitflip]
- **What it tests:** A single-bit flip in a middle byte of a multi-byte buffer is detected.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Ensures detection is not limited to boundary bytes.

## TEST_CASE: "Detect single bit flip in last byte" [block_bitflip]
- **What it tests:** A single-bit flip in the final byte of the buffer is detected.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Boundary condition at the end of the buffer.

## TEST_CASE: "No flip detected when data is unchanged" [block_bitflip]
- **What it tests:** The function returns false (no flip) when the original and copy are identical.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Baseline correctness — no false positives.

## TEST_CASE: "Size limits: at and above WT_BITFLIP_MAX_SIZE" [block_bitflip]
### SECTION: "at WT_BITFLIP_MAX_SIZE"
- **What it tests:** Detection works on a buffer exactly equal to `WT_BITFLIP_MAX_SIZE` bytes.
- **Components:** `__ut_block_bitflip_detect`, `WT_BITFLIP_MAX_SIZE`
- **Notes:** Checks that the maximum supported size is handled without overflow.

### SECTION: "above WT_BITFLIP_MAX_SIZE"
- **What it tests:** Buffers larger than `WT_BITFLIP_MAX_SIZE` are skipped (function returns false without scanning).
- **Components:** `__ut_block_bitflip_detect`, `WT_BITFLIP_MAX_SIZE`
- **Notes:** Verifies the size cap is enforced.

## TEST_CASE: "All bit positions in one byte" [block_bitflip]
- **What it tests:** All 8 bit positions (bits 0–7) within a single byte are individually detected.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Exhaustive bit-position coverage within one byte.

## TEST_CASE: "Edge cases: single byte, all zeros, all ones" [block_bitflip]
### SECTION: "single byte buffer"
- **What it tests:** A 1-byte buffer with one bit flipped is detected.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Minimum possible buffer size.

### SECTION: "all zeros with one flip"
- **What it tests:** Flip detection on a buffer initialized to all-zero bytes.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** 0x00 → 0x01 style flip.

### SECTION: "all ones with one flip"
- **What it tests:** Flip detection on a buffer initialized to all-0xFF bytes.
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** 0xFF → 0xFE style flip.

## TEST_CASE: "Data integrity: data is restored after detection" [block_bitflip]
- **What it tests:** After the detection function runs, the comparison copy is not modified (original data integrity preserved).
- **Components:** `__ut_block_bitflip_detect`
- **Notes:** Confirms the function is read-only with respect to the original buffer.
