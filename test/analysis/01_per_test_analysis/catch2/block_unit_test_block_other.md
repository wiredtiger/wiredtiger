# test_block_other — Block header byte-swap and sweep eligibility tests

**File:** `test/catch2/block/unit/test_block_other.cpp`
**Storage mode:** General
**Components under test:** `__wt_block_header_byteswap_copy`, `__wt_block_header_byteswap`, `__wt_block_eligible_for_sweep`
**Test type:** Unit

## TEST_CASE: "__wt_block_header_byteswap_copy" [block_other]
### SECTION: "little-endian (no swap needed)"
- **What it tests:** On a little-endian system, the header fields are copied unchanged.
- **Components:** `__wt_block_header_byteswap_copy`, `WT_BLOCK_HEADER`
- **Notes:** Compiled-in byte-order detection; on LE systems the copy is a verbatim memcpy.

### SECTION: "big-endian (swap required)"
- **What it tests:** On a big-endian system, the header fields are byte-swapped in the copy.
- **Components:** `__wt_block_header_byteswap_copy`, `WT_BLOCK_HEADER`
- **Notes:** Only active when `__BYTE_ORDER == __BIG_ENDIAN`; verifies each field is individually swapped.

### SECTION: "source and destination do not alias"
- **What it tests:** The function writes to the destination without modifying the source.
- **Components:** `__wt_block_header_byteswap_copy`
- **Notes:** Source buffer integrity after copy.

### SECTION: "in-place swap round-trips"
- **What it tests:** Applying `__wt_block_header_byteswap` twice on the same header restores the original values.
- **Components:** `__wt_block_header_byteswap`
- **Notes:** Double-swap identity check.

## TEST_CASE: "__wt_block_eligible_for_sweep" [block_other]
- **What it tests:** Returns true for blocks that have no active references and are candidates for cache sweep.
- **Components:** `__wt_block_eligible_for_sweep`
- **Notes:** Tests with both eligible (ref_count == 0) and ineligible (ref_count > 0 or flags set) blocks.
