# test_block_api_write — Block manager write/read/write_size API tests

**File:** `test/catch2/block/api/test_block_api_write.cpp`
**Storage mode:** General
**Components under test:** Block manager write/read pipeline (`bm->write`, `bm->read`, `bm->write_size`)
**Test type:** API contract

## TEST_CASE: "BlockManager: write" [block_api]
### SECTION: "write_size"
- **What it tests:** `bm->write_size` returns the minimum allocation-aligned write size.
- **Components:** `WT_BM`, allocation size rounding
- **Notes:** Returned size must be a multiple of the allocation size.

### SECTION: "simple write"
- **What it tests:** A single write of the minimum block size round-trips correctly through `bm->write` then `bm->read`.
- **Components:** `WT_BM`, block I/O
- **Notes:** Verifies data integrity after write+read.

### SECTION: "complex write, data size < alloc size"
- **What it tests:** Writing data smaller than one allocation unit still produces a correctly-padded block.
- **Components:** `WT_BM`, allocation padding
- **Notes:** The block on disk is padded to the allocation size.

### SECTION: "complex write, data size changes"
- **What it tests:** Writing different-sized buffers successively works correctly.
- **Components:** `WT_BM`, variable-size block I/O
- **Notes:** Each write gets a fresh address cookie.

### SECTION: "os_cache_dirty_max"
- **What it tests:** The `os_cache_dirty_max` configuration option is respected during writes.
- **Components:** `WT_BM`, OS buffer cache management
- **Notes:** Ensures flushing behavior does not cause errors.
