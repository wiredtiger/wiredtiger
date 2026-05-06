# test_block_api_misc — Block manager miscellaneous API surface tests

**File:** `test/catch2/block/api/test_block_api_misc.cpp`
**Storage mode:** General
**Components under test:** Block manager API (`bm->addr_invalid`, `bm->addr_string`, `bm->block_header`, `bm->is_mapped`, `bm->size`, `bm->stat`)
**Test type:** API contract

## TEST_CASE: "BlockManager: addr invalid" [block_api]
- **What it tests:** `bm->addr_invalid` returns 0 for a zero-length item and non-zero for a non-empty item.
- **Components:** `WT_BM`, block address validation
- **Notes:** Uses a real block manager opened via `__wt_block_manager_open`.

## TEST_CASE: "BlockManager: addr string" [block_api]
- **What it tests:** `bm->addr_string` returns a human-readable address string and the returned string matches expectations.
- **Components:** `WT_BM`, address formatting
- **Notes:** Validates that the string is non-null and has non-zero length.

## TEST_CASE: "BlockManager: block header" [block_api]
- **What it tests:** `bm->block_header` returns the size of the block header in bytes.
- **Components:** `WT_BM`, `WT_BLOCK_HEADER`
- **Notes:** Checks that the returned size equals `sizeof(WT_BLOCK_HEADER)`.

## TEST_CASE: "BlockManager: is mapped" [block_api]
- **What it tests:** `bm->is_mapped` returns 0 (false) indicating the block is not memory-mapped by default.
- **Components:** `WT_BM`, memory-mapped I/O
- **Notes:** Block manager files are not mapped by default in the test configuration.

## TEST_CASE: "BlockManager: size and stat" [block_api]
- **What it tests:** `bm->size` returns the file size and `bm->stat` returns statistics without error.
- **Components:** `WT_BM`, file size, statistics
- **Notes:** Stat is verified to return 0 (success).
