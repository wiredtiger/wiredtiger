# test_block_file — Block file open/close configuration tests

**File:** `test/catch2/block/unit/test_block_file.cpp`
**Storage mode:** General
**Components under test:** `__wt_block_open`, `__wti_bm_close_block`
**Test type:** Unit

## TEST_CASE: "Block file open and close" [block_file]
### SECTION: "default configuration"
- **What it tests:** Opening a block file with no extra configuration succeeds and produces a valid `WT_BLOCK`.
- **Components:** `__wt_block_open`
- **Notes:** Default allocation size, first-fit block allocation.

### SECTION: "allocation size priority"
- **What it tests:** The `allocation_size` config key overrides the default; the block's `allocsize` field reflects the configured value.
- **Components:** `__wt_block_open`, `allocsize` field
- **Notes:** Tests multiple sizes (512 B, 4 KB, 128 KB).

### SECTION: "block_allocation=best"
- **What it tests:** Opening with `block_allocation=best` configures the block manager to use best-fit allocation.
- **Components:** `__wt_block_open`, `WT_BLOCK_BEST_ALLOC`
- **Notes:** Verifies the flag is set in the block structure.

### SECTION: "block_allocation=first"
- **What it tests:** Opening with `block_allocation=first` configures first-fit allocation.
- **Components:** `__wt_block_open`, first-fit strategy
- **Notes:** Default mode.

### SECTION: "block_allocation=garbage (invalid)"
- **What it tests:** An invalid `block_allocation` value returns an error code.
- **Components:** `__wt_block_open`
- **Notes:** Expects a non-zero return value.

### SECTION: "block_allocation missing"
- **What it tests:** Omitting `block_allocation` defaults to first-fit without error.
- **Components:** `__wt_block_open`
- **Notes:** Covers the default fallback path.

### SECTION: "os_cache_max and os_cache_dirty_max"
- **What it tests:** The `os_cache_max` and `os_cache_dirty_max` configuration values are stored in the block.
- **Components:** `__wt_block_open`, OS buffer cache limits
- **Notes:** Verifies fields in the `WT_BLOCK` struct.

### SECTION: "read-only"
- **What it tests:** Opening a block file in read-only mode succeeds.
- **Components:** `__wt_block_open`, `WT_BLOCK_READONLY`
- **Notes:** The block is opened without write permissions.

### SECTION: "close with sync"
- **What it tests:** `__wti_bm_close_block` with `sync=true` flushes and closes the block file cleanly.
- **Components:** `__wti_bm_close_block`
- **Notes:** Verifies that close returns 0 and the block pointer is nulled.
