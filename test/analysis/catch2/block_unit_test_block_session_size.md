# test_block_session_size — Block session size-struct allocation/free/discard tests

**File:** `test/catch2/block/unit/test_block_session_size.cpp`
**Storage mode:** General
**Components under test:** `__ut_block_size_alloc`, `__ut_block_size_prealloc`, `__wti_block_size_alloc`, `__wti_block_size_free`, `__ut_block_size_discard`
**Test type:** Unit

## TEST_CASE: "__ut_block_size_alloc: null BM session" [block_session_size]
- **What it tests:** When the block manager session is null, size structs are allocated directly from the heap.
- **Components:** `__ut_block_size_alloc`
- **Notes:** Mirrors the ext alloc null-session test for the `WT_SIZE` struct.

## TEST_CASE: "__ut_block_size_prealloc" [block_session_size]
### SECTION: "fill cache to requested count"
- **What it tests:** After `__ut_block_size_prealloc(n)`, the sz_cache holds exactly n entries.
- **Components:** `__ut_block_size_prealloc`
- **Notes:** Cache count tracking for `WT_SIZE` entries.

### SECTION: "idempotent when cache is full"
- **What it tests:** Prealloc does nothing when the cache already has enough entries.
- **Components:** `__ut_block_size_prealloc`
- **Notes:** No double-allocation.

## TEST_CASE: "__wti_block_size_alloc: cache hit" [block_session_size]
- **What it tests:** When the sz_cache is populated, `__wti_block_size_alloc` returns a cached entry without a heap call.
- **Components:** `__wti_block_size_alloc`, `WT_BLOCK_MGR_SESSION`
- **Notes:** Cache count decrements by 1.

## TEST_CASE: "__wti_block_size_alloc: cache miss" [block_session_size]
- **What it tests:** Heap allocation is used when the sz_cache is empty.
- **Components:** `__wti_block_size_alloc`
- **Notes:** Heap allocation path for size structs.

## TEST_CASE: "__wti_block_size_free" [block_session_size]
### SECTION: "return to cache"
- **What it tests:** A freed `WT_SIZE` is returned to the cache if space is available.
- **Components:** `__wti_block_size_free`
- **Notes:** Cache count increments.

### SECTION: "free to heap when cache is full"
- **What it tests:** Freed size structs go to the heap when the session cache is at capacity.
- **Components:** `__wti_block_size_free`
- **Notes:** Simulated full cache via fake count.

## TEST_CASE: "__ut_block_size_discard" [block_session_size]
- **What it tests:** All cached `WT_SIZE` entries are freed and the count drops to zero.
- **Components:** `__ut_block_size_discard`
- **Notes:** Session cleanup path.
