# test_block_session_ext — Block session extent allocation/free/discard tests

**File:** `test/catch2/block/unit/test_block_session_ext.cpp`
**Storage mode:** General
**Components under test:** `__ut_block_ext_alloc`, `__ut_block_ext_prealloc`, `__wti_block_ext_alloc`, `__wti_block_ext_free`, `__ut_block_ext_discard`
**Test type:** Unit

## TEST_CASE: "__ut_block_ext_alloc: null BM session" [block_session_ext]
- **What it tests:** When the block manager session pointer is null, `__ut_block_ext_alloc` allocates from the heap directly.
- **Components:** `__ut_block_ext_alloc`
- **Notes:** Fallback path when no cache is available.

## TEST_CASE: "__ut_block_ext_prealloc" [block_session_ext]
### SECTION: "fill cache to requested count"
- **What it tests:** After calling `__ut_block_ext_prealloc(n)`, the ext cache contains exactly n entries.
- **Components:** `__ut_block_ext_prealloc`
- **Notes:** Verifies count tracking in `ext_cache`.

### SECTION: "idempotent when cache is full"
- **What it tests:** Calling prealloc when the cache already has enough entries is a no-op.
- **Components:** `__ut_block_ext_prealloc`
- **Notes:** Prevents over-allocation.

## TEST_CASE: "__wti_block_ext_alloc: cache hit" [block_session_ext]
- **What it tests:** When the session cache has pre-allocated extents, `__wti_block_ext_alloc` returns one without a heap allocation.
- **Components:** `__wti_block_ext_alloc`, `WT_BLOCK_MGR_SESSION`
- **Notes:** Cache count decrements by 1 after the alloc.

## TEST_CASE: "__wti_block_ext_alloc: cache miss" [block_session_ext]
- **What it tests:** When the cache is empty, `__wti_block_ext_alloc` allocates from the heap.
- **Components:** `__wti_block_ext_alloc`
- **Notes:** Heap allocation path.

## TEST_CASE: "__wti_block_ext_free" [block_session_ext]
### SECTION: "return to cache"
- **What it tests:** Freeing an extent with available cache space returns it to the session cache.
- **Components:** `__wti_block_ext_free`
- **Notes:** Cache count increments.

### SECTION: "free to heap when cache is full"
- **What it tests:** When the session cache is at capacity, freed extents are returned to the heap.
- **Components:** `__wti_block_ext_free`
- **Notes:** Fake high cache count used to simulate full cache.

## TEST_CASE: "__ut_block_ext_discard" [block_session_ext]
- **What it tests:** Discarding all cache entries frees them from the session and reduces the count to zero.
- **Components:** `__ut_block_ext_discard`
- **Notes:** Called during session cleanup; final cache count must be 0.
