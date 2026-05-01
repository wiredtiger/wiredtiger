# test_block_session_bms — Block manager session combined ext+size prealloc and cleanup tests

**File:** `test/catch2/block/unit/test_block_session_bms.cpp`
**Storage mode:** General
**Components under test:** `__wti_block_ext_prealloc` (combined ext+size prealloc), `__ut_block_manager_session_cleanup`
**Test type:** Unit

## TEST_CASE: "Prealloc combined ext and size cache" [block_session_bms]
### SECTION: "basic prealloc"
- **What it tests:** `__wti_block_ext_prealloc` populates both the ext and size caches to the requested count.
- **Components:** `__wti_block_ext_prealloc`, `WT_BLOCK_MGR_SESSION`
- **Notes:** After prealloc, `ext_cache.bytes` and `sz_cache.bytes` reflect the allocated count.

### SECTION: "prealloc with existing cache entries"
- **What it tests:** Prealloc correctly adds to an already-populated cache without duplication.
- **Components:** `__wti_block_ext_prealloc`
- **Notes:** Exercises the loop that skips existing entries.

## TEST_CASE: "Block manager session cleanup" [block_session_bms]
### SECTION: "cleanup after prealloc"
- **What it tests:** `__ut_block_manager_session_cleanup` frees all ext and size cache entries, leaving counts at zero.
- **Components:** `__ut_block_manager_session_cleanup`
- **Notes:** Verifies memory is released; runs after a prior prealloc fills the caches.

### SECTION: "cleanup on empty session"
- **What it tests:** Cleanup on a freshly-initialized `WT_BLOCK_MGR_SESSION` returns 0 without crashing.
- **Components:** `__ut_block_manager_session_cleanup`
- **Notes:** No-op path when caches are already empty.

## TEST_CASE: "Prealloc error path: fake cache count" [block_session_bms]
- **What it tests:** When the number of existing cache entries exceeds the requested count, prealloc does not over-allocate and may return an error.
- **Components:** `__wti_block_ext_prealloc`
- **Notes:** Exercises the boundary where `ext_cache.bytes >= count`; validates early-exit behavior.
