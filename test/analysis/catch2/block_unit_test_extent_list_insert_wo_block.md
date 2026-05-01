# test_extent_list_insert_wo_block — Extent list insert tests without WT_BLOCK

**File:** `test/catch2/block/unit/test_extent_list_insert_wo_block.cpp`
**Storage mode:** General
**Components under test:** `__ut_block_ext_insert`, `__ut_block_off_insert`
**Test type:** Unit

## TEST_CASE: "__ut_block_ext_insert" [extent_list_insert_wo_block]
### SECTION: "insert into empty list"
- **What it tests:** Inserting the first extent into an empty `WT_EXTLIST` produces a list with exactly one entry at the correct position.
- **Components:** `__ut_block_ext_insert`, `WT_EXTLIST`, `WT_EXT`
- **Notes:** No `WT_BLOCK` is required; the function takes a pre-computed insertion stack.

### SECTION: "insert ordering"
- **What it tests:** Multiple extents inserted in arbitrary order appear in ascending offset order in the skip list.
- **Components:** `__ut_block_ext_insert`
- **Notes:** Skip list ordering is verified by traversal.

## TEST_CASE: "__ut_block_off_insert" [extent_list_insert_wo_block]
### SECTION: "insert single entry"
- **What it tests:** `__ut_block_off_insert` places a new `WT_EXT` at the correct position in the offset-ordered skip list.
- **Components:** `__ut_block_off_insert`, `WT_EXTLIST`
- **Notes:** Distinct from `__ut_block_ext_insert` — operates on the offset skip list directly.

### SECTION: "insert multiple entries"
- **What it tests:** Multiple inserts maintain the sorted offset ordering of the skip list.
- **Components:** `__ut_block_off_insert`
- **Notes:** Verifies integrity of the skip list after several insertions.
