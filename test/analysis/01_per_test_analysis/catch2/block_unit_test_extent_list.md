# test_extent_list — Extent list skip-list search function tests

**File:** `test/catch2/block/unit/test_extent_list.cpp`
**Storage mode:** General
**Components under test:** `__ut_block_off_srch_last`, `__ut_block_off_srch`, `__ut_block_first_srch`, `__ut_block_size_srch`
**Test type:** Unit

## TEST_CASE: "__ut_block_off_srch_last" [extent_list]
- **What it tests:** Finds the last entry in the skip list that has an offset less than a given target offset.
- **Components:** `__ut_block_off_srch_last`, `WT_EXTLIST`, `WT_EXT`
- **Notes:** Uses `ExtentListWrapper` helper to build skip lists. Verifies that the returned pointer is the expected predecessor.

## TEST_CASE: "__ut_block_off_srch" [extent_list]
- **What it tests:** Searches for the insert position for a given offset in the skip list.
- **Components:** `__ut_block_off_srch`, `WT_EXTLIST`, `WT_EXT`
- **Notes:** Returns the stack of skip-list pointers for insertion; the search does not insert an element.

## TEST_CASE: "__ut_block_first_srch" [extent_list]
- **What it tests:** Searches the extent list for the first entry that is large enough to satisfy a given size request.
- **Components:** `__ut_block_first_srch`, `WT_EXTLIST`
- **Notes:** Implements first-fit allocation search. Verifies that the returned extent is >= the requested size.

## TEST_CASE: "__ut_block_size_srch" [extent_list]
- **What it tests:** Searches the size-ordered skip list for the insert position for a given size.
- **Components:** `__ut_block_size_srch`, `WT_EXTLIST`, `WT_SIZE`, `SizeListWrapper`
- **Notes:** Returns the insertion stack for the size-ordered skip list, used in best-fit allocation.
