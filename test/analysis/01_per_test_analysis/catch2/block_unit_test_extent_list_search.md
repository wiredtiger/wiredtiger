# test_extent_list_search — Extent list pair search and diagnostic match tests

**File:** `test/catch2/block/unit/test_extent_list_search.cpp`
**Storage mode:** General
**Components under test:** `__ut_block_off_srch_pair`, `__ut_block_off_match` (diagnostic only)
**Test type:** Unit

## TEST_CASE: "__ut_block_off_srch_pair" [extent_list_search]
### SECTION: "find matching pair"
- **What it tests:** Given a target offset and size, `__ut_block_off_srch_pair` finds the exact matching extent in the list.
- **Components:** `__ut_block_off_srch_pair`, `WT_EXTLIST`, `WT_EXT`
- **Notes:** Returns pointers to both the offset-list and size-list positions for the matching extent.

### SECTION: "no match in empty list"
- **What it tests:** Searching an empty extent list returns null for both result pointers.
- **Components:** `__ut_block_off_srch_pair`
- **Notes:** Boundary condition.

### SECTION: "no match for wrong offset"
- **What it tests:** A search for an offset not present in the list returns null.
- **Components:** `__ut_block_off_srch_pair`
- **Notes:** Verifies the function does not return a false-positive match.

## TEST_CASE: "__ut_block_off_match" [extent_list_search] (HAVE_DIAGNOSTIC)
- **What it tests:** Checks whether two extents overlap in the offset-ordered list (diagnostic assertion helper).
- **Components:** `__ut_block_off_match`, `WT_EXT`
- **Notes:** Only compiled when `HAVE_DIAGNOSTIC` is defined. Detects overlapping extents that would indicate a corruption.
