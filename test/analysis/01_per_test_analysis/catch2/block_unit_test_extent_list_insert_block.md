# test_extent_list_insert_block — Extent list insert/merge/remove/append/extend with WT_BLOCK

**File:** `test/catch2/block/unit/test_extent_list_insert_block.cpp`
**Storage mode:** General
**Components under test:** `__ut_block_merge`, `__ut_block_off_remove`, `__ut_block_append`, `__ut_block_extend`
**Test type:** Unit

## TEST_CASE: "__ut_block_merge" [extent_list_insert_block]
### SECTION: "basic merge"
- **What it tests:** Merging two adjacent extents into the avail list produces a single coalesced entry.
- **Components:** `__ut_block_merge`, `WT_EXTLIST`, `WT_BLOCK`
- **Notes:** Uses the BREAK macro for test body separation. Checks that adjacent extents are combined.

## TEST_CASE: "__ut_block_off_remove" [extent_list_insert_block]
### SECTION: "remove existing entry"
- **What it tests:** Removes a specific (offset, size) entry from the extent list; the list shrinks by one entry.
- **Components:** `__ut_block_off_remove`, `WT_EXTLIST`
- **Notes:** Verifies the exact removed entry is gone and remaining entries are intact.

## TEST_CASE: "__ut_block_append" [extent_list_insert_block]
### SECTION: "append block at end of file"
- **What it tests:** `__ut_block_append` adds a new extent at the end of the current file, extending the file size.
- **Components:** `__ut_block_append`, `WT_BLOCK`, `WT_EXTLIST`
- **Notes:** The block's file size (`fh->size`) increases by the appended extent size.

## TEST_CASE: "__ut_block_extend" [extent_list_insert_block]
### SECTION: "extend file"
- **What it tests:** `__ut_block_extend` grows the extent list's tracked file size to accommodate a larger requested region.
- **Components:** `__ut_block_extend`, `WT_BLOCK`
- **Notes:** The on-disk file is not actually grown; only the metadata is updated during the test.
