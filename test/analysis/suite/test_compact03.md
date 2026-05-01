# test_compact03 — Compaction blocked by overflow values at end of file

**File:** `test/suite/test_compact03.py`
**Storage mode:** General (skips tiered)
**Components under test:** compaction subsystem, block manager, overflow values

## Test Cases

### `test_compact03.test_compact03`
- **What it tests:** Verifies that compaction cannot shrink the file when large overflow values are at the end of the file (since WT does not rewrite overflow items). After deleting 90% of normal values and compacting, the file remains roughly the same size as before deletion. Also verifies that re-inserting data into freed middle extents does not increase file size.
- **Components:** `src/block/block_compact.c`, `src/btree/bt_ovfl.c`
- **Notes:** Skip: tiered. Two scenarios: `allocation_size=1KB leaf_page_max=1KB` and `4KB/4KB`. Two sub-scenarios: direct delete vs `truncate` to delete middle 90%. Inserts 400 000 normal records (~25 MB), then 5 000 overflow records (5 KB each) at the end. After compaction, asserts file remains within 10% of the with-overflow size. Pages are rewritten (`pages_rewritten > 0`) but file does not shrink. After inserting 50% of the deleted range back, asserts file size unchanged (free extents reused).
