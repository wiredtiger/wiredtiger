# test_stat13 — Btree maximum depth statistic

**File:** `test/suite/test_stat13.py`
**Storage mode:** General
**Components under test:** `btree_maximum_depth` statistic, btree structure

## Test Cases

### `test_stat13.test_btree_depth`
- **What it tests:** Populates 100 records and checkpoints; verifies `btree_maximum_depth == 2` (root + leaf pages); reopens the connection, performs a search to trigger depth tracking, and confirms `btree_maximum_depth` is still 2.
- **Components:** `stat.c`, `btree`
- **Notes:** Parameterized over column and string-row key formats. The `btree_maximum_depth` stat is updated lazily — a cursor operation must be performed after reopen before the stat reflects the tree depth.
