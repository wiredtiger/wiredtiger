# test_stat10 — Per-table-type btree statistics (row, VLCS)

**File:** `test/suite/test_stat10.py`
**Storage mode:** General
**Components under test:** btree statistics (`btree_row_empty_values`, `btree_column_deleted`, `btree_column_rle`, `btree_overflow`)

## Test Cases

### `test_stat10.test_tree_stats`
- **What it tests:** (Currently skipped via FIXME-WT-16633.) Inserts 100 keys with identical values and 100 with varying values (including overflow and empty values) at timestamp 20; deletes 2 keys at timestamp 30; evicts pages; then verifies format-specific btree stats: `btree_row_empty_values` only for row-store (appears after oldest > 20), `btree_column_deleted` and `btree_column_rle` only for VLCS (deleted count appears after oldest > 30), `btree_overflow` (3 for row-store: 2 overflow keys + 1 value; 1 for VLCS). Also confirms `btree_entries` accounts for deletions when oldest passes timestamp 30.
- **Components:** `stat.c`, `btree`, `col_store.c`, `row_store.c`, `evict.c`
- **Notes:** Parameterized over column/row key formats × oldest timestamps (15, 25, 35) × stable timestamps (15, 25, 35) with `oldest <= stable` filter. Visibility of timestamped changes to btree stats is not strictly specified.
