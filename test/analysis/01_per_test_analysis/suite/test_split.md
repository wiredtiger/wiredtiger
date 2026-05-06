# test_split — Page splits in BTree storage

**File:** `test/suite/test_split.py`
**Storage mode:** General
**Components under test:** btree, page splits, internal pages, leaf pages

## Test Cases

### `test_split.test_split_simple`
- **What it tests:** Verifies that page splits occur correctly when inserting enough data to exceed the leaf page size. Creates a table with `leaf_page_max=4KB` and inserts rows until the page splits. Uses the `btree_row_leaf` stat to confirm that at least 2 leaf pages exist after the split. Verifies data integrity (all rows readable) after the split.
- **Components:** `src/btree/bt_split.c`, `src/reconcile/`, `src/btree/`
- **Notes:** Uses `page_size=4096` (4KB) to force splits at a small number of records. Checks `stat.dsrc.btree_row_leaf` on the table-level statistics cursor to verify split occurred. No parameterization — single scenario.
