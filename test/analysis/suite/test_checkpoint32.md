# test_checkpoint32 — cursor_tree_walk_inmem_del_page_skip stat for deleted pages

**File:** `test/suite/test_checkpoint32.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, fast delete, tree walk optimization, statistics

## Test Cases

### `test_checkpoint32.test_checkpoint32`
- **What it tests:** Verifies that the `cursor_tree_walk_inmem_del_page_skip` statistic is incremented when a checkpoint cursor traverses and skips in-memory fast-deleted pages during a tree walk.
- **Components:** `src/btree/bt_walk.c`, `src/btree/bt_delete.c`, `src/cursor/cur_btree.c`
- **Notes:** Truncates a range of keys (creating in-memory `WT_REF_DELETED` pages), then checkpoints and opens a checkpoint cursor that iterates across the deleted range. Asserts `stat.conn.cursor_tree_walk_inmem_del_page_skip > 0`. Tests that the tree-walk optimization correctly counts skipped deleted pages.
