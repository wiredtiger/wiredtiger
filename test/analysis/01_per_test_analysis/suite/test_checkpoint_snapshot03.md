# test_checkpoint_snapshot03 — RTS skips unnecessary pages; txn_rts_tree_walk_skip_pages > 0

**File:** `test/suite/test_checkpoint_snapshot03.py`
**Storage mode:** General
**Components under test:** rollback to stable, tree walk optimization, statistics

## Test Cases

### `test_checkpoint_snapshot03.test_checkpoint_snapshot03`
- **What it tests:** Verifies that RTS skips pages that do not need rollback (pages whose aggregate time window is entirely within the stable timestamp) and that `txn_rts_tree_walk_skip_pages` is greater than zero after recovery.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/btree/bt_walk.c`
- **Notes:** Creates a table with data at multiple timestamps, takes a checkpoint, then modifies data above stable_ts and crash-restarts. After recovery, reads `stat.conn.txn_rts_tree_walk_skip_pages` and asserts it is positive. Tests the page-skip optimization in RTS that avoids reading and processing pages that are already in a stable state.
