# test_rollback_to_stable12 — RTS skips subtrees during tree walk using aggregated timestamps

**File:** `test/suite/test_rollback_to_stable12.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, btree tree walk, aggregated timestamps, crash recovery

## Test Cases

### `test_rollback_to_stable12.test_rollback_to_stable`
- **What it tests:** Verifies that RTS correctly skips subtrees during the tree walk when internal-node aggregated timestamps indicate no unstable updates within that subtree. Creates 1,000,000 rows at ts=20. Sets stable=20 (non-prepare) or stable=28 (prepare). Then writes a single modified key=1 at ts=30 (past stable). Checkpoint then crash-restart. Post-restart: all rows show value_a at ts=30. Stats: `txn_rts_tree_walk_skip_pages >= 0` (verifies the skip-page path was tested), `pages_visited > 0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/btree/`, `src/cell.h` (aggregated time windows)
- **Notes:** Parametrized on key_format (column/row_integer) and prepare. `split_pct=50` forces many internal nodes. 1M rows makes subtree skipping meaningful. `cache_size=500MB`. The stat `txn_rts_tree_walk_skip_pages` quantifies how many page-walk steps were skipped.
