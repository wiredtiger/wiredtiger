# test_rollback_to_stable04 — RTS replaces on-disk value with full update from history store after mix of modifies

**File:** `test/suite/test_rollback_to_stable04.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, modify operations, checkpoint, eviction

## Test Cases

### `test_rollback_to_stable04.test_rollback_to_stable`
- **What it tests:** Verifies that RTS always restores the on-disk value to a full update from the history store (not a delta), even when the history contains a mix of full updates and modifies. Writes value_a at ts=20, then applies modifies Q@30, R@40, S@50, then more updates and modifies up to ts=140 (11 versions total). Sets stable=30 (non-prepare) or stable=40 (prepare). After checkpoint and RTS, verifies value_modQ is seen at ts=150 (non-dryrun); in dryrun mode value_modZ still visible. Stats verify `upd_aborted + hs_removed + hs_sweep >= nrows*11` (non-dryrun, non-memory).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/modify.c`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, prepare, dryrun, evict (optional mid-test eviction after first 3 modifies), worker threads (0/4/8). Oldest and stable initially pinned to ts=10. Also checks `hs_sweep` and `hs_sweep_dryrun` stats. `cache_size=500MB`.
