# test_rollback_to_stable03 — RTS clears history store updates from reconciled pages

**File:** `test/suite/test_rollback_to_stable03.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, btree reconciliation, checkpoint

## Test Cases

### `test_rollback_to_stable03.test_rollback_to_stable`
- **What it tests:** Verifies that RTS correctly removes history store updates that were written during reconciliation of on-disk pages. Writes 1,000 rows at ts=10 (value_a), ts=20 (value_b), ts=30 (value_c). Sets stable=20 (non-prepare) or stable=30 (prepare). Checkpoints and calls RTS; verifies value_b is visible at ts=20 and value_a at ts=10. Then calls RTS a second time to test `txn_rts_btrees_applied`/`txn_rts_btrees_skipped` stats: in non-memory mode RTS may skip the already-clean btree; in in-memory mode both calls are applied. Verifies `upd_aborted + hs_removed >= nrows` for non-in-memory.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/btree/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, prepare, worker threads (0/4/8). No dryrun parameter. `cache_size=4GB`. Second RTS call verifies that `rts_btrees_skipped + rts_btrees_applied == 2` with `rts_btrees_applied >= 1`.
