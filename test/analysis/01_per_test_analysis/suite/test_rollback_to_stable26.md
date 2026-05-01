# test_rollback_to_stable26 — RTS restores prepare rollback entry from history store

**File:** `test/suite/test_rollback_to_stable26.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, prepared transactions, history store, concurrent checkpoint

## Test Cases

### `test_rollback_to_stable26.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly restores the HS state after an active prepared transaction (that subsequently rolls back) is concurrent with a background checkpoint. Writes value_a@20, value_b@30, optionally removes@40 (`hs_remove=True`). Opens a prepared txn writing value_c@50 (optionally also removing — `prepare_remove=True`). Evicts pages. Sets stable=40. Background checkpoint runs, then prepared txn is rolled back. Writes value_d@60. Crash-restart. Post-restart: value_b visible at ts=30, value_a at ts=20. Stats: `hs_restore_updates == nrows`, `hs_removed == nrows`. Then writes value_e@70 and re-evicts to confirm DB integrity.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), hs_remove (true/false), prepare (true/false), prepare_remove (true/false). 10 rows. `cache_size=10MB`, `timing_stress_for_test=[history_store_checkpoint_delay]`. Background checkpoint polls `checkpoint_state` stat.
