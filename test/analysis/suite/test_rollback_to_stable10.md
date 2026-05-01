# test_rollback_to_stable10 — RTS sweeps history store with concurrent checkpoint

**File:** `test/suite/test_rollback_to_stable10.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, checkpoint, concurrency, prepared transactions

## Test Cases

### `test_rollback_to_stable10.test_rollback_to_stable`
- **What it tests:** Verifies that RTS correctly sweeps the history store when updates are written concurrently with a background checkpoint thread. Two tables each receive 4 updates (ts=20/30/40/50). Stable set to 50 (non-prepare) or 60 (prepare). A background checkpoint thread runs while value_e@70 and value_f@80 are written with intermediate eviction. Crash-restart is used; post-restart, value_a is expected at ts=50/80, value_b at ts=40, value_c at ts=30, value_d at ts=20. Stats: `hs_removed + hs_sweep > 0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`, `src/evict/`
- **Notes:** Uses `timing_stress_for_test=[history_store_checkpoint_delay]` to create timing overlap. Two tables: `rollback_to_stable10_1` and `rollback_to_stable10_2`. Parametrized on key_format and prepare. The balance between `hs_removed` and `hs_sweep` depends on timing; test only checks their sum is >0.

### `test_rollback_to_stable10.test_rollback_to_stable_prepare`
- **What it tests:** Verifies RTS correctly handles active prepared transactions concurrent with a checkpoint. Same two-table setup. An explicit first checkpoint is done, then a background checkpointer runs while two sessions prepare (but do not commit) value_e@69/70. The DB is crash-copied before commit; then committed in original dir; the restart dir is opened. Post-restart, both tables show value_a at ts=50/80, value_b at ts=40, value_c at ts=30, value_d at ts=20. Stats: `hs_removed + hs_sweep > 0` and `cache_hs_ondisk > 0` before and after restart.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Uses `copy_wiredtiger_home` (not simulate_crash_restart) so the prepared txns can be committed in the original dir before opening the crash copy. Background checkpoint waits for checkpoint to start (polls `checkpoint_state` stat). `prepare_extraconfig` is empty string (overridable by subclasses).
