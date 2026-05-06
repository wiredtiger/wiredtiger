# test_rollback_to_stable39 — RTS with parallel checkpoint delay and eviction moving data to history store

**File:** `test/suite/test_rollback_to_stable39.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, checkpoint, eviction, history store, crash recovery

## Test Cases

### `test_rollback_to_stable39.test_rollback_to_stable`
- **What it tests:** Verifies RTS handles the case where eviction moves content from the data store to the history store concurrent with a delayed checkpoint, and then a subsequent crash requires recovery RTS. Writes value_a@20, removes@30. Sets stable=30 (non-prepare) or stable=40 (prepare). Background checkpoint starts while value_b@50 is written and evicted concurrently. Crash-restart (with `checkpoint_slow` timing stress during recovery). Post-restart: value_a visible at ts=20, no rows at ts=40. Stats after restart: all zeros (`hs_removed=0`, `hs_sweep=0`, `upd_aborted=0`, `keys_removed=0`, `keys_restored=0`). Then writes value_c@60, does another checkpoint with parallel eviction.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`, `src/evict/`
- **Notes:** Parametrized on key_format (column/row_integer) and prepare. `cache_size=25MB`. Uses `history_store_checkpoint_delay` stress pre-crash, `checkpoint_slow` stress post-crash (via `restart_config=True`). Background checkpoint polls `checkpoint_state` stat. All-zero RTS stats after restart confirm the recovery RTS found no work to do (all unstable updates already removed by checkpoint timing).
