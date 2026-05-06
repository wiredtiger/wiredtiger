# test_rollback_to_stable14 — RTS uses proper base update when restoring modifies from history store

**File:** `test/suite/test_rollback_to_stable14.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, modify operations, crash recovery, concurrent checkpoint

## Test Cases

### `test_rollback_to_stable14.test_rollback_to_stable`
- **What it tests:** Verifies RTS uses the correct base update (not a delta) when restoring a chain of modifies from the history store after crash. Writes value_a@20, then modifies Q@30, R@40, S@50, T@60 (stable=50/60). Background checkpoint runs while modifies W@70, X@80, Y@90, Z@100 are written concurrently with eviction. Crash-restart: checks value_a at ts=20, modQ at ts=30/40/50. Stats: `hs_restore_updates == nrows`, `hs_removed + hs_sweep >= nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/modify.c`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer) and prepare. 100 rows. Uses `timing_stress_for_test=[history_store_checkpoint_delay]`. Background checkpoint waits for `checkpoint_state != 0`.

### `test_rollback_to_stable14.test_rollback_to_stable_same_ts`
- **What it tests:** Same modify chain restoration test but with modifies R, S, T all at the same timestamp ts=60 (non-prepare) or ts=51/55/60 (prepare to avoid same-ts constraint). Stable set to ts=50. Otherwise identical to `test_rollback_to_stable`. Stats: `hs_removed + hs_sweep >= nrows*3`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/modify.c`
- **Notes:** Tests HS ordering when multiple modifies share a timestamp. Prepare mode uses distinct timestamps to avoid prepare-timestamp constraints.

### `test_rollback_to_stable14.test_rollback_to_stable_same_ts_append`
- **What it tests:** Same as `test_rollback_to_stable_same_ts` but uses appending modifies (each modify appends a character to the end of the string) rather than in-place modifications. Stable=50. Post-crash: checks value_a at ts=20, value_modQ at ts=30.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/modify.c`
- **Notes:** Tests that append-type modifies (growing value) are handled correctly during HS restore.
