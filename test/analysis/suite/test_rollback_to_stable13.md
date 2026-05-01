# test_rollback_to_stable13 — RTS retains/restores tombstones from update list or history store

**File:** `test/suite/test_rollback_to_stable13.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, tombstone restoration, crash recovery, prepared transactions

## Test Cases

### `test_rollback_to_stable13.test_rollback_to_stable`
- **What it tests:** Verifies that after crash-restart, RTS restores tombstones from HS when a remove (ts=30) was written between two stable updates. Inserts value_a@20, removes@30, updates value_b@60. Sets stable=40 (non-prepare) or stable=50 (prepare). Post-crash: verifies 0 rows at ts=50, value_a visible at ts=20. Stat `txn_rts_hs_restore_tombstones == nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/log/`, `src/checkpoint/`
- **Notes:** Uses `@wttest.prevent(["timestamp"])`. Parametrized on key_format, prepare, dryrun, worker threads (0/4/8). `split_pct=50`.

### `test_rollback_to_stable13.test_rollback_to_stable_with_aborted_updates`
- **What it tests:** Same tombstone restoration scenario but with additional aborted (rolled-back) updates interspersed between stable insert and the remove. Two rounds of value_b and value_c updates are fully rolled back. After crash-restart: still expects tombstone at ts=50 and value_a at ts=20. `txn_rts_hs_restore_tombstones == nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`
- **Notes:** Tests that aborted in-memory updates do not interfere with HS tombstone restoration.

### `test_rollback_to_stable13.test_rollback_to_stable_with_history_tombstone`
- **What it tests:** Verifies tombstone restoration when tombstone exists in the history store (not just update list). Inserts value_a@20, removes@30, then in a single txn inserts+removes value_b@40 (tombstone goes into HS). Sets stable=40 (or 50). Writes value_c@60 and takes two checkpoints. Crash-restart: expects 0 rows at ts=50, value_a at ts=20. `txn_rts_hs_restore_tombstones == nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`
- **Notes:** The same-txn update+remove pattern pushes a delete/tombstone into HS during eviction.

### `test_rollback_to_stable13.test_rollback_to_stable_with_stable_remove`
- **What it tests:** Verifies tombstone behavior when the remove itself is at the stable timestamp. Inserts value_a@20, value_b@30, removes@40. Sets stable=40 (or 50). Writes value_c@60 and checkpoints. Calls runtime RTS (with optional dryrun). Then writes value_c again and checkpoints. Crash-restart: expects 0 rows at ts=50, value_a at ts=20. `txn_rts_hs_restore_tombstones == nrows` (from shutdown/startup RTS, not the runtime call).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`
- **Notes:** Also tests dryrun — the `large_updates` after RTS uses ts=65 in dryrun vs ts=60 in non-dryrun to avoid timestamp ordering violations.
