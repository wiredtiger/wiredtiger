# test_rollback_to_stable01 — RTS clears remove operations back to stable timestamp

**File:** `test/suite/test_rollback_to_stable01.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, transactions, history store, checkpoint

## Test Cases

### `test_rollback_to_stable01.test_rollback_to_stable`
- **What it tests:** Verifies that RTS clears remove operations that were committed after the stable timestamp. Inserts 10,000 rows at ts=10, then removes all rows at ts=20. Sets stable=10 (non-prepare) or stable=20 (prepare). After checkpoint and RTS, verifies rows are visible again at ts=20 (restored from HS). In dryrun mode, rows remain absent. Checks stats: `calls=1`, `hs_removed=0`, `keys_removed=0`; in non-dryrun `upd_aborted + keys_restored == nrows`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column `r` / row_integer `i`), in_memory (true/false), prepare (true/false), dryrun (true/false), worker threads (0/4/8). In-memory mode skips checkpoints and disables logging. Prepare shifts stable and check timestamps by +10. Dryrun-mode stats use `txn_rts_upd_aborted_dryrun` and `txn_rts_keys_restored_dryrun` instead.
