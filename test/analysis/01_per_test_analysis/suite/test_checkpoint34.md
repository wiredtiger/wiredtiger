# test_checkpoint34 — Precise checkpoint + fast truncate + crash restart; RTS aborts = 0

**File:** `test/suite/test_checkpoint34.py`
**Storage mode:** General
**Components under test:** precise checkpoint, fast delete, rollback to stable, crash recovery

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that after a precise checkpoint captures a fast-truncated range, a simulated crash restart runs RTS with zero aborts (`txn_rts_keys_removed == 0` and `txn_rts_upd_aborted == 0`). Precise checkpoints only capture stable data, so no rollback is needed.
- **Components:** `src/checkpoint/checkpoint.c`, `src/btree/bt_delete.c`, `src/txn/txn_rollback_to_stable.c`
- **Notes:** Truncation is committed at `stable_timestamp` or below; precise checkpoint is taken. After `simulate_crash_restart`, RTS runs but finds nothing to roll back since all changes were stable. Tests the invariant that precise checkpoints never produce work for RTS. Uses `checkpoint=(precise=true)` configuration.
