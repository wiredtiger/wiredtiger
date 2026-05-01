# test_checkpoint35 — Precise checkpoint with unstable writes + crash restart; stable data survives

**File:** `test/suite/test_checkpoint35.py`
**Storage mode:** General
**Components under test:** precise checkpoint, rollback to stable, crash recovery

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a precise checkpoint captures only data at or below `stable_timestamp`, and that after a simulated crash restart, the unstable writes (above stable_ts) are rolled back by RTS while the stable data survives intact.
- **Components:** `src/checkpoint/checkpoint.c`, `src/txn/txn_rollback_to_stable.c`
- **Notes:** Writes are made at two timestamps: one at or below stable_ts (stable) and one above (unstable). A precise checkpoint is taken and then crash-restarted. After recovery, reads confirm only the stable value is present. Tests that `checkpoint=(precise=true)` combined with RTS correctly restores the database to the stable state.
