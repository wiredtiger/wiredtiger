# test_rollback_to_stable31 — RTS behavior when stable timestamp was never set

**File:** `test/suite/test_rollback_to_stable31.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, crash recovery, stable timestamp, checkpoint

## Test Cases

### `test_rollback_to_stable31.test_rollback_to_stable`
- **What it tests:** Verifies RTS behavior when no stable timestamp has ever been set. Writes value_a@10, value_b@20, value_c@30. Optionally checkpoints. Then either crashes (recovery RTS) or calls runtime RTS. Results: (1) runtime RTS: does nothing — all data remains. (2) crash without checkpoint: all data disappears (no checkpoint means nothing to recover). (3) crash with checkpoint: recovery RTS does nothing when no stable timestamp is set — all data from checkpoint is visible.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/checkpoint/`, `src/log/`
- **Notes:** Parametrized on key_format (column/row_integer), checkpoint (true/false), crash/runtime (true/false), worker threads (0/4/8). 10 rows. Does not set oldest_timestamp either (cannot be later than stable). Key finding: runtime `rollback_to_stable()` is a no-op without stable set; recovery RTS is a no-op with a checkpoint but no stable set.
