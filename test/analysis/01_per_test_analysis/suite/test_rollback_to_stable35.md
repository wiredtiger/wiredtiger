# test_rollback_to_stable35 — RTS verifies log is flushed for all checkpoint writes

**File:** `test/suite/test_rollback_to_stable35.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, logging, checkpoint, concurrency, crash recovery

## Test Cases

### `test_rollback_to_stable35.test_rollback_to_stable`
- **What it tests:** Verifies that the WAL is properly flushed for all writes that occurred during a checkpoint. Two tables receive updates: valuea (no-timestamp), then valueb (no-timestamp). Background checkpoint starts. While checkpoint is in progress, writes valuec. Evicts data. Waits for `checkpoint_stop_stress_active` (timing stress that pauses at checkpoint stop), then copies the DB directory. After checkpoint completes, does a final checkpoint and cleans up long-running txn. Opens the copied directory. Post-restart: verifies valuec is visible (confirms checkpoint flush was complete). Stats: `calls=0`, `pages_visited=0`, `hs_removed>=0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/log/`, `src/checkpoint/`
- **Notes:** Skipped for tiered. Parametrized on key_format (column/row_integer). Uses `timing_stress_for_test=[checkpoint_slow, checkpoint_stop]` and `log=(enabled,force_write_wait=60)`. Background checkpoint polls `checkpoint_state`. Uses `copy_wiredtiger_home` (not crash simulate). 10 rows, 2 tables.
