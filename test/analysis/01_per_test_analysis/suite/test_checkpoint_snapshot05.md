# test_checkpoint_snapshot05 — Bulk load + update concurrent with checkpoint; backup recovery

**File:** `test/suite/test_checkpoint_snapshot05.py`
**Storage mode:** General
**Components under test:** checkpoint snapshot, bulk load, backup, crash recovery

## Test Cases

### `test_checkpoint_snapshot05.test_checkpoint_snapshot05`
- **What it tests:** Verifies that a checkpoint taken while both a bulk-load operation and an update transaction are in progress produces a consistent backup, and that recovering from the backup yields the correct stable state.
- **Components:** `src/cursor/cur_bulk.c`, `src/checkpoint/`, `src/backup/`, `src/txn/txn_rollback_to_stable.c`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_slow]` to ensure concurrency overlap. A bulk-load cursor and a regular update transaction run concurrently with checkpoint. The resulting backup is recovered and data is verified. Tests that bulk-load concurrency with checkpoint does not corrupt the checkpoint snapshot.
