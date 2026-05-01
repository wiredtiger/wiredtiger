# test_checkpoint06 — Truncation committed after stable_ts is rolled back after checkpoint

**File:** `test/suite/test_checkpoint06.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, truncation, rollback to stable, timestamps

## Test Cases

### `test_checkpoint06.test_checkpoint06`
- **What it tests:** Verifies that a truncation committed at a timestamp above the current stable timestamp is rolled back (not visible) after a checkpoint is taken and the database is reopened. RTS after restart must remove the unstable truncation.
- **Components:** `src/checkpoint/`, `src/txn/txn_rollback_to_stable.c`, `src/btree/bt_delete.c`
- **Notes:** Inserts rows, checkpoints at stable_ts, then commits a truncation at a timestamp above stable_ts, checkpoints again, and simulates crash restart. After restart, verifies rows that were truncated above stable_ts are restored. Confirms RTS correctly handles fast-delete pages resulting from truncation.
