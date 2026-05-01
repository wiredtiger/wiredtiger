# test_checkpoint04 — Checkpoint timing statistics are populated correctly

**File:** `test/suite/test_checkpoint04.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, timing statistics

## Test Cases

### `test_checkpoint04.test_checkpoint04`
- **What it tests:** Verifies that after running a checkpoint, timing-related checkpoint statistics (total time, prepare time, data source flush time) are greater than zero.
- **Components:** `src/checkpoint/checkpoint.c`
- **Notes:** Runs a checkpoint on a populated table and checks `stat.conn.txn_checkpoint_time_total`, `stat.conn.txn_checkpoint_time_prepare`, and `stat.conn.txn_checkpoint_time_data_flush` (or equivalent timing stats). Confirms the checkpoint instrumentation is wired up correctly.
