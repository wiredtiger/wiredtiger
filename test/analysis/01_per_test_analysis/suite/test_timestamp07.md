# test_timestamp07 — Timestamp checkpoints with column groups and backup recovery

**File:** `test/suite/test_timestamp07.py`
**Storage mode:** General
**Components under test:** timestamps, checkpoints, column groups, backup recovery, log_flush

## Test Cases

### `test_timestamp07.test_timestamp07`
- **What it tests:** Opens a non-logged timestamped table and a logged non-timestamped table; inserts nkeys at timestamp=key; verifies point-in-time reads; advances oldest/stable; updates with value2 at timestamp=key+nkeys; checkpoints at stable (confirms value2 absent in non-logged backup, present in logged); advances stable to 2×nkeys; confirms value2 in both; updates with value3 at timestamp=key+2×nkeys; calls `log_flush` without checkpoint; takes backup and verifies: non-logged table has no value3 (not checkpointed), logged table has value3 (from log). Also verifies reads at stable timestamp.
- **Components:** `txn_timestamp.c`, `checkpoint.c`, `log.c`, `backup.c`, `col_group.c`
- **Notes:** Parameterized over 2 key formats × 2 URI types (file, table-cg) × 1 log config × 3 nkeys variants (100, 500, 1000). Column group variant tests colgroup table structure.
