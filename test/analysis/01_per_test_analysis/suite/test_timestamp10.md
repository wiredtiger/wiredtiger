# test_timestamp10 — Recovery timestamp: last_checkpoint and recovery queries

**File:** `test/suite/test_timestamp10.py`
**Storage mode:** General
**Components under test:** `last_checkpoint` timestamp, `recovery` timestamp, checkpoint `use_timestamp`, log-based recovery

## Test Cases

### `test_timestamp10.test_timestamp_recovery`
- **What it tests:** Creates 3 collection-like (non-logged) tables and 1 oplog-like (logged) table; for each collection inserts 10 entries and checkpoints at a distinct stable timestamp; closes the connection with `use_timestamp=true/false/default`; optionally runs the `wt` tool 0, 1, or 2 times; reopens and queries `get=recovery`; verifies recovery timestamp equals last stable timestamp (when `use_timestamp != false`) or 0 (when false). Then scans all tables to verify data completeness.
- **Components:** `txn_timestamp.c`, `checkpoint.c`, `log.c`, `recovery.c`
- **Notes:** Parameterized over 2 key formats × 9 type+wt-runs combinations. Verifies that the recovery timestamp survives repeated `wt -R list` runs.
