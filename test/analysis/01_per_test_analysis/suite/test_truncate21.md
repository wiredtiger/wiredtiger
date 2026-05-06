# test_truncate21 — Truncate with logging and recovery when truncate range has no work to do

**File:** `test/suite/test_truncate21.py`
**Storage mode:** General
**Components under test:** truncate, logging, crash recovery, overlapping transactions

## Test Cases

### `test_truncate21.test_truncate21`
- **What it tests:** Creates a row-store table with 1,000 entries; truncates the range [250, 500] in one transaction; then in two concurrent transactions, re-truncates the same range (in session1) while session2 inserts into the middle of the range at key 375 (insert_key); commits the insert first, then the truncate; flushes log; copies the database directory and opens it to force recovery; verifies that the key inserted by session2 is NOT found because the overlapping truncate committed after the insert.
- **Components:** `btree.c`, `log.c`, `recovery.c`, `txn.c`
- **Notes:** Tests that logged truncate operations correctly replay during recovery even when the range has already been truncated (no-op truncate) and when transactions overlap (insert within truncation range).
