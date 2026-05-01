# test_truncate12 — Transaction IDs on fast truncates handled properly after crash recovery

**File:** `test/suite/test_truncate12.py`
**Storage mode:** General
**Components under test:** fast delete, crash recovery, transaction ID wrap, write generation, `simulate_crash_restart`

## Test Cases

### `test_truncate12.test_truncate12`
- **What it tests:** Creates two tables; writes 10,000 rows to table1 at ts=10; reopens; writes 10,000 small updates one-by-one to table2 at ts=20 to cycle through many transaction IDs; truncates table1 (keeping only 5 rows) at ts=30; updates the remaining rows at ts=40; advances stable to 35; checkpoints (writing truncate to disk); simulates crash restart (forcing RTS to roll back the unstable ts=40 update, but not the ts=30 truncate); verifies only the 5 kept rows exist at ts=50 with value_a. Also validates the named checkpoint. Checks that no deleted pages were instantiated during recovery.
- **Components:** `btree.c`, `txn.c`, `rts.c`, `recovery.c`, `checkpoint.c`
- **Notes:** Parameterized over column and integer-row formats. Specifically tests that large transaction IDs from cycling do not corrupt write generations for internal pages loaded during RTS, making truncate transactions invisible.
