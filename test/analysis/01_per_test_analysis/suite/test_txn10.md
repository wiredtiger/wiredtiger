# test_txn10 — Recovery: file ID allocation correctness across table creates

**File:** `test/suite/test_txn10.py`
**Storage mode:** General
**Components under test:** file ID allocation, log recovery, `simulate_crash_restart`

## Test Cases

### `test_txn10.test_recovery`
- **What it tests:** Creates table t1 and does a clean restart; creates table t2 and inserts 10,000 records; simulates a crash restart (forcing recovery); verifies t2 contains all 10,000 records in correct order and t1 is empty. Regression test ensuring log recovery applies records to the correct table when file IDs are allocated across restarts.
- **Components:** `log.c`, `recovery.c`, `meta.c`
- **Notes:** No parameterization. Tests the specific bug where file IDs allocated in different sessions/restarts could cause log records for t2 to be applied to t1 during recovery.
