# test_txn14 — Log flush: data survives crash when flushed, not when unflushed

**File:** `test/suite/test_txn14.py`
**Storage mode:** General
**Components under test:** `session.log_flush`, crash recovery, `simulate_crash_restart`

## Test Cases

### `test_txn14.test_log_flush`
- **What it tests:** Inserts 10,000 records, calls `log_flush(sync=off)` or `log_flush(sync=on)`; inserts 5 more records and calls `log_flush` again; simulates crash restart; verifies all 10,005 records are present with correct values. Tests that `log_flush` ensures data durability before a simulated crash.
- **Components:** `log.c`, `recovery.c`, `txn.c`
- **Notes:** Parameterized over write/sync × integer-row/column (4 scenarios). `sync=off` flushes to OS, `sync=on` does an fsync. Both should ensure all inserted records survive the crash, because `log_flush` is called before the crash copy.
