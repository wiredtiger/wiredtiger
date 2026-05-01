# test_txn03 — Transactions with multiple cursor and session handles

**File:** `test/suite/test_txn03.py`
**Storage mode:** General
**Components under test:** multi-session cursor isolation, snapshot reads, concurrent commits

## Test Cases

### `test_txn03.test_ops`
- **What it tests:** Creates two tables; inserts initial value in both; commits an update to table1 in session1; opens session2 with a transaction; commits an update to table2 in session1 while session2's transaction is still open; reads both tables from session2: table1 shows the committed update (visible before session2's transaction began), table2 shows the original value (update committed after session2's snapshot was taken).
- **Components:** `txn.c`, `cursor.c`
- **Notes:** Parameterized over row (string key) and var (record number key). Tests that snapshot isolation correctly captures a consistent view across multiple tables based on the transaction's start time.
