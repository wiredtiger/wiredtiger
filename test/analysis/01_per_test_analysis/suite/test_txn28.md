# test_txn28 — Transaction snapshot array dump correctness

**File:** `test/suite/test_txn28.py`
**Storage mode:** General
**Components under test:** `conn.debug_info('txn')`, snapshot array output, transaction state dump

## Test Cases

### `test_txn28.test_snapshot_array_dump`
- **What it tests:** Creates 3 concurrent transactions (session1 updates key 5, session2 updates key 6, session3 updates key 7); calls `conn.debug_info('txn')` and reads `stdout.txt`; for each line containing "snapshot count:" parses the count and the number of integers in the "snapshot: [...]" array and asserts they are equal; also asserts that the maximum snapshot list item count equals 2 (since at most 2 transactions are visible in any given session's snapshot at the time of the dump).
- **Components:** `txn.c`, `conn.c`
- **Notes:** No parameterization. Tests that the transaction state dump output correctly formats the snapshot array with the right count.
