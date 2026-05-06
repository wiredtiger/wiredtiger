# test_txn29 — Logged transaction cannot be rolled back after commit; out-of-order timestamp rejected

**File:** `test/suite/test_txn29.py`
**Storage mode:** General
**Components under test:** logging, crash recovery, out-of-order commit timestamp, `simulate_crash_restart`

## Test Cases

### `test_txn29.test_transaction_logging`
- **What it tests:** Requires a non-diagnostic build. Creates a logged file and a non-logged file; commits a transaction to both at ts=20 with `sync=on`; begins a second transaction writing to both and attempts to commit at ts=10 (out-of-order) which raises `WiredTigerError`; simulates crash restart; verifies: in the non-logged file key 1 is `WT_NOTFOUND` (non-logged data not recovered), in the logged file key 1 is "aaaa" (first commit survived, second aborted commit not present).
- **Components:** `log.c`, `txn_timestamp.c`, `recovery.c`
- **Notes:** No parameterization. Tests two things: (1) that an out-of-order timestamp on commit causes the transaction to be aborted, and (2) that crash recovery correctly recovers logged data while the failed commit is not persisted.
