# test_timestamp22 — Timestamp API randomized fuzz test

**File:** `test/suite/test_timestamp22.py`
**Storage mode:** General
**Components under test:** all timestamp APIs, prepared transactions, global timestamp ordering

## Test Cases

### `test_timestamp22.test_timestamp_randomizer`
- **What it tests:** Over 1000 (or 100,000 in long-test mode) iterations, randomly mixes: transactions with/without timestamps and read_timestamps; calls to `timestamp_transaction` before and after writes; prepare+commit sequences; `set_timestamp` calls for oldest/stable/durable in various combinations. For each operation, predicts whether success or failure is expected based on current global timestamp state and asserts the outcome matches. Verifies at the end that all successfully-committed data is readable.
- **Components:** `txn.c`, `txn_timestamp.c`, `txn_prepare.c`
- **Notes:** Parameterized over integer-row and column formats. Tests do not crash on any combination of valid/invalid timestamp API usage. Seeds are printed for reproducibility. Constraints enforced: OOO commit not allowed, commit > oldest/stable, prepare > stable, durable >= commit.
