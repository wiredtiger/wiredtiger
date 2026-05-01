# test_prepare05 — Timestamp ordering constraints for prepare_transaction

**File:** `test/suite/test_prepare05.py`
**Storage mode:** General
**Components under test:** prepared transactions, timestamp API validation

## Test Cases

### `test_prepare05.test_timestamp_api`
- **What it tests:** Verifies timestamp ordering constraints enforced by WiredTiger: `prepare_timestamp` must be strictly newer than `stable_timestamp`; `commit_timestamp` must not be set before `prepare_transaction()`; `commit_timestamp` may equal `prepare_timestamp`
- **Components:** `txn/txn_prepare.c`, `txn/txn_timestamp.c`
- **Notes:** No scenarios; tests that violating the prepare_ts > stable_ts constraint raises an error; tests that calling `set_timestamp` with commit_ts before prepare results in an error; confirms the boundary case where commit_ts == prepare_ts is accepted
