# test_txn21 — Smoke test for operation_timeout_ms API

**File:** `test/suite/test_txn21.py`
**Storage mode:** General
**Components under test:** `operation_timeout_ms` transaction configuration

## Test Cases

### `test_txn21.test_operation_timeout_txn`
- **What it tests:** Verifies that `operation_timeout_ms=2000` can be passed to `begin_transaction`, `rollback_transaction`, and `commit_transaction` without raising an error.
- **Components:** `txn.c`, `session.c`
- **Notes:** No parameterization. Smoke test only — does not actually test that operations time out, only that the API accepts the configuration option correctly.
