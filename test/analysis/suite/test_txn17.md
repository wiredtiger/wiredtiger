# test_txn17 — API requires_transaction and requires_notransaction error checks

**File:** `test/suite/test_txn17.py`
**Storage mode:** General
**Components under test:** transaction API state validation, error messages

## Test Cases

### `test_txn17.test_txn_api`
- **What it tests:** Verifies that: (1) `timestamp_transaction` outside a transaction raises "only permitted in a running" error; (2) `commit_transaction` outside a transaction raises "only permitted in a running transaction" error; (3) `rollback_transaction` outside a transaction raises "only permitted in a running transaction" error; (4) `begin_transaction` while a transaction is running raises "not permitted in a running transaction" error; (5) `session.checkpoint` while a transaction is running raises "not permitted in a running transaction" error.
- **Components:** `txn.c`, `session.c`, `txn_timestamp.c`
- **Notes:** No parameterization. Tests the API contract enforcement for operations that require or prohibit a running transaction.
