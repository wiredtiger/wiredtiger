# test_prepare18 — prepare_transaction rejected for logged tables

**File:** `test/suite/test_prepare18.py`
**Storage mode:** General
**Components under test:** prepared transactions, write-ahead logging, API validation

## Test Cases

### `test_prepare18.test_prepare18`
- **What it tests:** Verifies that calling `prepare_transaction()` on a session that has modified a logged table returns an error stating "a prepared transaction cannot include a logged table"
- **Components:** `txn/txn_prepare.c`, `log/log.c`
- **Notes:** `conn_config = 'log=(enabled)'`; creates a table with logging enabled (the default when `log=(enabled)` is set at the connection level); inserts a value without disabling logging on the table; then attempts `prepare_transaction()`; the expected error message matches `/a prepared transaction cannot include a logged table/`; no scenarios
