# test_prepare02 — Session operations forbidden after prepare_transaction

**File:** `test/suite/test_prepare02.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** prepared transactions, session API restrictions

## Test Cases

### `test_prepare02.test_prepare_session_operations`
- **What it tests:** Exhaustively verifies that all session methods prohibited after `prepare_transaction()` return the correct error; also confirms that `commit_transaction()`, `rollback_transaction()`, and `session.close()` are permitted after prepare
- **Components:** `txn/txn_prepare.c`, `session/session_api.c`
- **Notes:** Forbidden operations tested: `open_cursor`, `alter`, `create`, `compact`, `drop`, `log_flush`, `reset`, `salvage`, `truncate`, `verify`, `begin_transaction`, `prepare_transaction`, `checkpoint`; each is expected to raise a `WiredTigerError` with a message about the operation being forbidden after prepare; skipped for tiered storage hook
