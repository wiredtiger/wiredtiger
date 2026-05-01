# test_rollback_to_stable33 — RTS smoke test on in-memory databases with logged and non-logged tables

**File:** `test/suite/test_rollback_to_stable33.py`
**Storage mode:** In-memory
**Components under test:** rollback_to_stable, in-memory databases, logging

## Test Cases

### `test_rollback_to_stable33.test_rollback_to_stable33`
- **What it tests:** Smoke test verifying RTS works correctly on in-memory databases. Creates a 500-row table that is either logged or non-logged. Makes changes at ts=30. Sets stable=20 and calls RTS. Logged tables: changes survive (log-based recovery semantics apply even in-memory). Non-logged tables: changes are rolled back (RTS restores to prior values). Verifies 3 specific keys have either new or original values depending on `logged`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/log/`
- **Notes:** Parametrized on key_format (row_integer/var=column), logged (true/false), worker threads (0/4/8). Always `in_memory=true`. Uses RTS verifier as teardown. Key distinction: logged in-memory tables are not subject to RTS (log takes precedence).
