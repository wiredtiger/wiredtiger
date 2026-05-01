# test_rollback_to_stable30 — RTS fails with active cursors/transactions, succeeds after resolution

**File:** `test/suite/test_rollback_to_stable30.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, prepared transactions, error handling, API validation

## Test Cases

### `test_rollback_to_stable30.test_rts_prepare_commit`
- **What it tests:** Verifies that RTS fails with appropriate error messages when called while a cursor is positioned or a transaction is active, and that it succeeds after the prepared transaction is committed. First RTS attempt with positioned cursor raises `/rollback_to_stable illegal with active file cursors/`. Second attempt with active prepared txn raises `/rollback_to_stable illegal with active transactions/`. After commit, RTS succeeds.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`
- **Notes:** Parametrized on table format (table-r, table-S, table-r-complex, table-S-complex) and worker threads (0/4/8). Both simple and complex datasets tested. Uses `expectedStdoutPattern` to verify diagnostic messages. RTS verifier as teardown.

### `test_rollback_to_stable30.test_rts_prepare_rollback`
- **What it tests:** Same as `test_rts_prepare_commit` but the prepared transaction is rolled back (not committed) before the final successful RTS call.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/txn/txn_prepare.c`
- **Notes:** Shares `prepare_resolve()` helper with `test_rts_prepare_commit`. Stable timestamp pinned to 1 throughout; the prepare txn was at ts=10/20 (prepare/durable).
