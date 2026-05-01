# test_rollback_to_stable20 — RTS does not open dhandles without unstable updates

**File:** `test/suite/test_rollback_to_stable20.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, dhandle management, crash recovery

## Test Cases

### `test_rollback_to_stable20.test_rollback_to_stable`
- **What it tests:** Verifies that RTS does not unnecessarily open data handles (dhandles) for tables that have no unstable updates. Creates 100 tables each with 10,000 rows updated at ts=10. Sets stable=10. Checkpoint and crash-restart. Post-restart stat `dh_conn_handle_count < 5`, confirming RTS did not open handles for the 100 tables that had no work to do.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/conn/`, `src/dhandle/`
- **Notes:** Parametrized on key_format (column/row_integer) only. No prepare or worker thread params. `cache_size=50MB`. The key invariant being tested is that opening 100+ dhandles at startup just for RTS inspection is avoided.
