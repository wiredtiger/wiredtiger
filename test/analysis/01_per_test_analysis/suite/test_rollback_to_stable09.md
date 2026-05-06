# test_rollback_to_stable09 — RTS does not abort schema operations (create/drop table and index)

**File:** `test/suite/test_rollback_to_stable09.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, schema operations, table/index create/drop

## Test Cases

### `test_rollback_to_stable09.test_rollback_to_stable`
- **What it tests:** Verifies that RTS does not abort schema operations (create/drop table and index) since schema operations do not have transaction support. Sets stable=10. Creates table at ts=20, creates index at ts=30; calls RTS — table and index still exist on disk and cursors can be opened. Then drops table at ts=40; calls RTS again — table and index files no longer exist on disk and cursor open raises `WiredTigerError`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/schema/`, `src/meta/`
- **Notes:** Skipped for tiered storage (`@wttest.skip_for_hook("tiered", ...)`). Parametrized on use_columns (column/row key format), in_memory, prepare, worker threads (0/4/8). Column format: `key_format=r,value_format=5sHQ,columns=(id,country,year,population)`; row format: `key_format=5s,value_format=HQ,columns=(country,year,population)`. Index URI: `index:test_rollback_stable09:country`. File existence checked via `os.path.exists` (non-memory only).
