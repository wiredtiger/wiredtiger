# test_drop01 — Drop cleans up history store entries for the dropped table

**File:** `test/suite/test_drop01.py`
**Storage mode:** General
**Components under test:** schema drop, history store, column groups

## Test Cases

### `test_drop01.test_drop_hs_truncate`
- **What it tests:** Verifies that `session.drop()` removes all history store (`WiredTigerHS.wt`) entries associated with the dropped table. Creates a table with two column groups (`cg1`, `cg2`), inserts a record at timestamp 5, updates it at timestamp 8, and checkpoints. At this point the history store should have exactly 2 entries (one per column group for the superseded value). After dropping the table, confirms the history store is empty (0 entries).
- **Components:** `src/schema/schema_drop.c`, `src/history/hs.c`, `src/schema/schema_config.c`
- **Notes:** Directly opens `file:WiredTigerHS.wt` via a cursor to count entries. Uses timestamped inserts (ts=5, ts=8). Column group scheme: `columns=(key,value1,value2)`, `colgroups=(cg1,cg2)`.
