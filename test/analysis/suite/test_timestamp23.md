# test_timestamp23 — Delete/re-insert/delete cycle and write conflict detection

**File:** `test/suite/test_timestamp23.py`
**Storage mode:** General
**Components under test:** timestamp delete, re-insert, eviction, write conflict (WT_ROLLBACK)

## Test Cases

### `test_timestamp23.test_timestamp`
- **What it tests:** Writes value1/value2 at ts=11; deletes key at ts=21; re-inserts value3 at ts=31; deletes again at ts=41; evicts the page via debug cursor; opens a second session at read_ts=12 (sees value1); attempts to delete the key from session2 — expects `WT_ROLLBACK` (write conflict). Verifies the column-store bug (fixed August 2021) where a conflicting remove could succeed and produce an invalid tombstone.
- **Components:** `txn.c`, `txn_timestamp.c`, `evict.c`, `col_store.c`
- **Notes:** Parameterized over column and integer-row key formats. Regression test for a column store bug.
