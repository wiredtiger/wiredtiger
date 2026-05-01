# test_salvage02 — Salvage after removing WiredTigerHS.wt (history store)

**File:** `test/suite/test_salvage02.py`
**Storage mode:** General
**Components under test:** salvage, history store, recovery

## Test Cases

### `test_salvage02.test_hs_removed`
- **What it tests:** Verifies that WiredTiger can open and salvage a database after `WiredTigerHS.wt` (the history store file) has been manually deleted. Opens with `salvage=true` in the connection config. Verifies the connection opens without error and the database is usable afterward.
- **Components:** `src/conn/conn_open.c`, `src/history/hs_cursor.c`, `src/btree/bt_salvage.c`
- **Notes:** Simulates a scenario where the history store is corrupted or missing. The `salvage=true` connection option triggers salvage of all tables at open time.
