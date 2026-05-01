# test_backup27 — Selective backup clears history store entries for excluded tables

**File:** `test/suite/test_backup27.py`
**Storage mode:** General
**Components under test:** backup cursor (selective), history store, partial restore, timestamps

## Test Cases

### `test_backup27.test_backup27`
- **What it tests:** Creates 2 tables, writes timestamped data at ts=1 and ts=5 to both, sets stable_timestamp=10, checkpoints. Takes a selective backup excluding one table (`table_no_hs`). Opens the backup with `backup_restore_target` for only the first table. Verifies: (1) history store data is still intact for the retained table (reads at ts=1 and ts=10 return correct values); (2) the excluded table cannot be opened; (3) after recreating the excluded table, it has no historical data (WT_NOTFOUND at ts=1 and ts=10).
- **Components:** `src/cursor/cur_backup.c`, `src/history/hs_cursor.c`, `src/conn/conn_open.c`
- **Notes:** Non-parametrized. Demonstrates that selective backup partial restore properly cleans up history store entries for excluded tables.
