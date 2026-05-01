# test_backup14 — Incremental backup lifecycle: add/remove/drop/recreate tables and bulk inserts

**File:** `test/suite/test_backup14.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental, block-based), schema (drop/create), bulk cursor, history store

## Test Cases

### `test_backup14.test_backup14`
- **What it tests:** Multi-phase incremental backup integration test covering: (1) initial data population + full+incremental backup validation; (2) removal of all records and backup comparison; (3) dropping the main table and adding a new one, taking incremental backup and confirming the dropped table is absent; (4) recreating the dropped table with new content and validating; (5) bulk-loading data into logged and not-logged tables and verifying both appear correctly in incremental backups. Uses `WT_BLOCK` as the home directory, separate `home_full` and `home_incr` directories.
- **Components:** `src/cursor/cur_backup.c`, `src/cursor/cur_bulk.c`, `src/schema/schema_drop.c`, `src/backup/backup_config.c`
- **Notes:** Non-parametrized. Max 7 iterations. bigkey/bigval are 300/500 bytes each.
