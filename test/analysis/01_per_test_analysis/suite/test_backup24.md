# test_backup24 — Selective backup with logged/not-logged tables, tables created during backup

**File:** `test/suite/test_backup24.py`
**Storage mode:** General
**Components under test:** backup cursor (selective), logging, partial restore, metadata cleanup

## Test Cases

### `test_backup24.test_backup24`
- **What it tests:** Creates 2 logged and 2 not-logged tables. Populates them until log file 2 is reached. Takes a selective full backup excluding one not-logged table (`not2.wt`). Creates new logged/not-logged tables while the backup cursor is open, confirms they are not in the backup file list. Takes a log-target duplicate backup. Opens the backup directory with `backup_restore_target` specifying only the 3 desired tables. Verifies: excluded tables are not present in the backup directory; excluded tables are absent from backup metadata; included tables recovered correctly.
- **Components:** `src/cursor/cur_backup.c`, `src/log/log.c`, `src/meta/meta_table.c`, `src/conn/conn_open.c`
- **Notes:** Non-parametrized. Uses `debug_mode=(table_logging=true)` and `log=(remove=false)`. Demonstrates partial restore removing tables from metadata.
