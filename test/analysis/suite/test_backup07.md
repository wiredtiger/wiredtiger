# test_backup07 — Backup with tables created during backup and recovery of log records

**File:** `test/suite/test_backup07.py`
**Storage mode:** General
**Components under test:** backup cursor, duplicate backup cursor (log target), logging, recovery

## Test Cases

### `test_backup07.test_backup07`
- **What it tests:** Inserts data until log file 2 is created, then opens a backup cursor and creates/populates a new table (`newtable`) while the backup is open. Takes a full backup (confirming `newtable` is not included in the backup file list, since it was created after cursor open). Uses a duplicate log-target cursor to copy all logs. Opens the backup directory and confirms successful recovery even though the log contains records for the new table's file ID (which does not exist in the backup).
- **Components:** `src/cursor/cur_backup.c`, `src/log/log.c`, `src/conn/conn_open.c`
- **Notes:** 1 GB cache. Requires crossing into log file 2 before starting backup. Parametrized: single scenario `table:test`.
