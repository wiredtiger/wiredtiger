# test_backup12 — Block-based incremental backup: full + incremental cycle with table drop

**File:** `test/suite/test_backup12.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental, block-based), data handle lifecycle, log target duplicate

## Test Cases

### `test_backup12.test_backup12`
- **What it tests:** Creates 3 tables with big key/value pairs, takes a full incremental backup (ID1, granularity 1 MB), adds more data, drops one table (`uri_rem`), then takes an incremental backup (ID1→ID2). Removes files from the backup directory that are not in the incremental set. Opens and verifies the resulting backup. Tests interaction between table drop and incremental block bitmap tracking.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`, `src/schema/schema_drop.c`
- **Notes:** Single non-parametrized test. `bigkey = 'Key' * 100`, `bigval = 'Value' * 100`, 1000 ops. Also uses a log-target duplicate cursor to capture mid-backup log files.
