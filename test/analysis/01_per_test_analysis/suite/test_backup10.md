# test_backup10 — Duplicate backup cursor for log target; log removal interaction

**File:** `test/suite/test_backup10.py`
**Storage mode:** General
**Components under test:** backup cursor, duplicate backup cursor, log removal, error handling

## Test Cases

### `test_backup10.test_backup10`
- **What it tests:** Inserts data until log file 2 exists, takes a full backup, then opens a duplicate cursor with `target=("log:")` to capture logs written while the primary cursor is open. Verifies the duplicate log set is a strict superset of the original log set by exactly 1 file (log file 3 appears only in the duplicate, since opening the duplicate triggers a second log switch). Asserts log file 4 does not appear. Tests error cases: multiple duplicate cursors rejected; duplicate of duplicate rejected; duplicate without log target rejected.
- **Components:** `src/cursor/cur_backup.c`, `src/log/log.c`
- **Notes:** Parametrized across log removal=true and log removal=false. 1 GB cache, 100 KB max log.
