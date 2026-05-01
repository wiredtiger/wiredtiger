# test_backup06 — Backup cursor does not open file handles; schema protection during backup

**File:** `test/suite/test_backup06.py`
**Storage mode:** General
**Components under test:** backup cursor, data handle management, schema protection, statistics

## Test Cases

### `test_backup06.test_cursor_open_handles`
- **What it tests:** Populates hundreds of tables (10 sets × 6 objects each), reopens the connection (so dhandles are closed), then measures the `dh_conn_handle_count` statistic before and after opening a backup cursor. Asserts the count does not change, confirming that opening a backup cursor does not open file handles. Unix-only (skipped on Windows).
- **Components:** `src/cursor/cur_backup.c`, `src/conn/conn_dhandle.c`
- **Notes:** Adjusts `RLIMIT_NOFILE` to at least 1024 before the test. Connection config: `statistics=(fast)`.

### `test_backup06.test_cursor_schema_protect`
- **What it tests:** Opens a backup cursor and verifies that: (1) `session.create()` is allowed during backup; (2) `session.drop()` on any backed-up file or table raises `WiredTigerError` (schema protection).
- **Components:** `src/cursor/cur_backup.c`, `src/schema/schema_drop.c`

### `test_backup06.test_cursor_reset`
- **What it tests:** Iterates a backup cursor to exhaustion, calls `cursor.reset()`, iterates again, and confirms the total count is exactly twice the initial count.
- **Components:** `src/cursor/cur_backup.c`
