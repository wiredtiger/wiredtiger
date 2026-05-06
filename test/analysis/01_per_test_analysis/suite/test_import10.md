# test_import10 — Import/export while a backup cursor is open

**File:** `test/suite/test_import10.py`
**Storage mode:** General
**Components under test:** schema/import, backup cursor, statistics

## Test Cases

### `test_import10.test_import_with_open_backup_cursor`
- **What it tests:** Verifies that importing a table succeeds while a backup cursor is concurrently open. Also checks that the newly imported file is correctly excluded from the backup (since it was imported after the backup cursor was opened).
- **Components:** `src/schema/schema_create.c`, `src/backup/backup_cursor.c`, `src/stat/`
- **Notes:** Parameterized by:
  - `import_with_metadata` — uses `import=(enabled,repair=false,file_metadata=(...))`
  - `import_repair` — uses `import=(enabled,repair=true)`

  Verifies stats: `stat.conn.session_table_create_import_success == 1`. For repair scenario, additionally checks `stat.conn.session_table_create_import_repair == 1`. After import, takes a full backup and asserts the imported `.wt` file is NOT in the backup file list (expected: backup cursor does not include files created after it was opened).
