# test_export01 — backup:export cursor and WiredTiger.export file lifecycle

**File:** `test/suite/test_export01.py`
**Storage mode:** General (tiered storage supported for test_export; test_export_restart skipped for tiered)
**Components under test:** backup (export cursor), schema, filesystem

## Test Cases

### `test_export01.test_export`
- **What it tests:** Creates three tables, inserts one record each, checkpoints (with optional flush_tier for tiered), opens a `backup:export` cursor, copies the home directory, and verifies:
  1. `WiredTiger.export` exists in the home directory while the backup cursor is open.
  2. After closing the backup cursor, `WiredTiger.export` is removed from the home directory.
  3. `WiredTiger.export` exists in the backup directory after the copy.
- **Components:** `src/backup/`, `src/conn/`
- **Notes:** Uses `copy_wiredtiger_home` helper. Tiered scenarios flush with `flush_tier=(enabled)`.

### `test_export01.test_export_restart`
- **What it tests:** Creates two tables in the main database, opens a `backup:export` cursor, copies the database, closes the cursor. Opens a new connection on the copy. Creates a third table, inserts a record, checkpoints, drops the second table, then opens another `backup:export` cursor. Asserts that the `WiredTiger.export` file in the copy directory:
  1. Does not contain the name of the dropped table (`exportb`).
  2. Does contain the name of the newly created table (`exportc`).
- **Components:** `src/backup/`, `src/schema/`, `src/conn/`
- **Notes:** Skipped for tiered storage. Makes an extra copy of the export file (`WiredTiger.export.original`) for debugging WT-9203 regressions.
