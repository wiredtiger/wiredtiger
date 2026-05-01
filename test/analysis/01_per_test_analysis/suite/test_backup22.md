# test_backup22 — Import of dropped table in incremental backup

**File:** `test/suite/test_backup22.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental), table import, schema

## Test Cases

### `test_backup22.test_import_with_open_backup_cursor`
- **What it tests:** Creates and populates a table, takes a full incremental backup (ID1, 4 KB granularity), drops the table (keeping the file with `remove_files=false`), then re-imports the table using either metadata-based import or repair-based import (optionally with a checkpoint). Takes an incremental backup (ID1→ID2) to an empty `incr_dir` and verifies the full backup and incremental backup contain matching data, validating that an imported table is treated as fully changed in the incremental.
- **Components:** `src/cursor/cur_backup.c`, `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/backup/backup_config.c`
- **Notes:** Parametrized across 4 scenarios: import_with_metadata (no checkpoint), import_repair (no checkpoint), import_with_metadata_ckpt (with checkpoint), import_repair_ckpt (with checkpoint).
