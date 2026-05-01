# test_alter03 — Alter app_metadata on tables, verify exclusive vs. non-exclusive lock behavior

**File:** `test/suite/test_alter03.py`
**Storage mode:** General | Tiered
**Components under test:** schema/alter, metadata, session API

## Test Cases

### `test_alter03.test_alter03_table_app_metadata`
- **What it tests:** Creates a table with initial `app_metadata`, then exercises the full matrix of metadata-alter semantics: normal alter (exclusive), alter with `exclusive_refreshed=true`, alter with `exclusive_refreshed=false` (non-exclusive, only updates the table-level metadata not the file-level metadata), and verifies that attempting an exclusive alter while a cursor is open fails with `WiredTigerError`. Also verifies that non-exclusive alter with an open cursor succeeds. After connection reopen, confirms metadata is retained correctly.
- **Components:** `src/schema/schema_alter.c`, `src/meta/meta_table.c`
- **Notes:** Parametrized across tiered storage sources only; always uses `table:` URI. Key edge case: `exclusive_refreshed=false` updates only the `table:` metadata entry, leaving the `file:` entry unchanged. Confirms both table and file metadata entries individually via a metadata cursor.
