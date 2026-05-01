# test_backup11 — Incremental backup error cases: ID validation, consolidation, mixed targets

**File:** `test/suite/test_backup11.py`
**Storage mode:** General
**Components under test:** backup cursor (incremental), error handling, ID management

## Test Cases

### `test_backup11.test_backup11`
- **What it tests:** Comprehensive error-case validation for incremental backup cursor configuration. Covers: (1) `file=` on primary cursor rejected; (2) incremental duplicate requires incremental primary; (3) `consolidate=true` cannot appear on a duplicate; (4) multiple duplicate cursors rejected; (5) duplicate of duplicate rejected; (6) file target on duplicate rejected; (7) mixing block-incremental with log target rejected; (8) `src_id`/`this_id` not allowed on duplicate; (9) `force_stop` not allowed on duplicate; (10) incremental duplicate requires a known `src_id`; (11) unknown `src_id` rejected; (12) IDs in WiredTiger namespace rejected; (13) IDs with grouping characters (colon) rejected; (14) same `src_id` and `this_id` rejected; (15) re-used `this_id` rejected. After all error testing, verifies recovery from the backup directory.
- **Components:** `src/cursor/cur_backup.c`, `src/backup/backup_config.c`
- **Notes:** Single, non-parametrized test. Uses an initial incremental full backup to establish ID1/ID2. Then exercises all error paths on IDs ID3, ID4.
