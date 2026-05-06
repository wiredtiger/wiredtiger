# test_backup21 — Concurrent table create/drop operations while backup cursor is open

**File:** `test/suite/test_backup21.py`
**Storage mode:** General
**Components under test:** backup cursor, schema (create/drop), concurrent ops

## Test Cases

### `test_backup21.test_concurrent_operations_with_backup`
- **What it tests:** Runs a background op thread that creates and drops tables concurrently with repeated backup cursor opens and full backup iterations. For each iteration: a new table is created (first 25 ops) or dropped (second 25 ops) by the op thread while a backup is taken. Verifies that newly created tables do not appear in the backup file list (they were created after cursor open), and that tables being dropped do still appear in the backup file list (they existed when the cursor was opened). 50 total iterations.
- **Components:** `src/cursor/cur_backup.c`, `src/schema/schema_create.c`, `src/schema/schema_drop.c`
- **Notes:** Non-parametrized. Uses `wtthread.op_thread` with `'t'` (create table) and `'d'` (drop table) operations.
