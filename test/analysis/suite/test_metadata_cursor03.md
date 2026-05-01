# test_metadata_cursor03 — Atomic schema create operations and log record count

**File:** `test/suite/test_metadata_cursor03.py`
**Storage mode:** General (logging enabled: `log=(enabled)`)
**Components under test:** schema atomicity, log cursor, metadata, create/drop log records

## Test Cases

### `test_metadata03.test_metadata03_create`
- **What it tests:** Verifies that creating and dropping tables (including column groups and indexes) writes exactly 2 log records per schema operation (one commit record and one metadata sync record). Uses a log cursor (`log:`) to count whole-record entries (opcount == 0) before and after each DDL operation.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/log/log.c`, `src/cursor/cur_log.c`
- **Notes:** Parameterized by:
  - `file` — `file:` URI, no column groups or indexes
  - `table-cg` — `table:` URI with a column group `g0`
  - `table-index` — `table:` URI with an index `i0`
  - `table-simple` — `table:` URI with no sub-objects

  The `verify_logrecs` check (`count == origcnt + 2`) is currently commented out pending WT-3965 fix. The test still runs the DDL operations and counts log records, but does not assert the exact count.
