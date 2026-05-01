# test_compact09 — Background compaction exclude list prevents specified tables from being compacted

**File:** `test/suite/test_compact09.py`
**Storage mode:** General (skips tiered)
**Components under test:** background compaction server, exclude list, statistics

## Test Cases

### `test_compact09.test_compact09`
- **What it tests:** Verifies that the background compaction `exclude=[...]` list correctly prevents specified tables from being compacted. When both tables are excluded, neither is compacted. When only one is excluded, only the other is compacted.
- **Components:** `src/support/background_compact.c`, `src/session/session_compact.c`
- **Notes:** Skip: tiered. Creates 2 tables with 90% of rows deleted. First run: excludes both tables — asserts `background_compact_skipped_exclude == n_tables` and `files_compacted == 0`. Second run: excludes only table_0 — waits for skipped count to increment by 1, then waits for table_1 to be compacted. Verifies `pages_rewritten(table_0) == 0` and `pages_rewritten(table_1) > 0`. Uses cumulative stats across runs.
