# test_cursor07 — Log cursor reading: logged vs non-logged tables

**File:** `test/suite/test_cursor07.py`
**Storage mode:** General
**Components under test:** log cursor (`log:` URI), WAL reading, logged/non-logged tables

## Test Cases

### `test_cursor07.test_log_cursor`
- **What it tests:** Opens a log cursor (`log:` URI) and reads WAL entries after inserting into both logged and non-logged tables (both in the same transaction and in separate transactions). Verifies that log records appear for logged tables and not for non-logged tables. Also tests cursor reopen with cached log cursors.
- **Components:** `src/log/`, `src/cursor/cur_log.c`
- **Notes:** Scenarios: `regular` (no cursor caching) and `reopen` (with `cache_cursors=true`). Uses DSync transaction sync (`sync=(method=dsync)`). Verifies that non-logged table inserts (same txn or separate) do not produce log records.
