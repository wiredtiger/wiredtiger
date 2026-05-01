# test_bug006 — verify/salvage/drop fail with open cursor; succeed after cursor close

**File:** `test/suite/test_bug006.py`
**Storage mode:** General
**Components under test:** session API (verify, salvage, drop, truncate), cursor lifecycle

## Test Cases

### `test_bug006.test_bug006`
- **What it tests:** Creates a table with 1000 rows, leaves a cursor open (not yet closed), then asserts that `session.drop()`, `session.salvage()`, and `session.verify()` each raise `WiredTigerError` (EBUSY). After closing the cursor, confirms that `session.salvage()`, `session.truncate()`, and `session.verify()` all succeed, followed by `session.drop()`.
- **Components:** `src/schema/schema_drop.c`, `src/session/session_api.c`, `src/verify/verify.c`
- **Notes:** Parametrized across (file:, table:) URIs. Skipped for tiered storage (negative API tests behave differently). Skipped if tiered hook is active.
