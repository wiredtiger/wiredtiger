# test_drop_create — Drop and re-create table with and without schema changes across sessions

**File:** `test/suite/test_drop_create.py`
**Storage mode:** General
**Components under test:** schema drop, schema create, session table cache, multi-session

## Test Cases

### `test_drop_create.test_drop_create`
- **What it tests:** Iterates over three configs (`None`, `key_format=S,value_format=S`, `None`) and for each: force-drops `table:test`, creates it with that config, drops it without force, opens a new session, creates again, and closes. Verifies that repeated drop-create cycles with varying configs (including None/default) do not cause errors or leave stale metadata.
- **Components:** `src/schema/schema_drop.c`, `src/schema/schema_create.c`
- **Notes:** The default session is closed at the start and replaced with explicit sessions to test session lifecycle interactions.

### `test_drop_create.test_drop_create2`
- **What it tests:** Tests that the per-session table cache is invalidated correctly when a table is dropped and re-created with a different schema by a different session. Session 2 opens a cursor on the original table (key=S, value=S + named columns), then the table is dropped and re-created with a different schema (key=S, value=l). Session 2 opens a cursor on the re-created table and inserts data, confirming it uses the new schema (`value=l` — integer) rather than the stale cached schema.
- **Components:** `src/schema/schema_drop.c`, `src/schema/schema_create.c`, `src/session/session_api.c`
- **Notes:** The key scenario is multi-session table cache invalidation on schema change. The second cursor insert (`c2["Hi"] = 1`) exercises the new `value_format=l` schema.
