# test_alter02 — Alter log=(enabled) setting on tables and verify WAL records

**File:** `test/suite/test_alter02.py`
**Storage mode:** General | Tiered
**Components under test:** schema/alter, logging (WAL), metadata, session API

## Test Cases

### `test_alter02.test_alter02_log`
- **What it tests:** Creates a connection with logging enabled or disabled, creates a table with logging on or off, writes data, then alters the log setting (via `session.alter()`), optionally reopens the connection with a different log setting, writes more data, and verifies both metadata correctness and that WAL records appear exactly the expected number of times (2× per logged operation, since the log cursor returns both the full record and the individual operation).
- **Components:** `src/schema/schema_alter.c`, `src/log/log.c`, `src/meta/meta_table.c`
- **Notes:** Parametrized across connection log state (always/create/reopen/never logged) × 4 URI types (file, table+colgroup, table+index, table-simple) × 4 table log settings (always/create/alter/never logged) × 2 reopen scenarios × N tiered sources. Uses binary sentinel values to distinguish pre- and post-alter writes. Overrides `setUpConnectionOpen`/`setUpSessionOpen` to control logging at connection creation.
