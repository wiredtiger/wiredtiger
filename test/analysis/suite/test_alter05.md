# test_alter05 — Alter log setting with open cursor and verify checkpoint stat is triggered

**File:** `test/suite/test_alter05.py`
**Storage mode:** General | Tiered
**Components under test:** schema/alter, logging (WAL), checkpoint, statistics, session API

## Test Cases

### `test_alter05.test_alter05`
- **What it tests:** Creates a `file:` URI with logging enabled, writes timestamped data, then: (1) alters the log setting to disabled and verifies that the `session_table_alter_trigger_checkpoint` statistic increments by 1 per alter; (2) attempts to alter again while a cursor is open and asserts this raises `WiredTigerError`; (3) confirms the failed alter also increments the checkpoint counter (alter triggers a checkpoint even before detecting the busy state). Verifies metadata reflects the correct log setting throughout.
- **Components:** `src/schema/schema_alter.c`, `src/log/log.c`, `src/session/session_api.c`, `src/stat`
- **Notes:** Uses `statistics=(all)` connection config. Relies on timestamps and stable_timestamp to gate the alter-triggered checkpoint. Parametrized across tiered storage sources.
