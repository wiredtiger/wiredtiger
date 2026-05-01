# test_schema08 — Schema operations (alter/drop) with log truncation at each LSN during recovery

**File:** `test/suite/test_schema08.py`
**Storage mode:** General
**Components under test:** schema, recovery, logging, alter, drop, LSN truncation

## Test Cases

### `test_schema08.test_schema08_create`
- **What it tests:** Tests schema operation recovery with simulated log truncation at each possible log sequence number (LSN). For each LSN checkpoint, truncates the log at that point, reopens the database, and verifies that the schema is in a consistent state (either the operation completed or did not, but not partially). Covers `alter` and `drop` schema operations as crash scenarios.
- **Components:** `src/schema/schema_alter.c`, `src/schema/schema_drop.c`, `src/log/`, `src/meta/`
- **Notes:** Truncates the WAL at every possible LSN to create all crash scenarios between schema operations. Each truncation point is reopened to verify consistency. Verifies that partial schema operations are not left in an inconsistent state after recovery. Tests `alter` (changing table configuration) and `drop` (removing tables).
