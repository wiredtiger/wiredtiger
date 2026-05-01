# test_schema09 — Schema recovery with incomplete table cleanup

**File:** `test/suite/test_schema09.py`
**Storage mode:** General
**Components under test:** schema, recovery, table create/drop, metadata consistency

## Test Cases

### `test_schema09.test_schema09`
- **What it tests:** Tests that recovery handles incomplete schema operations (interrupted before completion) correctly. Simulates four crash points: (1) before_insert_file: crash before inserting the main file into metadata, (2) before_insert_colgroup: crash before inserting a column group, (3) after_drop_file: crash after dropping the file but before completing drop, (4) after_drop_colgroup: crash after dropping column group but before completing. For each crash point, verifies that after recovery either the table exists and is usable, or is fully absent — never in a partial/corrupted state.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/meta/`, `src/log/`
- **Notes:** Uses failpoints (injected crash points) to simulate crashes at specific points within schema operations. Tests the schema recovery transaction logic. Four crash scenarios × column/row format = 8+ test variants.
