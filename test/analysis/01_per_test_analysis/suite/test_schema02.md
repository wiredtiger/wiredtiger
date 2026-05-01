# test_schema02 — Schema index creation error handling and validation

**File:** `test/suite/test_schema02.py`
**Storage mode:** General
**Components under test:** schema, index creation, column groups, error validation

## Test Cases

### `test_schema02.test_colgroup_after_failure`
- **What it tests:** Verifies that creating a column group after a failed column group creation (e.g., mismatched columns) correctly fails and leaves the schema in a consistent state.
- **Components:** `src/schema/schema_create.c`, `src/meta/`
- **Notes:** Tests error recovery from partial schema creation.

### `test_schema02.test_colgroup_failures`
- **What it tests:** Attempts to create column groups with various invalid configurations (wrong column names, invalid formats) and verifies each raises an appropriate error.
- **Components:** `src/schema/schema_create.c`
- **Notes:** Error path testing for column group creation.

### `test_schema02.test_index`
- **What it tests:** Creates indices on a table both before and after data is populated. Verifies index cursor iteration returns data in sorted key order. Tests that indices created after populate reflect existing data.
- **Components:** `src/schema/schema_create.c`, `src/cursor/cur_index.c`
- **Notes:** Index creation before-populate and after-populate scenarios.

### `test_schema02.test_colgroups`
- **What it tests:** Creates a multi-column-group table, populates it, and verifies data is correctly split across column groups. Tests cursor access to individual column groups.
- **Components:** `src/schema/schema_create.c`, `src/cursor/cur_colgroup.c`
- **Notes:** Multi-column-group data layout validation.
