# test_schema01 — Schema with column groups and tiered storage

**File:** `test/suite/test_schema01.py`
**Storage mode:** General
**Components under test:** schema, column groups, create/drop, tiered storage

## Test Cases

### `test_schema01.test_populate`
- **What it tests:** Creates tables with column groups using various combinations of column group count and key/value format. Populates each table, verifies data is correct via cursor scan, then drops the table and re-creates it. Tests column group schema across the full create/drop lifecycle.
- **Components:** `src/schema/`, `src/meta/`, `src/cursor/`
- **Notes:** Parametrized on number of column groups and table type (simple table, table with multiple column groups). Tests that column group creation and drop works correctly. Tiered storage hook support mentioned in class-level skip.
