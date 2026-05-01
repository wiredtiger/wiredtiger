# test_schema07 — Schema stress test: 20K table create/drop loop to prevent metadata cache overflow

**File:** `test/suite/test_schema07.py`
**Storage mode:** General
**Components under test:** schema, metadata, cache, table create/drop

## Test Cases

### `test_schema07.test_many_tables`
- **What it tests:** Creates and drops 20,000 tables in a loop to verify that the metadata cache does not fill up and cause the system to stall or fail. Each table is created, a cursor is opened, data is inserted, and then the table is dropped. Verifies that WiredTiger correctly evicts metadata cache entries and doesn't accumulate unbounded metadata for dropped tables.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/meta/`, `src/cache/`
- **Notes:** Tagged as `longtest` — only runs under extended test suites. Tests the metadata cache pressure path specifically. 20K tables is chosen to reliably trigger the issue if the cache doesn't evict properly.
