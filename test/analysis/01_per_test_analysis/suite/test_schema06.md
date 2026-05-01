# test_schema06 — Schema index stress test: repeated create and drop

**File:** `test/suite/test_schema06.py`
**Storage mode:** General
**Components under test:** schema, index, create/drop cycles

## Test Cases

### `test_schema06.test_index_stress`
- **What it tests:** Repeatedly creates and drops indices on a populated table to stress the index lifecycle. Each iteration creates a new index, verifies its cursor works, then drops it. Tests that repeated create/drop does not corrupt the schema or the underlying data.
- **Components:** `src/schema/schema_create.c`, `src/schema/schema_drop.c`, `src/meta/`, `src/cursor/cur_index.c`
- **Notes:** Stress test targeting index create/drop code paths. Number of iterations is configurable. Tests that metadata is consistent after each drop and that a new create with the same name succeeds.
