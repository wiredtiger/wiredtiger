# test_schema03 — Schema stress test with random operations and connection reopens

**File:** `test/suite/test_schema03.py`
**Storage mode:** General
**Components under test:** schema, create/drop/rename, index, column groups, recovery

## Test Cases

### `test_schema03.test_schema`
- **What it tests:** Large random schema stress test that creates multiple tables with column groups and indices in random order, performs data operations (insert, update, read), and periodically reopens the WiredTiger connection. Verifies that the schema is consistent after each reopen and that data is not corrupted across connection restarts.
- **Components:** `src/schema/`, `src/meta/`, `src/conn/conn_open.c`, `src/cursor/`
- **Notes:** Runs a long stress test with random schema operations. Tests schema recovery across multiple connection opens/closes. Random seed can be set for reproducibility. Schema operations include: table create, column group create, index create, table drop, all interleaved with cursor inserts and reads.
