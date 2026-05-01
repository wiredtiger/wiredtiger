# test_bug020 — WiredTiger.turtle.set replaces missing WiredTiger.turtle on open

**File:** `test/suite/test_bug020.py`
**Storage mode:** General
**Components under test:** turtle file recovery, connection open

## Test Cases

### `test_bug020.test_bug020`
- **What it tests:** Verifies that if `WiredTiger.turtle` is missing but `WiredTiger.turtle.set` exists, WiredTiger can still open the database successfully by using the `.set` file as a replacement. Populates 1000 rows, closes the connection, renames `WiredTiger.turtle` to `WiredTiger.turtle.set`, then reopens the connection. Asserts that stdout contains the message `WiredTiger.turtle not found` (confirming the fallback path was taken) and that no error is raised.
- **Components:** `src/conn/conn_open.c`, `src/meta/meta_turtle.c`
- **Notes:** Non-parametrized.
