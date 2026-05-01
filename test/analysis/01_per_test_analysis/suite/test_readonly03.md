# test_readonly03 — Read-only connection: all write operations return "Unsupported" error

**File:** `test/suite/test_readonly03.py`
**Storage mode:** General
**Components under test:** read-only connection, cursor write operations, session write operations, error handling

## Test Cases

### `test_readonly03.test_readonly`
- **What it tests:** Creates a table with data, reopens in `readonly=true` mode, and exhaustively verifies that all write operations return an "Unsupported" error; covers cursor operations (insert, remove, update) and session operations (alter, create, compact, drop, flush_tier, log_flush, log_printf, salvage, truncate)
- **Components:** `conn/conn_open.c`, `cursor/cur_std.c`, `session/session_api.c`
- **Notes:** No scenarios; each write operation is called after opening the database in readonly mode and the resulting exception is checked for the "Unsupported" message; read operations (open_cursor, search, next) are also tested to confirm they succeed in readonly mode; this test provides a comprehensive API contract check for the readonly connection guarantee
