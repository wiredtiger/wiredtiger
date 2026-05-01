# test_debug_mode03 — Tests debug_mode table_logging forces log records for non-logged tables

**File:** `test/suite/test_debug_mode03.py`
**Storage mode:** General
**Components under test:** debug mode, logging, table logging override, timestamps in log records

## Test Cases

### `test_debug_mode03.test_table_logging`
- **What it tests:** Verifies that with `debug_mode=(table_logging=true)`, inserts into a table created with `log=(enabled=false)` still produce log records containing the inserted binary value. Confirms that the number of log records with matching payload equals the number of entries (100).
- **Components:** `src/log/`, `src/conn/conn_debug.c`, `src/btree/`
- **Notes:** Searches for a specific binary value (`\x01\x02abcd\x03\x04`) in log record values via a log cursor. Binary value is chosen to be recognisable but not common.

### `test_debug_mode03.test_table_logging_off`
- **What it tests:** Verifies that reconfiguring `debug_mode=(table_logging=false)` suppresses all log records for operations on a non-logged table (count of matching log records is 0).
- **Components:** `src/log/`, `src/conn/conn_debug.c`
- **Notes:** Uses `conn.reconfigure()` to disable table logging after the connection was opened with it enabled.

### `test_debug_mode03.test_table_logging_ts`
- **What it tests:** Verifies that when `table_logging=true`, prepared transaction timestamps (read, prepare, commit, durable) are all present in the log records. Uses large timestamps (`0x1020304050600000`-range) to make packed-integer encoding unique and avoid false positives. Checks commit, read, prepare, commit, and durable timestamps all appear in log.
- **Components:** `src/log/`, `src/txn/`, `src/conn/conn_debug.c`
- **Notes:** Implements its own packed-integer encoding helper (`pack_large_int`) to search for timestamps in raw log bytes. Tests both a normal committed-with-timestamp transaction and a prepared transaction using `timestamp_transaction` for read, prepare, commit, and durable timestamps.
