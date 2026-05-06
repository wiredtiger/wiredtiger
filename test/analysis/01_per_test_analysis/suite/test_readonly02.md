# test_readonly02 — Read-only connection: invalid configuration combinations

**File:** `test/suite/test_readonly02.py`
**Storage mode:** General
**Components under test:** read-only connection, error handling, configuration validation, crash recovery

## Test Cases

### `test_readonly02.test_readonly`
- **What it tests:** Verifies three error cases for invalid use of `readonly=true`: (1) opening a non-existent database with readonly (expects "No such file" error); (2) opening a database after an unclean shutdown with readonly (expects "needs recovery" error, since recovery cannot run in readonly mode); (3) opening a database with both `log=(zero_fill=true)` and `readonly=true` (expects "Invalid argument" error, as zero_fill requires write access)
- **Components:** `conn/conn_open.c`, `log/log.c`, `conn/conn_recover.c`
- **Notes:** No scenarios; each case is tested by constructing the invalid configuration and asserting the expected exception message; the unclean shutdown case is simulated by writing data without a clean close (leaving the database in a state requiring recovery)
