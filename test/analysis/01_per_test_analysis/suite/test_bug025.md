# test_bug025 — WT-7208: crash on second access to missing index file

**File:** `test/suite/test_bug025.py`
**Storage mode:** General
**Components under test:** index open, error recovery, dhandle lifecycle

## Test Cases

### `test_bug025.test_bug025`
- **What it tests:** Reproduces WT-7208 where, after an index file is missing and the first access returns an error, a subsequent access to the same table crashes with a NULL pointer dereference. Populates a `ComplexDataSet` table (which includes indexes), closes the connection, removes the first index `.wti` file, then reopens the connection. Attempts to insert a new record twice using the same cursor — the first insert must raise an exception (missing file), and crucially the second insert must also raise an exception rather than crashing. Verifies that `No such file or directory` is reported in stderr at some point.
- **Components:** `src/schema/schema_open.c`, `src/cursor/cur_table.c`
- **Notes:** Non-parametrized. Uses `ComplexDataSet` (creates indexes). The `expectedStderrPattern` context manager wraps both insert attempts.
