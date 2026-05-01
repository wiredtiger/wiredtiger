# test_sweep07 — Regression: sweep after checkpoint with open cursor (WT-15647)

**File:** `test/suite/test_sweep07.py`
**Storage mode:** General
**Components under test:** file manager sweep, dhandle removal, session close, checkpoint

## Test Cases

### `test_sweep07.test_sweep_with_cursor`
- **What it tests:** Creates a table, writes one record, closes the cursor; waits 2 seconds (to let sweep handle any LAS/HS cleanup dhandles); records `dh_sweep_remove`; checkpoints the table; opens a second session/cursor, reads the table, closes cursor and session; closes the main session; waits 5 seconds for sweep to run; reopens a session and verifies `dh_sweep_remove` increased.
- **Components:** `file_manager.c`, `dhandle.c`, `checkpoint.c`
- **Notes:** Regression test for WT-15647 where a sweep could occur with an invalid pointer to a dhandle. The sequence tests that sweeping after closing all session references works correctly.
