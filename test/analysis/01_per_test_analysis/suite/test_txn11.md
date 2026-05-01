# test_txn11 — Empty checkpoints and log removal

**File:** `test/suite/test_txn11.py`
**Storage mode:** General
**Components under test:** log removal, empty checkpoint, log file lifecycle

## Test Cases

### `test_txn11.test_ops`
- **What it tests:** Populates a table with 700 rows; runs up to 500 forced checkpoints until all original log files have been removed (i.e., the log has rolled over completely past the checkpoint LSN); then reopens the connection with log removal disabled. Verifies that empty checkpoints correctly advance the log removal horizon and that the system can cleanly reopen after all original log files are gone.
- **Components:** `log.c`, `checkpoint.c`
- **Notes:** No parameterization. Uses `verbose=[transaction]` and `prealloc=false` to avoid preallocated log files interfering with the check. Tests that repeated empty checkpoints drive log removal forward.
