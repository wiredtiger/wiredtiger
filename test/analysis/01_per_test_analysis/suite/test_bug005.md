# test_bug005 — Verify succeeds when file has trailing garbage after the last checkpoint

**File:** `test/suite/test_bug005.py`
**Storage mode:** General
**Components under test:** verify, file format tolerance

## Test Cases

### `test_bug005.test_bug005`
- **What it tests:** Creates a file with 1000 key/value pairs, verifies it, forces to disk, verifies again. Then appends the string `'random data'` to the end of the raw file. Calls `session.verify()` again and confirms it still succeeds — WiredTiger should tolerate trailing garbage after the last valid checkpoint.
- **Components:** `src/session/session_api.c`, `src/verify/verify.c`, `src/block/block_ext.c`
- **Notes:** Non-parametrized. Regression for a case where trailing file data was incorrectly treated as corruption.
