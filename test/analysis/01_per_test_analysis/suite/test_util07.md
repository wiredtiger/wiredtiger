# test_util07 — wt read CLI: key lookup in a table

**File:** `test/suite/test_util07.py`
**Storage mode:** General
**Components under test:** `wt read`, cursor search, key-not-found error

## Test Cases

### `test_util07.test_read_empty`
- **What it tests:** Creates an empty table; runs `wt read table:<name> NoMatch`; verifies the output file is empty and the error file contains `'NoMatch: not found'`; expects failure exit code.
- **Components:** `util_read.c`, `cursor.c`
- **Notes:** No parameterization.

### `test_util07.test_read_populated`
- **What it tests:** Populates a table with 1,000 string key/value pairs (`KEY0..KEY999` → `VAL0..VAL999`); runs `wt read` with an existing key (`KEY49`) and verifies `VAL49` is returned; runs again with wrong case (`key49`) and verifies not-found error.
- **Components:** `util_read.c`, `cursor.c`
- **Notes:** Tests case-sensitive key matching (string keys are case-sensitive).
