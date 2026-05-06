# test_util14 — wt truncate CLI: full table truncation and error cases

**File:** `test/suite/test_util14.py`
**Storage mode:** General
**Components under test:** `wt truncate`, URI validation, truncate error cases

## Test Cases

### `test_util14.test_truncate_process`
- **What it tests:** Creates a table and populates it with 1,000 string key/value pairs; runs `wt truncate table:<name>`; verifies the table still exists but is empty (read on any key returns "not found"); then tests 4 error cases: (1) missing URI — expects "usage:", (2) invalid URI `foobar` — expects "No such file or directory", (3) non-existent table URI — expects "No such file or directory", (4) two URIs specified — expects "usage:".
- **Components:** `util_truncate.c`, `schema.c`
- **Notes:** No parameterization. Covers both the success path and argument validation edge cases.
