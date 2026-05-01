# test_util17 — wt stat CLI: connection and table statistics output

**File:** `test/suite/test_util17.py`
**Storage mode:** General
**Components under test:** `wt stat`, statistics output format

## Test Cases

### `test_util17.test_stat_process`
- **What it tests:** Creates a table; runs `wt stat` (connection-level statistics); verifies the output contains `"cursor: cursor create calls="`; runs `wt stat table:<name>` (table-level statistics); verifies the output contains `"cache_walk: Entries in the root page=1"`.
- **Components:** `util_stat.c`, `stat.c`
- **Notes:** No parameterization. Tests output format and basic content rather than specific numeric values. Confirms both connection-level and data-source-level stat output paths work.
