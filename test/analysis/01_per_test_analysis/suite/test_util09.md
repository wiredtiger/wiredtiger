# test_util09 — wt loadtext CLI: load newline-delimited key/value text

**File:** `test/suite/test_util09.py`
**Storage mode:** General
**Components under test:** `wt loadtext`, text-format data loading

## Test Cases

### `test_util09.test_loadtext_empty`
- **What it tests:** Creates an empty table; runs `wt loadtext -f loadtext.in table:<name>` with an empty input file; verifies the table remains empty.
- **Components:** `util_load.c` (loadtext path)
- **Notes:** No parameterization.

### `test_util09.test_loadtext_empty_stdin`
- **What it tests:** Same as `test_loadtext_empty` but passes the input file via stdin instead of `-f`.
- **Components:** `util_load.c`
- **Notes:** Tests stdin ingestion path.

### `test_util09.test_loadtext_populated`
- **What it tests:** Creates a table; writes 210 key/value pairs to `loadtext.in` (range 1010–1220) with format `key\nval\n`; loads via `wt loadtext -f`; verifies all 210 key/value pairs are present and correct.
- **Components:** `util_load.c`
- **Notes:** No parameterization.

### `test_util09.test_loadtext_populated_stdin`
- **What it tests:** Same as `test_loadtext_populated` but uses stdin for input; range 200–300 (100 entries).
- **Components:** `util_load.c`
- **Notes:** Tests stdin path with non-empty data.
