# test_util11 — wt list CLI: listing tables and output file option

**File:** `test/suite/test_util11.py`
**Storage mode:** General
**Components under test:** `wt list`, metadata enumeration

## Test Cases

### `test_util11.test_list_none`
- **What it tests:** Runs `wt list` on an empty database; verifies the output file is empty (no tables to list).
- **Components:** `util_list.c`, `meta.c`
- **Notes:** No parameterization.

### `test_util11.test_list`
- **What it tests:** Creates 5 tables (some populated, some empty); runs `wt list table:`; verifies all 5 tables appear in alphabetical order in the output.
- **Components:** `util_list.c`, `meta.c`
- **Notes:** Tables are created in non-alphabetical order (5,3,1,2,4) to verify output is sorted.

### `test_util11.test_list_drop`
- **What it tests:** Creates 5 tables; drops tables 2 and 4; runs `wt list table:`; verifies only tables 1, 3, 5 appear in the output.
- **Components:** `util_list.c`, `meta.c`, `schema.c`
- **Notes:** Tests that dropped tables are not included in list output.

### `test_util11.test_list_drop_all`
- **What it tests:** Creates 5 tables; drops all 5; runs `wt list`; verifies the output is empty.
- **Components:** `util_list.c`, `meta.c`, `schema.c`
- **Notes:** No parameterization.

### `test_util11.test_list_file_output`
- **What it tests:** Runs `wt list -c -f customfile table:`; verifies stdout is empty; then runs without `-f`; verifies that output with and without `-f` is identical (i.e., `-f` redirects output to a file).
- **Components:** `util_list.c`
- **Notes:** Tests the `-f <file>` option for directing list output to a custom file.
