# test_dump — wt dump utility: dump/load round-trip and content verification

**File:** `test/suite/test_dump.py`
**Storage mode:** General
**Components under test:** wt utility (dump, load, list), cursor dump format, schema

## Test Cases

### `test_dump.test_dump`
- **What it tests:** Full dump-load round-trip for all combinations of URI type (file, table-simple, table-index, table-complex), key format (integer, recno, string), and dump format (hex `-x`, text). Steps:
  1. Populates the dataset.
  2. Dumps to `dump.out` (hex or text).
  3. Loads into a new directory.
  4. Compares `wt list` output between original and loaded directories.
  5. Reopens the loaded directory and calls `ds.check()` for data correctness.
  6. Re-loads into the original directory (overwrite) and checks again.
  7. Re-loads with `-n` (no-overwrite) and confirms failure with a non-empty error file.
  8. For `ComplexDataSet`, dumps an individual index URI and compares values with the table dump.
  9. Re-loads with `-r new_name` to rename the table and checks data correctness in the renamed URI.
- **Components:** `src/utilities/util_dump.c`, `src/utilities/util_load.c`, `src/schema/`
- **Notes:** 2500 entries. Scenarios: 4 types x 3 key formats x 2 dump formats = 24 combinations. Tags: `wt_util`.
