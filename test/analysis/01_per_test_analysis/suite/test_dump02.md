# test_dump02 — wt dump utility: single-key lookup, nearest-key, and range bound options

**File:** `test/suite/test_dump02.py`
**Storage mode:** General
**Components under test:** wt utility (dump), key filtering, range bounds

## Test Cases

### `test_dump.test_dump`
- **What it tests:** Populates 99 rows and runs a plain `wt dump`, then counts the number of data lines in the output. Expects `(n_rows - 1) * 2` lines (99 rows x 2 lines each = 198).
- **Components:** `src/utilities/util_dump.c`
- **Notes:** Uses `key_format=u,value_format=u`. Tags: `wt_util`.

### `test_dump.test_dump_single_key`
- **What it tests:** Tests the `-k` (single key lookup) and `-n` (nearest key) options:
  - `-k key0` (non-existent): 0 data lines.
  - `-k key0 -n` (nearest to non-existent): 2 data lines (one record, the nearest key found).
  - `-k key1` (existing): 2 data lines.
- **Components:** `src/utilities/util_dump.c`
- **Notes:** String keys `key1`..`key99`; lexicographic ordering means `key0` sorts between `key` (empty) and `key1`.

### `test_dump.test_dump_bounds`
- **What it tests:** Tests the `-l` (lower bound) and `-u` (upper bound) options for range-filtered dump:
  - `-l key50`: Dumps from key50 onward. Expects keys 50-99 plus key6, key7, key8, key9 (lexicographic order places these after key50+) = 54 records x 2 lines = 108 lines.
  - `-u key11`: Dumps up through key11. Expects key1, key10, key11 = 3 records x 2 = 6 lines.
  - `-l key50 -u key59`: Range dump, expects 10 records x 2 = 20 lines.
- **Components:** `src/utilities/util_dump.c`
- **Notes:** Lexicographic key ordering is important for understanding the expected counts. Tags: `wt_util`.
