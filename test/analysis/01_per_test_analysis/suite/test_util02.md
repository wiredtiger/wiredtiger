# test_util02 — wt load CLI: dump-then-load roundtrip and load command-line argument validation

**File:** `test/suite/test_util02.py`
**Storage mode:** General
**Components under test:** `wt load`, `wt dump`, load command-line argument processing

## Test Cases

### `test_util02.test_load_process`
- **What it tests:** Dumps a table with 1,000 entries using `wt dump` (print format) and loads it into a second table using `wt load -f dump.out -r <tablename>`; verifies all keys and values are correctly reproduced.
- **Components:** `util_load.c`, `util_dump.c`
- **Notes:** Parameterized over SS/rS/ri/ii key-value format combinations. Uses pseudo-random strings with all character values 1–255 and wide integer ranges.

### `test_util02.test_load_process_hex`
- **What it tests:** Same roundtrip as `test_load_process` but using `wt dump -x` (hex format) and then `wt load -f`.
- **Components:** `util_load.c`, `util_dump.c`
- **Notes:** Same parameterization. Verifies hex dump/load preserves data fidelity.

### `test_load_commandline.test_load_commandline_1`
- **What it tests:** `wt load -f dump.out` with no additional arguments; verifies success.
- **Components:** `util_load.c`
- **Notes:** Uses `ComplexDataSet` with 20 entries.

### `test_load_commandline.test_load_commandline_2`
- **What it tests:** Verifies that unpaired arguments (odd number of URI/config pairs) cause failure with error output.
- **Components:** `util_load.c`
- **Notes:** Two cases: single unpaired arg, and 3-arg list.

### `test_load_commandline.test_load_commandline_3`
- **What it tests:** Verifies that short-hand URI `table` with config succeeds for matching a single object; `colgroup` with config fails when no colgroup matches.
- **Components:** `util_load.c`
- **Notes:** Tests URI type short-form matching logic.

### `test_load_commandline.test_load_commandline_4`
- **What it tests:** Verifies that referencing an existing URI with config succeeds; referencing a non-existent URI (`table:bar`) fails.
- **Components:** `util_load.c`
- **Notes:** No parameterization.

### `test_load_commandline.test_load_commandline_5`
- **What it tests:** Verifies that multiple configuration arguments for the same object (repeated URI/config pairs) succeed.
- **Components:** `util_load.c`
- **Notes:** 4 pairs all referring to the same URI with alternating `block_allocation` values.

### `test_load_commandline.test_load_commandline_6`
- **What it tests:** Verifies that attempting to modify `key_format` or `value_format` during load fails with an error.
- **Components:** `util_load.c`
- **Notes:** Format changes are not permitted at load time.

### `test_load_commandline.test_load_commandline_7`
- **What it tests:** Verifies that `filename=`, `source=`, and `version=` config settings are stripped (not applied) during load; commands succeed even though these fields are filtered out.
- **Components:** `util_load.c`
- **Notes:** Confirms that internal metadata fields are silently dropped during load.
