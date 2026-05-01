# test_dump04 — wt dump utility: JSON output format and single-key filter combined

**File:** `test/suite/test_dump04.py`
**Storage mode:** General
**Components under test:** wt utility (dump), JSON format (-j), key filter (-k)

## Test Cases

### `test_dump04.test_dump`
- **What it tests:** Runs five combinations of dump flags and verifies output correctness:
  1. `-j` (JSON only): All 3 key-value pairs present; output is valid JSON (loaded with `json.load`).
  2. `-k key` (single key, no JSON): Only the record for key `"key"` is present; others absent.
  3. `-k table` (non-matching key, no JSON): No records in output (0 data lines).
  4. `-j -k 1` (JSON + matching key): Only the record for key `"1"` present in valid JSON.
  5. `-j -k table` (JSON + non-matching key): No records present but output is still valid JSON.
- **Components:** `src/utilities/util_dump.c`
- **Notes:** Uses `key_format=u,value_format=u`. Table is checkpointed before dump to push data to disk. Validates JSON correctness by calling `json.load()`. The helper `check_key_value` searches for key and value strings in the raw output file. Dataset: 3 records (`"key"="value"`, `"key0"="value0"`, `"1"="1"`).
