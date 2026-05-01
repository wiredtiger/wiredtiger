# test_dump05 — wt dump utility: JSON format integrity with random-length string/byte values

**File:** `test/suite/test_dump05.py`
**Storage mode:** General
**Components under test:** wt utility (dump), JSON format (-j), string encoding

## Test Cases

### `test_dump05.test_dump_string`
- **What it tests:** Validates that JSON dump output (`-j`) for a string-format table (`key_format=S,value_format=S`) contains properly quoted key-value pairs with no extra quotes (no junk after the closing quote) and at least one valid record in the expected `"key0" : "value0"` JSON form. Uses regex-based `check_file_contains`/`check_file_not_contains` assertions.
- **Components:** `src/utilities/util_dump.c`
- **Notes:** 1000 records with random-length suffixes (`n` repetitions of `_NNN`) to make each record identifiable and avoid false-positive matches.

### `test_dump05.test_dump_bytes`
- **What it tests:** Same validation as `test_dump_string` but for byte-array format (`key_format=u,value_format=u`), ensuring the JSON encoder correctly handles raw byte values without producing malformed JSON (e.g., unterminated or extra quotes).
- **Components:** `src/utilities/util_dump.c`
- **Notes:** Byte-array format requires URL-encoding or similar escaping in JSON output; the test checks that no double-quote corruption occurs. Random seed is not fixed, so record content varies per run.
