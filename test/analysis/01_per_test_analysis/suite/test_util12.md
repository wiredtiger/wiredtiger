# test_util12 — wt write CLI: insert, overwrite, and remove records

**File:** `test/suite/test_util12.py`
**Storage mode:** General
**Components under test:** `wt write`, cursor insert/remove, overwrite semantics

## Test Cases

### `test_util12.test_write`
- **What it tests:** Runs `wt write table:<name> def 456 abc 123`; verifies that both key/value pairs are inserted and can be read back via cursor in sorted order (abc/123, def/456).
- **Components:** `util_write.c`, `cursor.c`
- **Notes:** No parameterization. Basic insert without overwrite flag.

### `test_util12.test_write_overwrite`
- **What it tests:** Pre-inserts `def=789`; runs `wt write` without `-o` to insert `def=456` — expects failure with "attempt to insert an existing key"; then runs with `-o` and verifies both `abc=123` and `def=456` are written correctly.
- **Components:** `util_write.c`, `cursor.c`
- **Notes:** Tests that duplicate key insertion fails without `-o` and succeeds with `-o`.

### `test_util12.test_write_remove`
- **What it tests:** Inserts two records; attempts to remove a non-existent key (`efg`) — expects failure with "item not found"; attempts to remove two keys at once — expects failure with "usage:"; successfully removes one existing key (`def`); verifies only `abc=123` remains.
- **Components:** `util_write.c`, `cursor.c`
- **Notes:** Tests `-r` (remove) flag: only one key allowed, key must exist.

### `test_util12.test_write_no_keys`
- **What it tests:** Runs `wt write table:<name>` with no key/value arguments; verifies failure with "usage:" error.
- **Components:** `util_write.c`
- **Notes:** Argument validation: at least one key/value pair required.

### `test_util12.test_write_bad_args`
- **What it tests:** Runs `wt write` with an odd number of key/value arguments (missing the value for the second key); verifies failure with "usage:" error.
- **Components:** `util_write.c`
- **Notes:** Argument validation: keys and values must be paired.
