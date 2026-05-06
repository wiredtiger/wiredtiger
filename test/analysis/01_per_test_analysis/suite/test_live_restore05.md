# test_live_restore05 — Live restore does not produce duplicate metadata entries

**File:** `test/suite/test_live_restore05.py`
**Storage mode:** General with logging; Unix only
**Components under test:** live restore, metadata integrity, wt utility (dump)

## Test Cases

### `test_live_restore05.test_live_restore05`
- **What it tests:** Reproduces a bug where live restore could write duplicate `live_restore=` entries into the `WiredTiger.wt` metadata file. Opens an incomplete live restore connection (`threads_max=0`), dumps `file:WiredTiger.wt` via the utility, and asserts no line contains `live_restore=` more than once.
- **Components:** `src/live_restore/`, `src/meta/`, `src/utilities/util_dump.c`
- **Notes:** Parameterized by key format: `row_integer` or `column_store`. 1 table, 10 rows. The check parses each line for `live_restore=` and looks for a second occurrence after the first — if found, the assertion fails. This specifically validates that the WiredTiger.wt turtle/metadata does not accumulate duplicate live restore configuration strings across repeated open/close cycles.
