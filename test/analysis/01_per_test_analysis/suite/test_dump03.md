# test_dump03 — wt dump utility: windowed context dump around a key

**File:** `test/suite/test_dump03.py`
**Storage mode:** General
**Components under test:** wt utility (dump), window option (-w)

## Test Cases

### `test_dump.test_window`
- **What it tests:** Validates the `-w <size>` (window) dump option, which dumps a key and `size` neighboring records on each side. Parameterized via `make_scenarios` across six cases covering boundary conditions:
  - `window-size-1` at key2: 3 records (key1, key2, key3) = 6 lines.
  - `window-size-1-at-start` at key1: 2 records (key1, key2) = 4 lines (no key0).
  - `window-size-1-at-end` at key99: 2 records (key98, key99) = 4 lines.
  - `window-size-2` at key3: 5 records = 10 lines.
  - `window-size-3` at key5: 7 records = 14 lines.
  - `window-size-0` at key61: 1 record (just the key itself) = 2 lines.
- **Components:** `src/utilities/util_dump.c`
- **Notes:** 99 rows with string keys `key1`..`key99`. All scenarios use lexicographic ordering for key lookup. Tags: `wt_util`.
