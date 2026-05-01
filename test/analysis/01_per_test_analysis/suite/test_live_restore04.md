# test_live_restore04 — wt utility usage with live restore

**File:** `test/suite/test_live_restore04.py`
**Storage mode:** General with logging (`log=(enabled)`); Unix only
**Components under test:** live restore, wt utility (dump, verify, printlog), logging

## Test Cases

### `test_live_restore04.test_live_restore04`
- **What it tests:** Verifies that the `wt` utility commands (`dump`, `verify`, `printlog`) work correctly when a live restore is in an unfinished state. Checks that opening without the proper live restore path fails, and that using `-l SOURCE` provides the correct source path so data can be read.
- **Components:** `src/live_restore/`, `src/utilities/util_dump.c`, `src/utilities/util_verify.c`, `src/utilities/util_printlog.c`, `src/log/`
- **Notes:** Parameterized by key format: `column` (`key_format='r'`) or `row_integer` (`key_format='i'`). 3 files, 10000 rows each. Steps:
  1. Populate 3 `file:collection-{i}` files and take reference dumps via `wt dump -x`.
  2. Back up to SOURCE, close, open live restore connection with `threads_max=0` (leaves migration incomplete), close.
  3. Run `wt dump -x` without `-l SOURCE` → expect failure (checks `wterr.txt` is non-empty).
  4. Run `wt -l SOURCE printlog` → expect success and non-empty output.
  5. For each file: run `wt -l SOURCE dump -x` and compare byte-for-byte against the reference dump. Run `wt -l SOURCE verify` and expect success.
