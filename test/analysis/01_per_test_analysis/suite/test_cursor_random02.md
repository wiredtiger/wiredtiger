# test_cursor_random02 — next_random distribution quality across various record counts

**File:** `test/suite/test_cursor_random02.py`
**Storage mode:** General
**Components under test:** cursor next_random, random distribution quality

## Test Cases

### `test_cursor_random02.test_cursor_random_reasonable_distribution`
- **What it tests:** Inserts N records (1, 250, 500, 5000, 10000, 50000) into a single large page table (`leaf_page_max=100MB`) and calls `cursor.next()` N times. Tracks unique keys seen and sequential key pairs. Verifies: (1) more than 25% of distinct keys are visited (distribution is reasonably uniform); (2) the number of sequential key pairs is less than N-1 (cursor is not returning purely sequential data).
- **Components:** `src/cursor/cur_std.c`, `src/btree/`
- **Notes:** Table URI only, `next_random=true` without sample size. Large leaf page prevents page splits. The single-record case (N=1) is excluded from the non-sequential check. `leaf_page_max=100MB` keeps all data on one page to test the insert-list random path specifically.
