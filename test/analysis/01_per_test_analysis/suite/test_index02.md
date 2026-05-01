# test_index02 — search_near in index cursors

**File:** `test/suite/test_index02.py`
**Storage mode:** General
**Components under test:** schema/index, cursor/search_near, btree

## Test Cases

### `test_index02.test_search_near_exists`
- **What it tests:** Verifies that `search_near` on an index cursor finds an exact match (returns 0) when the key exists. Also retests after reopening the connection to ensure durability.
- **Components:** `src/cursor/cur_index.c`, `src/btree/bt_cursor.c`
- **Notes:** Parameterized by:
  - `index` — index on value column only (`columns=(v)`, `ncol=1`)
  - `index-with-key` — index includes primary key as tiebreaker (`columns=(v,k)`, `ncol=2`)

  Inserts values 1, 5, 5, 5, 10 (with recno keys). Searches for value 5; expects exact match. The loop runs twice (before and after `reopen_conn()`).

### `test_index02.test_search_near_between`
- **What it tests:** Verifies that `search_near` returns the correct adjacent key and exact comparison value (`-1`, `0`, `+1`) when searching for values that may or may not exist in the index.
- **Components:** `src/cursor/cur_index.c`, `src/btree/bt_cursor.c`
- **Notes:** Table has integer keys 0-2 mapped to values 10, 15, 20. Searches for keys 1, 11, 15, 19, 21 and checks that `exact` equals `cmp(found_key, search_key)`. Also runs twice (with `reopen_conn()`). For `ncol=2`, searching `(15, 1)` produces a full match.

### `test_index02.test_search_near_empty`
- **What it tests:** Verifies that `search_near` on an empty index returns `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_index.c`, `src/btree/bt_cursor.c`
- **Notes:** Both `index` and `index-with-key` variants. No data is inserted before the search.
