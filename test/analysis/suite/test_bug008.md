# test_bug008 — cursor.search and cursor.search_near with invisible records and end-of-table positions

**File:** `test/suite/test_bug008.py`
**Storage mode:** General
**Components under test:** cursor search, cursor search_near, MVCC visibility, column-store duplicates

## Test Cases

### `test_bug008.test_search_empty`
- **What it tests:** In an empty table, search and search_near for a key past the end return `WT_NOTFOUND`.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_search.c`

### `test_bug008.test_search_eot`
- **What it tests:** In an on-disk table of 100 records: search at the last record succeeds; search_near at the last record returns 0; search past the end returns `WT_NOTFOUND`; search_near past the end returns -1 and positions at the last record.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_search.c`

### `test_bug008.test_search_duplicate`
- **What it tests:** Column-store only. Creates a range of duplicate values (records 20–99 identical), deletes a band before and after the duplicates, then verifies: search_near before the deleted band → returns +1 landing at first duplicate; search_near after the deleted band → returns -1 landing at last duplicate.
- **Components:** `src/cursor/cur_std.c`, `src/btree/col_srch.c`

### `test_bug008.test_search_invisible_one`
- **What it tests:** With on-disk records and in-progress inserts that are invisible to a second session: search for deleted records returns `WT_NOTFOUND`; search for invisible added records returns `WT_NOTFOUND`; search_near for deleted records finds next visible record (+1); search_near for invisible added records finds previous visible record (-1).
- **Components:** `src/cursor/cur_std.c`, `src/txn/txn_api.c`, `src/btree/bt_search.c`

### `test_bug008.test_search_invisible_two`
- **What it tests:** On-disk records, additional visible records in insert list, and additional invisible records in insert list. Search for invisible record fails; search_near finds the last visible record at key 119.
- **Components:** `src/cursor/cur_std.c`, `src/btree/bt_search.c`
- **Notes:** Parametrized across `row` (S) and `var` (r) formats. All tests are btree-layer only (file: URI).
