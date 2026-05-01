# test_search_near01 — search_near with key past end of table (no timestamp)

**File:** `test/suite/test_search_near01.py`
**Storage mode:** General
**Components under test:** cursor search_near, implicit record cursor insert, column store

## Test Cases

### `test_search_near01.test_implicit_record_cursor_insert_next`
- **What it tests:** Verifies that `cursor.search_near()` correctly handles searching for a key past the last existing key in the table. Tests two sub-scenarios: (1) the last key is updated (value changed): `search_near` should find it with `WT_NOTFOUND` or exact match behavior; (2) the last key is deleted: `search_near` past the last key should position before the deleted key or return `WT_NOTFOUND`. No timestamps involved.
- **Components:** `src/cursor/cur_std.c`, `src/btree/`, `src/col/`
- **Notes:** Parametrized on key format (recno column store and integer row store). Tests the boundary behavior of `search_near` at the end of a table, including the implicit-record-insertion path for column store.
