# test_cursor_bound05 — Cursor bound with prefix-style string keys

**File:** `test/suite/test_cursor_bound05.py`
**Storage mode:** General
**Components under test:** cursor bound API, prefix bounds, string key ordering

## Test Cases

### `test_cursor_bound05.test_bound_special_scenario`
- **What it tests:** Uses a key range of 1000–1999 formatted as strings (prefix-style ordering) and sets a lower bound that is a prefix of all keys. Verifies that next/prev traversal correctly scopes to all keys matching the prefix (i.e., keys 1000–1999 all start with "1" and are above a lower bound of "1000"). Exercises prefix semantics where the lower bound is a prefix of the actual keys.
- **Components:** `src/cursor/cur_bound.c`, `src/cursor/cur_std.c`, `src/btree/bt_cursor.c`
- **Notes:** Scenarios: file/table × evict/no-evict. Key format: string (`key_format=S`). Key range: 1000–1999 as strings. Tests the case where string ordering makes lower bound a prefix of all valid keys.
