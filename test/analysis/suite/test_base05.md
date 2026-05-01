# test_base05 — Unicode and non-ASCII string storage and retrieval

**File:** `test/suite/test_base05.py`
**Storage mode:** General
**Components under test:** cursor API, string encoding (UTF-8/Unicode), btree key comparator

## Test Cases

### `test_base05.test_table_ss`
- **What it tests:** Inserts 1000 mixed-language string key/value pairs (drawn from English excerpts and Unicode "Hello" strings in Chinese, Arabic, Hebrew, Japanese, Korean, Russian, Georgian) built by the `mixed_string()` function. Spot-checks 3 specific entries via `cursor.search()`. Iterates all entries, verifying each key and value, and confirms all 1000 unique keys were stored.
- **Components:** `src/cursor/cur_table.c`, `src/btree/bt_compare.c`

### `test_base05.test_table_string`
- **What it tests:** Inserts all non-English Unicode strings (as raw Python string objects) and reads them back, verifying exact key/value match. Tests non-ASCII byte storage.
- **Components:** `src/cursor/cur_table.c`

### `test_base05.test_table_unicode`
- **What it tests:** Same as `test_table_string` but converts each string to a `str` object (explicit unicode) before inserting.
- **Components:** `src/cursor/cur_table.c`
- **Notes:** Non-parametrized. Uses `key_format=S,value_format=S`. The `mixed_string()` method uses 8 non-English Unicode strings and ~50 English sentences from Moby Dick.
