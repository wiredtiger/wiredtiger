# test_base03 — Cursor iteration over all key/value format combinations

**File:** `test/suite/test_base03.py`
**Storage mode:** General
**Components under test:** cursor API (insert, iterate), schema (table creation)

## Test Cases

### `test_base03.test_table_ss`
- **What it tests:** Creates a table with `key_format=S,value_format=S`, inserts 10 string/string pairs, resets the cursor, and iterates verifying each key and value in order.
- **Components:** `src/cursor/cur_table.c`, `src/btree/bt_walk.c`

### `test_base03.test_table_si`
- **What it tests:** Creates a table with `key_format=S,value_format=i`, inserts 10 string/int pairs, and verifies iteration.
- **Components:** `src/cursor/cur_table.c`

### `test_base03.test_table_is`
- **What it tests:** Creates a table with `key_format=i,value_format=S`, inserts 10 int/string pairs, and verifies iteration.
- **Components:** `src/cursor/cur_table.c`

### `test_base03.test_table_ii`
- **What it tests:** Creates a table with `key_format=i,value_format=i`, inserts 10 int/int pairs, and verifies iteration.
- **Components:** `src/cursor/cur_table.c`
- **Notes:** Non-parametrized. Uses 4 separate tables, one per format pair.
