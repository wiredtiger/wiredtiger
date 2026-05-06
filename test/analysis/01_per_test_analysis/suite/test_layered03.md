# test_layered03 — Basic layered table cursor insert and read traversal

**File:** `test/suite/test_layered03.py`
**Storage mode:** Disagg/Layered
**Components under test:** layered cursor insert, cursor search, forward/backward cursor traversal, cur_layered.c

## Test Cases

### `test_layered03.test_layered03`
- **What it tests:** Creates a layered table, inserts three key/value pairs ("Hello"/"World", "Hi"/"There", "OK"/"Go"), performs a point-read via `cursor.search()`, then traverses all records forward (`cursor.next()`) and backward (`cursor.prev()`). After closing and reopening the cursor, performs another forward traversal to verify the inserted data is still accessible.
- **Components:** cursor insert and search (`cur_layered.c`), ingest btree, layered table manager
- **Notes:** Tests the most basic CRUD path for a layered table in ingest mode (no checkpoint). Covers both point-reads and full sequential scans (forward and backward). The final re-open of the cursor verifies that data remains accessible across cursor close/open within the same session. Would fail if cursor insert, search, or iteration is broken in the layered cursor implementation.
