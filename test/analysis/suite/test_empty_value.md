# test_empty_value — Row-store zero-length value storage optimization

**File:** `test/suite/test_empty_value.py`
**Storage mode:** General
**Components under test:** btree (row-store), statistics

## Test Cases

### `test_row_store_empty_values.test_row_store_empty_values`
- **What it tests:** Inserts 25,000 records with zero-length (`b''`) byte values into a `file:` (btree-layer) row-store table, then reopens the connection to force everything to disk, and asserts via the `btree_row_empty_values` statistic that none of the empty values were stored on disk (the optimization that omits zero-length value cells).
- **Components:** `src/btree/`, `src/cell/`, `src/stat/`
- **Notes:** Uses `key_format=S,value_format=u` (raw bytes). Checks `stat.dsrc.btree_row_empty_values` equals `nentries` (25,000). Connection opened with `statistics=(all)`.
