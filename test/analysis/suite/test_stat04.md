# test_stat04 — Statistics key/value pair count accuracy

**File:** `test/suite/test_stat04.py`
**Storage mode:** General
**Components under test:** statistics cursor (`btree_entries`), btree insert/remove

## Test Cases

### `test_stat04.test_stat_nentries`
- **What it tests:** Periodically checks `btree_entries` against the actual running count while inserting records; verifies the count decrements correctly as records are removed one by one; after reopening the connection confirms the count persists correctly from disk.
- **Components:** `stat.c`, `btree`, `block_mgr.c`
- **Notes:** Parameterized over column/row key formats and four size scenarios: small (100 entries, 50-byte values), medium (10,000 / 20 bytes), large (100,000 / 1 byte), jumboval (100 entries / 4.2 MB values — exercises overflow pages). Removal pattern uses prime-step modular indexing to avoid aliasing.
