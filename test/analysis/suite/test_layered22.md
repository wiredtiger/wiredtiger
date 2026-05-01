# test_layered22 — Secondary (follower) operations without stable component

**File:** `test/suite/test_layered22.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** follower reads and writes on ingest-only layered table (no stable component / no checkpoint), cursor next/prev, cursor search/search_near, cursor modify, cursor largest_key, random cursor, cur_layered.c

## Test Cases

### `test_layered22.test_secondary_reads_without_stable`
- **What it tests:** On a follower with no checkpoint (pure ingest state), inserts 10,000 * 3 = 30,000 records. Then does a full forward scan counting records, asserting 30,000. Repeats with `cursor.prev()` (backward scan), asserting the same count.
- **Components:** ingest btree insert, forward and backward cursor iteration without stable btree (`cur_layered.c`)
- **Notes:** Verifies that forward and backward full scans work correctly when there is no stable component at all.

### `test_layered22.test_secondary_modifies_without_stable`
- **What it tests:** Inserts 10,000 records at timestamp 10. Applies `cursor.modify()` (single-byte in-place change) to every 10th record at timestamp 20. Reads back all records and verifies modified keys have the new value, unmodified keys have the original.
- **Components:** cursor modify on ingest-only layered table, modify resolution without stable btree

### `test_layered22.test_secondary_search_without_stable`
- **What it tests:** On a fresh follower with no data, calls `cursor.search("nonexistent")` and `cursor.search_near()` — both must return `WT_NOTFOUND`. Then inserts key "found" and retries both; both must succeed (return 0).
- **Components:** cursor search and search_near on empty and non-empty ingest-only table

### `test_layered22.test_largest_key_without_stable`
- **What it tests:** Inserts 10,000 * 3 records (keys "Hello N", "Hi N", "OK N"). Calls `cursor.largest_key()` and verifies it returns the lexicographically largest key, which is "OK 9999".
- **Components:** `cursor.largest_key()` on ingest-only layered table (`cur_layered.c`)
- **Notes:** Tests that `largest_key` is correctly determined from the ingest btree alone with no stable component.

### `test_layered22.test_getrandom_without_stable`
- **What it tests:** Inserts 10,000 records (keys "Hello N"). Opens a `next_random=true` cursor and asserts `cursor.next()` returns 0 and the key starts with "Hello ".
- **Components:** random cursor on ingest-only layered table
- **Notes:** Verifies random cursor works without any stable data. Complements test_layered14 which also tests the stable case.
