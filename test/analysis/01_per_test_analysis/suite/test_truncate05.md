# test_truncate05 — Fast truncate rejected when read timestamp is older than newest update

**File:** `test/suite/test_truncate05.py`
**Storage mode:** General
**Components under test:** fast delete, timestamp ordering, `WT_ROLLBACK` on stale read

## Test Cases

### `test_truncate05.test_truncate_read_older_than_newest`
- **What it tests:** Inserts 1000 rows at ts=2, forces to disk, updates key 500 at ts=3, then begins a transaction with read_timestamp=2 and attempts to truncate keys 1-1000; expects `WiredTigerError` because the transaction's read timestamp is older than the newest update in the range.
- **Components:** `txn_timestamp.c`, `btree.c`, `cursor.c`
- **Notes:** Parameterized over column and integer-row formats. Skipped on disagg if fast truncate support is not built. Tests the write-conflict detection that prevents a truncate from succeeding when a newer committed update exists.
