# test_truncate25 — History store records correctly removed on no-timestamp truncate

**File:** `test/suite/test_truncate25.py`
**Storage mode:** General
**Components under test:** truncate with `no_timestamp=true`, history store cleanup, fast delete check

## Test Cases

### `test_truncate25.test_truncate25`
- **What it tests:** Inserts 10,000 rows at ts=30 and ts=50; checkpoints; reopens; performs a no-timestamp truncation (`no_timestamp=true`) on keys 5,000-8,000; verifies fast-delete did not happen (data not globally visible); re-inserts all rows at ts=60; checkpoints; reopens; reads at ts=30 and verifies keys 5,000-8,000 return `WT_NOTFOUND`, confirming the history store entries for the no-timestamp truncated range were correctly removed.
- **Components:** `btree.c`, `history_store.c`, `txn.c`, `checkpoint.c`
- **Notes:** Integer-row format only. Tests that history store records are purged when a no-timestamp truncate covers keys that previously had timestamped history.
