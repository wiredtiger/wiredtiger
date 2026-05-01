# test_truncate06 — Timestamped truncate of data with older history: conflict and success

**File:** `test/suite/test_truncate06.py`
**Storage mode:** General
**Components under test:** fast delete, write conflict (`WT_ROLLBACK`), eviction, checkpoint, timestamp ordering

## Test Cases

### `test_truncate06.test_truncate06`
- **What it tests:** Writes 10,000 rows at ts=10 (value_a); then updates or removes every other even-numbered key in the middle third at ts=20; optionally evicts pages (enabling fast delete) and optionally checkpoints; then truncates a range at either ts=15 (conflicts with the ts=20 changes and expects `WT_ROLLBACK`) or ts=25 (does not conflict, expects success).
- **Components:** `btree.c`, `cursor.c`, `txn_timestamp.c`, `evict.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × update/remove munging × eviction × checkpoint × conflicting/nonconflicting truncate time (16 scenarios per format). Tests that fast-truncate respects timestamp ordering and correctly returns `WT_ROLLBACK` when the truncate timestamp is older than existing updates.
