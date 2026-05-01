# test_truncate07 — Truncate returns WT_ROLLBACK when encountering prepared values

**File:** `test/suite/test_truncate07.py`
**Storage mode:** General
**Components under test:** fast delete, prepared transactions, `WT_ROLLBACK` on prepare conflict

## Test Cases

### `test_truncate07.test_truncate07`
- **What it tests:** Writes 10,000 rows at ts=10; then prepares (but does not commit) updates or removes on every other even-numbered key in the middle third at ts=20 in a separate session; optionally evicts pages and checkpoints; then attempts to truncate the middle half of the table and expects `WT_ROLLBACK` because prepared data lies in the range.
- **Components:** `btree.c`, `txn.c`, `txn_timestamp.c`, `evict.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × update/remove × eviction × checkpoint. Tests that truncate correctly detects and rejects ranges containing prepared-but-uncommitted data. Stable timestamp is advanced to 50 before exit to avoid unnecessary RTS work.
