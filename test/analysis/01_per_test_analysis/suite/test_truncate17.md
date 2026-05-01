# test_truncate17 — Stats cursor on fast-truncated prepared pages instantiates all deleted pages

**File:** `test/suite/test_truncate17.py`
**Storage mode:** General
**Components under test:** fast delete, prepared transactions, stats cursor, `btree_entries`, page instantiation

## Test Cases

### `test_truncate17.test_truncate17`
- **What it tests:** Writes 10,000 rows at ts=10; reopens twice (first to stat the on-disk tree baseline, then to start clean); in a separate session, truncates the middle half and prepares at ts=20; optionally checkpoints; opens a stats cursor on the table URI and checks `btree_entries` sees half the rows (read-uncommitted semantics) while page count remains the same (pages not freed yet); checks that `cache_read_deleted` equals the number of fast-deleted pages (stats cursor instantiated them all); rolls back the prepared transaction; verifies that rollback did not increase the page instantiation count.
- **Components:** `btree.c`, `txn.c`, `stat.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × checkpoint. Tests that the stats cursor causes instantiation of all prepared fast-deleted pages and that btree_entries correctly reflects read-uncommitted prepared state.
