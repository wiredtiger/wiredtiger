# test_truncate16 — Reading from a fast-truncated page of a prepared transaction returns WT_PREPARE_CONFLICT

**File:** `test/suite/test_truncate16.py`
**Storage mode:** General
**Components under test:** fast delete, prepared transactions, `WT_PREPARE_CONFLICT`, page instantiation

## Test Cases

### `test_truncate16.test_truncate16`
- **What it tests:** Writes 10,000 rows at ts=10; reopens; in a separate session, truncates the middle half and prepares at ts=20 (leaving it hanging); optionally checkpoints; reads key nrows//2 from the truncated range at read_ts=30 and confirms `WT_PREPARE_CONFLICT`; checks that exactly one deleted page was instantiated; rolls back the prepared transaction; reads the full table and confirms all rows are present (no pages instantiated by rollback).
- **Components:** `btree.c`, `txn.c`, `txn_timestamp.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × checkpoint. Tests that when a key in a fast-deleted page belongs to a prepared transaction, a reader gets `WT_PREPARE_CONFLICT`. Also validates that transaction rollback does not trigger additional page instantiations.
