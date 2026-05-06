# test_truncate22 — Setting commit timestamp before fast truncate

**File:** `test/suite/test_truncate22.py`
**Storage mode:** General
**Components under test:** fast delete, `timestamp_transaction` before truncate, commit ordering

## Test Cases

### `test_truncate22.test_truncate22`
- **What it tests:** Inserts 10,000 rows at ts=2; reopens; begins a transaction and sets commit_timestamp=5 via `timestamp_transaction` before calling truncate (rather than at commit time); truncates the first half of the table; commits; advances stable to 10 and checkpoints; verifies that keys 1 through nrows//2 are not found.
- **Components:** `btree.c`, `txn_timestamp.c`, `txn.c`
- **Notes:** Parameterized over column and integer-row formats. Skipped on disagg if fast truncate not built. Tests that commit timestamp set before (rather than at) the truncate call is correctly applied to the fast-delete operation.
