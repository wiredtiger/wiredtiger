# test_truncate24 — Commit timestamp not overwritten when reinstantiated deletes are committed

**File:** `test/suite/test_truncate24.py`
**Storage mode:** General (skipped for tiered)
**Components under test:** fast delete, page instantiation during active truncate, commit timestamp preservation

## Test Cases

### `test_truncate24.test_truncate24`
- **What it tests:** Inserts 100,000 rows without timestamps; reopens; begins a transaction and optionally sets commit_timestamp=10; truncates the entire table (URI truncate); while the truncate transaction is open, a second session scans the table with `cursor.next()` to reload the deleted pages into memory; then commits the truncation at ts=20; verifies that at ts=10 keys are not found (if ts was set) or still present (if not set), confirming the commit timestamp on fast-deleted pages was not overwritten when pages were instantiated during an active truncation.
- **Components:** `btree.c`, `txn.c`, `txn_timestamp.c`
- **Notes:** Parameterized over row (key_format='i') and var (key_format='r') × set_ts/not_set_ts. Tests that reinstantiating pages during an uncommitted truncate does not corrupt the pending commit timestamp.
