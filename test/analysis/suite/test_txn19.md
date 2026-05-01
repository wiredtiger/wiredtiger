# test_txn19 — Recovery with corrupted log files and metadata files

**File:** `test/suite/test_txn19.py`
**Storage mode:** General (skipped for disagg — corrupts log files not relevant to disagg)
**Components under test:** log corruption recovery, salvage, metadata file corruption

## Test Cases

### `test_txn19.test_corrupt_log`
- **What it tests:** Writes 10 or 11 large records (~60K each, 2 per log file); copies to RESTART; closes; applies one of 9 corruption types (removal, truncate, zero-begin, zero-trunc, zero-end, garbage-begin, garbage-middle, garbage-end, truncate-middle) to a specified log file, optionally to a second log file, with an optional checkpoint at a specific position; reopens with `recovery` (expects failure or warning depending on corruption type and checkpoint position); reopens with `salvage=true` and verifies the expected number of recovered records; inserts 2 more records and verifies data is correct across multiple reopen scenarios.
- **Components:** `log.c`, `recovery.c`, `salvage.c`
- **Notes:** Parameterized over integer-row/column × 9 corruption types × 9 position combinations × 2 record counts (pruned to ~20 / 1000). Contains detailed logic to predict whether corruption is detectable, recoverable without salvage, or requires salvage.

### `test_txn19_meta.test_corrupt_meta`
- **What it tests:** Creates 5 tables with 1,000 records each; copies to directories; applies one of 9 corruption types to one of 5 metadata files (WiredTiger, WiredTiger.basecfg, WiredTiger.turtle, WiredTiger.wt, WiredTigerHS.wt); verifies that `wiredtiger_open` without salvage either succeeds (for known-openable combinations) or fails; then tries `wiredtiger_open` with `salvage=true` from both copies, verifying either all 1,000 records are recoverable or the not-salvageable cases produce errors.
- **Components:** `meta.c`, `recovery.c`, `salvage.c`, `history_store.c`
- **Notes:** Parameterized over integer-row/column × 9 corruption types × 5 metadata files (90 scenarios). Contains explicit lists of openable and not-salvageable combinations. Corruption types: `WiredTiger.turtle` removal has a FIXME-WT-11995 anomaly noted.
