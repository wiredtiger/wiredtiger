# test_truncate20 — Oplog workload with checkpoint cleanup: disk space stays bounded under long-running readers

**File:** `test/suite/test_truncate20.py`
**Storage mode:** General
**Components under test:** fast delete, checkpoint cleanup (cc), disk space reclamation, logging, eviction

## Test Cases

### `test_truncate20.test_truncate`
- **What it tests:** Mimics a MongoDB oplog workload with logging enabled: inserts 1,000,000 rows with eviction; for 50 iterations, starts a long-running transaction, fast-truncates 10,000 rows, appends 10,000 new rows, evicts new rows, waits for checkpoint cleanup to run, verifies oplog file is under 600MB, then rolls back the long-running transaction. Skipped on disagg if fast truncate not built.
- **Components:** `btree.c`, `checkpoint.c`, `block.c`, `log.c`, `evict.c`
- **Notes:** Parameterized over column, integer-row, and string-row formats. Tagged `@wttest.longtest`. Extends `test_cc_base` (from test_cc01) to use checkpoint cleanup for page reclamation. Tests that checkpoint cleanup reclaims fast-deleted pages even when a long-running reader pins the data.
