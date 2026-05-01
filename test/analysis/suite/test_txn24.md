# test_txn24 — Eviction thread snapshot isolation and oldest ID logging

**File:** `test/suite/test_txn24.py`
**Storage mode:** General
**Components under test:** eviction thread snapshot isolation, `capacity_bytes_evict`, oldest transaction ID pinning, verbose log

## Test Cases

### `test_txn24.test_snapshot_isolation_and_eviction`
- **What it tests:** Populates 480,000 rows (240MB); checkpoints; starts a long-running transaction with one update; then 3 additional sessions each update ~120,000 rows without committing; checks that `capacity_bytes_evict` has increased (eviction threads made progress despite the pinned long-running transaction, enabled by eviction threads taking their own snapshots); commits the long-running transaction.
- **Components:** `txn.c`, `evict.c`, `stat.c`
- **Notes:** Parameterized over integer-row and column formats. `rollbacks_allowed=0` to immediately fail on any unexpected rollback. Tests that eviction threads use snapshot isolation to evict pages beyond the oldest pinned transaction.

### `test_txn24.test_oldest_id_log`
- **What it tests:** Populates 480,000 rows; starts two long-running transactions (session1 and session2) each with one update; session3 performs ~480,000 individual commits; after session1 commits and session3 takes a checkpoint, verifies that a verbose "oldest id ... pinned in session" message appears (because session2 now becomes the new oldest pinned transaction and the system logs this event).
- **Components:** `txn.c`, `verbose.c`
- **Notes:** Tests the diagnostic logging for oldest pinned transaction ID transitions. The `ignoreStdoutPattern` call in `__init__` prevents false positives from background eviction messages.
