# test_truncate19 — Oplog workload: fast-truncated pages cleaned up, disk size stays bounded

**File:** `test/suite/test_truncate19.py`
**Storage mode:** General (skipped for tiered and disagg — test depends on file sizes)
**Components under test:** fast delete, checkpoint, disk space reclamation, oplog simulation

## Test Cases

### `test_truncate19.test_truncate19`
- **What it tests:** Mimics a MongoDB oplog workload: inserts 1,000,000 rows; reopens; for 50 iterations, starts a long-running transaction (pinning oldest), fast-truncates 10,000 rows from the tail, checkpoints, verifies the oplog file is under 600MB, rolls back the long-running transaction, then appends 10,000 new rows. Checks that fast-delete pages are properly reclaimed on disk even when a long-running reader pins the data.
- **Components:** `btree.c`, `checkpoint.c`, `block.c`
- **Notes:** String-row format only. Uses a long-running transaction in session3 to make the truncation not globally visible at checkpoint time. The disk size bound (600MB) validates that old deleted pages are reclaimed.
