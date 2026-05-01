# test_truncate11 — Checkpoint does not read fast-truncated pages not visible to checkpoint

**File:** `test/suite/test_truncate11.py`
**Storage mode:** General (skipped for tiered — test depends on regular checkpoints)
**Components under test:** fast delete, checkpoint, `cache_read_deleted` stat, concurrent checkpoint

## Test Cases

### `test_truncate11.test_truncate11`
- **What it tests:** Inserts 80,000 rows; forces to disk; updates a small set at ts=120; starts a background checkpoint thread with `checkpoint_slow` timing stress; waits for the checkpoint to begin, then during the checkpoint commits a truncation (keys 20,000-40,000) at ts=150; joins the checkpoint thread; verifies that `cache_read_deleted` is less than 10, confirming the checkpoint did not read the fast-deleted pages that were not visible to it.
- **Components:** `btree.c`, `checkpoint.c`, `evict.c`
- **Notes:** Parameterized over column and integer-row formats. Uses `checkpoint_slow` stress and a concurrent `checkpoint_thread` to create the race condition. Skipped on disagg if fast truncate not built.
