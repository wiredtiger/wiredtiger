# test_compact16 — Compact and checkpoint running concurrently; space reclaimed after compact

**File:** `test/suite/test_compact16.py`
**Storage mode:** General (skips tiered)
**Components under test:** compaction subsystem, checkpoint concurrency, block manager

## Test Cases

### `test_compact16.test_compact16`
- **What it tests:** Verifies that foreground compaction reclaims space even when a concurrent checkpoint thread is continuously running checkpoints. After compaction, the available space in the file must be less than 20% of total file size.
- **Components:** `src/session/session_compact.c`, `src/block/block_compact.c`, `src/checkpoint/`
- **Notes:** Skip: tiered. Populates 1 000 000 rows (large table), deletes the first 25%, reopens connection to force everything to disk. Starts a `checkpoint_thread` from `wtthread`. Waits for checkpoint to be running (`checkpoint_state != 0`), then calls `session.compact()`. After compact and checkpoint thread join, asserts `bytes_avail_for_reuse / file_size < 20%`. Tests that compact correctly handles the interplay with concurrent checkpoints (compact respects checkpoint boundaries).
