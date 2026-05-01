# test_truncate27 — Unstable updates preceding stable fast truncate restored with correct transaction IDs after crash

**File:** `test/suite/test_truncate27.py`
**Storage mode:** General
**Components under test:** fast delete, crash recovery, RTS, transaction ID, unstable updates

## Test Cases

### `test_truncate27.test_truncate27`
- **What it tests:** Inserts 100,000 rows at per-row timestamps; updates stable to nrows and checkpoints; evicts everything; fast-truncates keys nrows//2 onward at ts=nrows+1; advances stable to ts+1; inserts an unstable update at ts+2 to key 100; checkpoints; simulates crash restart. The test verifies that RTS correctly restores unstable updates preceding the stable fast truncation with correct transaction IDs (regression test for corruption when write generation was mishandled).
- **Components:** `btree.c`, `rts.c`, `recovery.c`, `txn.c`
- **Notes:** Column format (key_format='r') only. Uses `simulate_crash_restart` to test post-recovery state. The primary assertion is that the crash restart completes without error or corruption.
