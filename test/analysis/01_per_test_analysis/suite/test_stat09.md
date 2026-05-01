# test_stat09 — Oldest active read timestamp statistic

**File:** `test/suite/test_stat09.py`
**Storage mode:** General
**Components under test:** transaction statistics, oldest active read timestamp, timestamp pinning

## Test Cases

### `test_stat09.test_oldest_active_read`
- **What it tests:** Inserts 100 keys each committed at timestamp=key; opens multiple sessions with read_timestamps 10, 20, 30, 40, 50; validates that `'transaction: transaction read timestamp of the oldest active reader'` equals the minimum read timestamp; validates the pinned range stat; confirms the stat updates as sessions commit; verifies interaction with the `oldest_timestamp` setting; confirms the stat returns 0 when no readers are active.
- **Components:** `stat.c`, `txn.c`, `txn_timestamp.c`
- **Notes:** Tests timestamp ordering: oldest reader = min(active read timestamps), independent of oldest_timestamp. Advancing oldest_timestamp past or ahead of readers does not change which reader is tracked as oldest.
