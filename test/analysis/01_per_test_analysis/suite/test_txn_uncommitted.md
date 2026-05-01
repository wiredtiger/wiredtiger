# test_txn_uncommitted — Stats for uncommitted transaction data: count and bytes

**File:** `test/suite/test_txn_uncommitted.py`
**Storage mode:** General
**Components under test:** `cache_updates_txn_uncommitted_count`, `cache_updates_txn_uncommitted_bytes`, `session.txn_updates`, `session.txn_bytes_dirty`

## Test Cases

### `test_txn_uncommitted.test_session_stats`
- **What it tests:** Runs five helper scenarios in sequence:
  1. **txn_one**: Single session inserts one 2KB entry; verifies conn stats increment on insert and reset to zero on commit; session stats match.
  2. **txn_two**: Two sessions insert concurrently; verifies conn stats accumulate per session; after session1 commits conn count drops by 1; after session2 rollback drops to 0; session stats for both sessions reset.
  3. **txn_two_seq**: Two sessions; session1 inserts and rolls back (conn stats drop to 0); session2 inserts and commits (stats drop to 0 again). Tests sequential rollback/commit ordering.
  4. **txn_many**: 20 sessions each insert one entry; verifies conn count increases by 1 per insert; after each session commits, conn count decreases by 1; all session stats reset to 0.
  5. **txn_many_many**: 20 sessions each insert 20 entries; verifies conn count equals n_sessions × n_updates; session stat `txn_updates` equals 20 per session; all reset on commit.
- **Components:** `txn.c`, `stat.c`, `session.c`
- **Notes:** No parameterization. Uses `statistics=(all)`, `key_format=i,value_format=S`. Entry value is 2KB (`"abcde" * 400`). Tests both connection-level and session-level uncommitted data stats across single/concurrent/sequential/bulk scenarios.
