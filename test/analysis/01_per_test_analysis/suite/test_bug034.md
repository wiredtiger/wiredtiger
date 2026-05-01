# test_bug034 — WT-12602: incorrect EBUSY when evicting page with modify and globally visible HS tombstone

**File:** `test/suite/test_bug034.py`
**Storage mode:** General
**Components under test:** history store, eviction, modify, globally visible tombstone, dirty eviction

## Test Cases

### `test_bug034.test_non_ts`
- **What it tests:** Exercises the non-timestamped variant of WT-12602. Populates 99 rows, checkpoints to disk. Adds tombstones for all rows (uncommitted, so oldest ID doesn't advance). Keeps a second long-running transaction open to pin the oldest ID. Adds an update and a `Modify` on each row (update chain: modify → update → tombstone). Adds another update on each row (so update, modify, tombstone all go to HS). Checkpoints. Performs dirty eviction by inserting another update per row and forcing eviction via `debug=(release_evict)` cursor. Commits the long-running transaction. Test passes if no spurious EBUSY is returned.
- **Components:** `src/history/hs_cursor.c`, `src/eviction/eviction.c`, `src/reconcile/rec_write.c`

### `test_bug034.test_ts`
- **What it tests:** Timestamped variant of the same bug. Inserts at ts=5, checkpoints. Adds a globally visible tombstone (`no_timestamp=true`), advances oldest to ts=7. Adds update and modify at ts=8. Adds another update at ts=10 (sending update/modify/tombstone to HS). Checkpoints. Adds dirty updates at ts=11 and force-evicts. Test passes if no spurious EBUSY is returned.
- **Components:** `src/history/hs_cursor.c`, `src/eviction/eviction.c`

**Notes:** Non-parametrized (both methods in same class). `debug_mode=(eviction_checkpoint_ts_ordering=true)` simulates parallel-checkpoint eviction behavior.
