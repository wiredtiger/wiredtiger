# test_bug033 — WT-12096: obsolete updates on update chain after rollback_to_stable

**File:** `test/suite/test_bug033.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, eviction, obsolete update pruning, concurrent checkpoint

## Test Cases

### `test_bug033.test_bug033`
- **What it tests:** Reproduces WT-12096 where inserting a new update after `rollback_to_stable` while obsolete updates exist on the update chain, then evicting concurrently with a checkpoint, could produce incorrect results. Sequence: (1) insert 'b'@ts=2 and 'c'@ts=4 (in-memory only); (2) force-evict (DS now has 'c'@ts=4); (3) `rollback_to_stable` with stable=1 (adds a tombstone in memory); (4) insert 'd'@ts=2 (update chain: tombstone → 'd'@2); (5) advance oldest/stable to ts=3 (both updates become obsolete); (6) sleep 1 s to allow the oldest ID to advance and pruning to remove the tombstone; (7) insert 'e'@ts=4 (update chain should now be 'e'@4 atop a pruned obsolete 'd'@2, but DS still has 'c'@4); (8) start a background checkpoint thread with `checkpoint_slow` timing stress; (9) once checkpoint starts, force-evict. Test passes if no crash or assertion failure occurs.
- **Components:** `src/txn/txn_rollback.c`, `src/eviction/eviction.c`, `src/checkpoint/checkpoint.c`
- **Notes:** Non-parametrized. `statistics=(all)` and `timing_stress_for_test=[checkpoint_slow]`. Uses a `wtthread.checkpoint_thread` and polls `stat.conn.checkpoint_state`.
