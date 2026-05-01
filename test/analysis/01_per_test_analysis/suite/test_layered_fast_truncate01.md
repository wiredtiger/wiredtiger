# test_layered_fast_truncate01 — Basic fast truncate functionality on layered and table URIs

**File:** `test/suite/test_layered_fast_truncate01.py`
**Storage mode:** Disagg/Layered (disagg_only). Skipped at runtime if `wiredtiger.disagg_fast_truncate_build() == 0`.
**Components under test:** `session.truncate()` on layered and table URIs with disagg block manager, commit/rollback of truncate ranges, write conflict detection between concurrent truncate and update operations

## Test Cases

All tests follow the same setup: leader populates 1000 keys (each value `"a" * 100`), checkpoints, then reopens as a follower (`disaggregated=(role="follower")`). Truncate operations run on the follower. Parametrized by URI (`layered:` vs. `table:` with `block_manager=disagg,type=layered`) × disagg storage variant.

### `test_layered_fast_truncate01.test_truncate_basic`
- **What it tests:** Leader writes 1000 keys and checkpoints. Follower opens, positions two cursors (c1=key "100", c2=key "700"), starts an uncommitted truncate transaction. While truncate is active but not committed, a second session reads key "150" inside a transaction and must find it (returns 0). After the first session commits the truncate, the second session reads key "150" again and must get `WT_NOTFOUND`. A final checkpoint is taken after commit. Verifies that fast-truncated key visibility changes correctly on commit.
- **Components:** `src/btree/bt_delete.c` (fast-delete), disagg fast truncate, MVCC isolation during uncommitted truncate
- **Notes:** Truncate range is ["100", "700"] (string-sorted; includes all string keys between these two values in lexicographic order).

### `test_layered_fast_truncate01.test_truncate_rollback`
- **What it tests:** Same setup (leader populates 1000 keys, follower opens). Follower creates an uncommitted truncate of ["100", "700"] then immediately rolls it back. After rollback, opens a new cursor and reads key "150": must return 0 (key found). Verifies that a rolled-back truncate leaves all data intact.
- **Components:** Rollback of fast truncate, MVCC rollback correctness

### `test_layered_fast_truncate01.test_truncate_write_conflict_1`
- **What it tests:** Follower creates an uncommitted truncate of ["100", "700"] (Session 1). Session 2 simultaneously attempts to update key "150" with value "hi" inside its own transaction. The update must raise `WiredTigerError` with message matching `/conflict between concurrent operations/`. Verifies that a concurrent write into an uncommitted truncate range is correctly detected as a write conflict.
- **Components:** Write conflict detection between `session.truncate()` and concurrent `cursor.update()`

### `test_layered_fast_truncate01.test_truncate_write_conflict_2`
- **What it tests:** Reverse conflict order: Session 2 first inserts an uncommitted value at key "100" (update). Session 1 then tries to start a truncate over ["100", "700"] while Session 2's write is still uncommitted. The `session.truncate()` call must raise `WiredTigerError` with `/conflict between concurrent operations/`. Session 1 rolls back, Session 2 commits. Verifies that a truncate that overlaps an uncommitted concurrent write also detects the conflict.
- **Components:** Write conflict detection in the truncate-initiator direction (truncate vs. prior uncommitted insert)
