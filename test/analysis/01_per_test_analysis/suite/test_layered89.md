# test_layered89 — Follower cursor operations on checkpointed prepared cells do not raise WT_PREPARE_CONFLICT

**File:** `test/suite/test_layered89.py`
**Storage mode:** Disagg/Layered
**Components under test:** Prepared transaction cells in checkpoint, follower cursor `next`/`prev`/`search`/`search_near`, `preserve_prepared=true`, eviction to force on-disk reads

## Test Cases

### `test_layered89.test_next_walk_prepared_update`
- **What it tests:** Both primary and follower commit initial values for keys 1–5 (ts=10), checkpoint. Both then prepare updates for keys 2 and 4 (same prepared_id=1, ts=20). Primary checkpoints at stable=20 (with prepare active). Follower advances checkpoint, then rolls back its own copy of the prepare (ts=30). Follower evicts pages for keys 2 and 4 to force on-disk reads. Calls `cursor.next()` until `WT_NOTFOUND`. Verifies all 5 keys are returned (no `WT_PREPARE_CONFLICT`).
- **Components:** `src/cursor/cur_layered.c`, prepared cell handling on follower, `preserve_prepared=true`
- **Notes:** The key invariant is that after the follower rolls back its own prepare, the stable (checkpointed) prepared cells from the leader's checkpoint must not raise `WT_PREPARE_CONFLICT` — the follower already resolved the prepare locally, so the on-disk prepared cell is effectively invisible.

### `test_layered89.test_prev_walk_prepared_update`
- **What it tests:** Identical setup to `test_next_walk_prepared_update` but uses `cursor.prev()`. Verifies all 5 keys returned in backward order.
- **Components:** Backward walk through checkpointed prepared cells

### `test_layered89.test_next_walk_prepared_tombstone`
- **What it tests:** Same setup but the prepared operation is a delete (`delete=True`). After the follower rolls back the local delete, keys 2 and 4 still have their committed values. `cursor.next()` must return all 5 keys (the uncommitted delete is not visible; no `WT_PREPARE_CONFLICT`).
- **Components:** Checkpointed prepared tombstone cells, forward walk

### `test_layered89.test_prev_walk_prepared_tombstone`
- **What it tests:** Same as `test_next_walk_prepared_tombstone` but backward scan.
- **Components:** Checkpointed prepared tombstone cells, backward walk

### `test_layered89.test_search_and_search_near_prepared_update`
- **What it tests:** Keys 1,2,3 committed. Key 2 prepared on both sides (ts=20). Primary checkpoints. Follower rolls back its copy, evicts key 2's page. Runs `cursor.search(1)`, `cursor.search_near(1)`, `cursor.search(2)`, `cursor.search_near(2)` — all must return the committed values "committed_1" and "committed_2" respectively without raising `WT_PREPARE_CONFLICT`.
- **Components:** `search` and `search_near` on follower with checkpointed prepared cells
