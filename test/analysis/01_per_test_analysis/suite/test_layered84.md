# test_layered84 — Layered cursor walks on follower with prepared conflicts

**File:** `test/suite/test_layered84.py`
**Storage mode:** Disagg/Layered
**Components under test:** Layered cursor `next`/`prev` with `WT_PREPARE_CONFLICT`, cursor position preservation, no-skip/no-duplicate invariants, overwrite+rollback, conflict at start/end/mid

## Test Cases

### `test_layered84.test_overwrite_update_then_next_on_follower`
- **What it tests:** After a leader checkpoint of keys 1–5, follower opens an overwrite cursor, positions at key=3 via search, performs an overwrite update in the same transaction, commits. Advances checkpoint. Calls `next()` in a new transaction and verifies all returned keys are strictly > 3 (no duplicates or backward movement after the overwrite update).
- **Components:** `src/cursor/cur_layered.c`, overwrite cursor path, stable cursor lazy open

### `test_layered84.test_next_walk_prepare_conflict_mid_scan`
- **What it tests:** Leader checkpoints keys 1–6. Follower commits even keys 2,4,6; prepares key=4 at ts=50. Forward walk collects keys before the conflict (must see 1,2,3 — including stable-only key 3). After commit resolves prepare, continues collecting. Verifies union of both segments equals all 6 keys.
- **Components:** Forward walk conflict at mid-scan, stable-only keys not skipped after conflict

### `test_layered84.test_prev_walk_prepare_conflict_mid_scan`
- **What it tests:** Leader checkpoints keys 1–5. Follower commits all 5, prepares key=3. Backward walk stops at conflict. After commit, continues. Union of both segments equals all 5 keys.
- **Components:** Backward walk conflict at mid-scan

### `test_layered84.test_next_walk_committed_keys_then_prepared`
- **What it tests:** Keys 1–5 from leader. Follower commits keys 1,2,3 and prepares key=4. Forward walk returns {1,2,3} before conflict, then after commit returns remaining. Verifies no duplicates across segments, union = all 5 keys.
- **Components:** Committed keys before prepared key in forward scan

### `test_layered84.test_prev_walk_committed_keys_then_prepared`
- **What it tests:** Keys 1–5 from leader. Follower commits keys 3,4,5 and prepares key=2 (lower in reverse order). Backward walk returns {3,4,5} before conflict, then after commit returns remaining. Verifies no duplicates, union = all 5 keys.
- **Components:** Committed keys before prepared key in backward scan

### `test_layered84.test_next_walk_ingest_only_committed_then_prepared`
- **What it tests:** Stable has keys 1,3,5. Follower commits ingest keys 2,4; prepares key=6 (beyond stable end). Forward walk before conflict must include ingest keys 2 and 4. After commit, all 6 keys returned. No duplicates.
- **Components:** Follower-only (ingest-only) committed keys before follower-only prepared key

### `test_layered84.test_next_walk_conflict_at_start`
- **What it tests:** Keys 1–5 from leader. Follower commits 2,3,4,5; prepares key=1 (lowest). First `next()` hits conflict immediately (no keys returned before conflict). After commit, all 5 keys returned.
- **Components:** Conflict at very start of forward walk (no prior position)

### `test_layered84.test_prev_walk_conflict_at_end`
- **What it tests:** Keys 1–5. Follower commits 2,3,4,5; prepares key=1 (lowest = last in backward order). Backward walk returns {2,3,4,5} before conflict. After commit, key=1 returned.
- **Components:** Conflict at end of backward walk

### `test_layered84.test_next_walk_conflict_at_end`
- **What it tests:** Keys 1–5. Follower commits 1,2,3,4; prepares key=5 (highest = last in forward order). Forward walk returns {1,2,3,4} before conflict. After commit, key=5 returned.
- **Components:** Conflict at end of forward walk

### `test_layered84.test_prev_walk_conflict_at_start`
- **What it tests:** Keys 1–5. Follower commits 1,2,3,4; prepares key=5 (highest = first in backward order). First `prev()` hits conflict immediately. After commit, all 5 keys returned.
- **Components:** Conflict at very start of backward walk (no prior position)

### `test_layered84.test_next_walk_prepare_conflict_first_key`
- **What it tests:** All 5 keys have both stable and ingest (committed) values. Ingest also prepares key=1 (lowest). First `next()` hits conflict. After commit, all 5 keys returned.
- **Components:** Conflict on first key when ingest has data for every key

### `test_layered84.test_next_walk_prepare_conflict_then_rollback`
- **What it tests:** Committed keys 1,2,4,5 (no committed value for key=3). Prepared key=3. Forward walk stops at conflict (keys_before = {1,2}). Prepared transaction rolled back. Resume: union must be {1,2,4,5} (key=3 never appears), strictly ascending order (no position loss).
- **Components:** Prepare conflict then rollback, rolled-back key must not appear

### `test_layered84.test_prev_walk_prepare_conflict_then_rollback`
- **What it tests:** Same setup as `test_next_walk_prepare_conflict_then_rollback` but backward. Keys_before = {5,4}, union = {1,2,4,5}, strictly descending order.
- **Components:** Backward scan, prepare then rollback

### `test_layered84.test_next_walk_overwrite_rollback`
- **What it tests:** All 5 keys have committed ingest values. Prepared overwrite on key=3 (which already has 'committed_3'). Forward walk stops at conflict. Prepared rolled back. Key=3 must reappear with its prior committed value 'committed_3' (not disappear). No duplicates, ascending order.
- **Components:** Overwrite rollback — key reverts to prior committed value

### `test_layered84.test_prev_walk_overwrite_rollback`
- **What it tests:** Same as `test_next_walk_overwrite_rollback` but backward. Descending order, key=3 reads back as 'committed_3' after rollback.
- **Components:** Backward overwrite rollback

### `test_layered84.test_next_walk_rollback_at_first_key`
- **What it tests:** Committed keys 2,3,4,5. Prepared key=1 (only prepared, never committed). First `next()` hits conflict. Prepared rolled back. Resume: union = {2,3,4,5}; key=1 must not appear.
- **Components:** Conflict at first key then rollback of never-committed key
