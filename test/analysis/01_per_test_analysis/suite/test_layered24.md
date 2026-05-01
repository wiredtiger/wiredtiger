# test_layered24 — Drop table on follower must not fall back to reading stable component

**File:** `test/suite/test_layered24.py`
**Storage mode:** Disagg/Layered
**Components under test:** table drop on follower, schema operations, stable btree isolation after drop, conn_layered.c

## Test Cases

### `test_layered24.test_layered24`
- **What it tests:** A three-part scenario testing drop semantics for layered tables on both leader and follower:

  1. **Setup:** Leader creates a layered table and inserts 10,000 * 3 = 30,000 records. Checkpoints and advances the follower checkpoint. Follower reads all records — verifies count = 30,000.
  2. **Follower drop:** `session_follow.drop(uri, 'force=true')`. Verifies that trying to open a cursor on the dropped URI raises `WiredTigerError` — the follower must not fall back to reading from the stable component that is still in the page log.
  3. **Leader drop:** Reads the leader to confirm it still has all 30,000 records. Reopens the connection to avoid any handle caching. Drops the table on the leader. Verifies that trying to open a cursor on the leader also raises `WiredTigerError`.

- **Components:** `session.drop()` on layered and follower connections, ingest btree cleanup, stable btree isolation, schema layer, conn_layered.c
- **Notes:** The key correctness invariant is that a local `drop()` on the follower must make the table completely inaccessible, even though the stable data still exists in the shared page log. Would break if `cur_layered.c` bypasses the dropped-table state and directly accesses the stable btree. Also tests that the leader's drop properly cleans up all metadata.
