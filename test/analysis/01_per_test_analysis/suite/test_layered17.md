# test_layered17 — Timestamp propagation through checkpoint to follower

**File:** `test/suite/test_layered17.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** checkpoint timestamps, stable timestamp, checkpoint metadata, follower timestamp pickup, page log (palite)

## Test Cases

### `test_layered17.test_layered17`
- **What it tests:** A three-phase timestamp test verifying that checkpoint timestamps are correctly propagated to followers:

  **Phase 1:** Inserts 500 records at commit_timestamp=100, sets stable_timestamp=100, checkpoints. Verifies the checkpoint's timestamp is 100 (via `disagg_get_complete_checkpoint_ext`). Creates a follower, advances it to the checkpoint, and verifies: (a) `conn.query_timestamp('get=last_checkpoint')` equals 100, (b) all 500 records are readable.

  **Phase 2:** Updates every 50th record at commit_timestamp=200, sets stable_timestamp=200, checkpoints. Verifies checkpoint timestamp is 200. Advances follower to new checkpoint. Verifies follower's `last_checkpoint` is 200 and updated records are visible.

  **Phase 3:** Updates every 25th record at commit_timestamp=300, but sets stable_timestamp=250 (commit is above stable). Checkpoints. Verifies checkpoint timestamp is 250 (not 300 — the uncommitted-beyond-stable data is excluded). Advances follower. Verifies follower's `last_checkpoint` is 250 and the timestamp=300 changes are NOT visible (follower sees Phase 2 data, not Phase 3 changes).

- **Components:** checkpoint timestamp bookkeeping, stable timestamp enforcement during checkpoint, checkpoint metadata (`checkpoint_meta` string), follower checkpoint pickup, `query_timestamp` API, conn_layered.c, palite page log
- **Notes:** Parametrized across 3 table types (layered: prefix, table: with disagg+type=layered, table: with disagg+log=disabled) and disagg_storage. Uses `precise_checkpoint=true`. The Phase 3 check is the most important: it validates that data committed above the stable timestamp does not appear in the checkpoint or in the follower's view. Would break if checkpoint metadata does not correctly capture the stable timestamp, or if the follower picks up more data than it should.
