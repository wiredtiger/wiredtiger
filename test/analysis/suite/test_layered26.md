# test_layered26 — Follower picks up checkpoint and stable component becomes visible; full role swap

**File:** `test/suite/test_layered26.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** checkpoint pickup making stable component visible to follower, leader/follower role swap with data verification, conn_layered.c, page log (palite)

## Test Cases

### `test_layered26.test_layered26`
- **What it tests:** A full leader/follower cycle with explicit before-and-after visibility checks:

  1. Starts as follower, steps up to leader. Creates a table (parametrized type). A secondary (follower) also creates the same table.
  2. Leader inserts 5000 records, checkpoints. Verifies leader sees 5000. Verifies follower sees 0 (no checkpoint picked up yet).
  3. Calls `disagg_advance_checkpoint(conn_follow)`. Verifies follower now sees all 5000 records.
  4. Swaps roles: follower becomes leader, leader becomes follower.
  5. New leader inserts 5000 more records (keys 5000-9999), checkpoints. Verifies new leader sees all 10,000. Verifies new follower (old leader) still sees only 5,000.
  6. Calls `disagg_advance_checkpoint(self.conn)` for the new follower. Verifies new follower now sees all 10,000.

- **Components:** `disagg_advance_checkpoint`, stable component pickup timing, role reconfiguration, layered table manager, checkpoint propagation, conn_layered.c
- **Notes:** Parametrized across 2 table types (layered: prefix, table:+disagg+type=layered) and disagg_storage. Uses `precise_checkpoint=true`. The key invariant is that the stable component is NOT visible on the follower until `disagg_advance_checkpoint` is called. This is a direct test of the checkpoint pickup mechanism and the `checkpoint_meta` reconfigure path. Would break if stable data leaks to the follower before the checkpoint is picked up, or if checkpoint pickup fails to make the stable component visible.
