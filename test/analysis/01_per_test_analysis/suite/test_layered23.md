# test_layered23 — Leader/follower oplog simulation with interleaved checkpoints and checkpoint pickup stats

**File:** `test/suite/test_layered23.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** leader/follower data divergence and convergence, Oplog helper, checkpoint pickup stats, checkpoint count stats, layered table manager, conn_layered.c

## Test Cases

### `test_layered23.test_leader_follower`
- **What it tests:** Uses the `Oplog` helper class to simulate a MongoDB-style oplog workload across leader and follower nodes. The test proceeds in interleaved phases:

  1. Creates the table on both leader and follower.
  2. Generates oplog traffic (900 inserts + 100 updates, then another 900 + 100 updates), applies all to the leader, checkpoints. Verifies checkpoint count = 1.
  3. Applies the first 2100 entries to the follower (slightly ahead of checkpoint 1).
  4. Advances the follower to checkpoint 1. Checks `layered_table_manager_checkpoints_disagg_pick_up_follower` stat equals 1. Re-verifies all 2100 follower entries.
  5. Repeats 9 more iterations: each iteration the leader inserts 900 + 100 updates, checkpoints; the follower applies entries up to just before the current leader position, then picks up the new checkpoint and verifies consistency.
  6. Occasionally skips data insertion between checkpoints (every 3rd iteration), verifying checkpoints still work with no new data.

- **Components:** oplog insert/update/check path, checkpoint (leader), checkpoint pick-up (follower), `disagg_advance_checkpoint`, statistics (`conn.checkpoints_total_succeed`, `layered_table_manager_checkpoints_disagg_pick_up_follower`), Oplog class (timestamp-based point-reads and cursor scans)
- **Notes:** Single disagg_storage scenario (no `make_scenarios`). Uses the `Oplog` helper's `check()` method which does both point-reads at each timestamp and a full cursor scan comparing against expected state. The follower deliberately stays behind the leader by up to 900 entries but must always be ahead of the most recent checkpoint boundary. Would break if: checkpoint propagation is unreliable, the follower misses entries when picking up a checkpoint, or stats are not updated correctly.
