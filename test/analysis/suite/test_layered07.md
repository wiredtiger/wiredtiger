# test_layered07 — Leader/follower role switch with data propagation

**File:** `test/suite/test_layered07.py`
**Storage mode:** Disagg/Layered
**Components under test:** leader/follower role switching, checkpoint propagation, layered table manager, conn_layered.c

## Test Cases

### `test_layered07.test_layered07`
- **What it tests:** A four-phase leader/follower role-switch scenario:
  1. The original leader inserts 500 * 3 = 1500 records and checkpoints.
  2. The leader steps down and the follower steps up (using `disagg_switch_follower_and_leader`), which also advances the follower to the last checkpoint.
  3. The newly-promoted leader (old follower) inserts another 1500 records and checkpoints.
  4. The now-follower (old leader) is advanced to the new checkpoint. Both nodes scan the table and assert exactly 3000 records are visible.
- **Components:** role switching (`conn_layered.c`, `conn_layered_ingest.c`), checkpoint propagation (`disagg_switch_follower_and_leader`, `disagg_advance_checkpoint`), layered cursor full-scan, page log (palite)
- **Notes:** Skipped on macOS (`sys.platform.startswith('darwin')`). Tests the complete leader-becomes-follower / follower-becomes-leader cycle and verifies that data written by each role is visible to both sides after the appropriate checkpoint pick-up. Would break if role transitions fail to correctly hand off stable checkpoint state, or if one side fails to see the other's committed data.
