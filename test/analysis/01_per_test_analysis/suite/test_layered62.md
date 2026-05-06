# test_layered62 — Checkpoint and role change (step-up/step-down) synchronization

**File:** `test/suite/test_layered62.py`
**Storage mode:** Disagg/Layered
**Components under test:** Checkpoint locking, role transitions (leader/follower), concurrent checkpoint/reconfigure

## Test Cases

### `test_layered62.test_layered62`
- **What it tests:** Two-part test verifying that checkpoint and role-change operations are correctly synchronized:
  1. **Part 1 (step-up):** Confirms that stepping up (follower → leader) does not retroactively change an already-completed checkpoint. After restart without local files, inserts data, steps up, and asserts the last checkpoint timestamp is still 1 (from the pre-restart checkpoint). After a new checkpoint the timestamp correctly advances to 2.
  2. **Part 2 (step-down during checkpoint):** Starts a checkpoint in a background thread with `timing_stress_for_test=[checkpoint_slow]` (minimum 10 s delay), then reconfigures to follower role mid-checkpoint. Verifies that the checkpoint completes with the leader's data (timestamp=3) and a subsequent restart can read all three keys.
- **Components:** `src/session/session_api.c` (checkpoint), `src/conn/conn_dhandle.c` (role reconfigure), `src/conn/conn_layered_ingest.c`
- **Notes:** Uses `threading.Thread` to run the checkpoint concurrently. `timing_stress_for_test=[checkpoint_slow]` ensures the step-down races with an active checkpoint. Tests that `WT_CHECKPOINT_STATE` transitions are visible before the role change.
