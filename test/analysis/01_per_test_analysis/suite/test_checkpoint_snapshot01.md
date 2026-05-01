# test_checkpoint_snapshot01 — Multiple session snapshots during checkpoint with crash simulation

**File:** `test/suite/test_checkpoint_snapshot01.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, snapshot isolation, crash recovery, RTS

## Test Cases

### `test_checkpoint_snapshot01.test_checkpoint_snapshot01`
- **What it tests:** Verifies that when a checkpoint is taken while multiple sessions hold open transactions (snapshots), the checkpoint correctly captures the committed state at stable_timestamp and that simulated crash + RTS restores the database consistently.
- **Components:** `src/checkpoint/`, `src/txn/txn_ckpt.c`, `src/txn/txn_rollback_to_stable.c`
- **Notes:** Multiple sessions each begin transactions at different timestamps (creating a diverse snapshot set) before a checkpoint is triggered. After `simulate_crash_restart`, verifies that committed-and-stable data survives while uncommitted data is rolled back. Tests the checkpoint snapshot mechanism with concurrent active sessions.
