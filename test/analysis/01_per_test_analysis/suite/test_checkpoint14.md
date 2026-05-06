# test_checkpoint14 — Two successive inconsistent checkpoints each carry their own snapshot

**File:** `test/suite/test_checkpoint14.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, checkpoint snapshot, crash recovery

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that two successive inconsistent checkpoints each maintain their own independent snapshot, and that after a simulated crash (`simulate_crash_restart`), the recovered database reflects the last completed checkpoint's snapshot correctly.
- **Components:** `src/checkpoint/`, `src/txn/txn_ckpt.c`, `src/txn/txn_rollback_to_stable.c`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_slow]` to create overlapping checkpoint/commit windows. Two checkpoints are taken at different stable timestamps. After crash restart, reading from `WiredTigerCheckpoint` returns data consistent with the second checkpoint's snapshot. Tests that successive checkpoint snapshots do not bleed into each other.
