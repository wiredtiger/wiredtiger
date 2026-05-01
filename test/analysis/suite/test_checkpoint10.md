# test_checkpoint10 — Inconsistent checkpoint with concurrent non-timestamped commit

**File:** `test/suite/test_checkpoint10.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, checkpoint snapshot, non-timestamped transactions, visibility

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies the all-or-nothing visibility guarantee for a non-timestamped transaction that commits concurrently with a checkpoint. A transaction whose commit falls inside the checkpoint's snapshot is either entirely visible or entirely invisible when reading from that checkpoint — never partially applied.
- **Components:** `src/checkpoint/`, `src/txn/txn_ckpt.c`, `src/cursor/cur_btree.c`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_slow]` to widen the race window. Two tables are updated in the same transaction (no timestamps). After checkpoint and potential crash restart, reading from the checkpoint either sees both tables updated or neither. Tests the checkpoint snapshot consistency guarantee for non-timestamped workloads.
