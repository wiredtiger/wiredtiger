# test_checkpoint11 — Inconsistent checkpoint with concurrent timestamped commit

**File:** `test/suite/test_checkpoint11.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, checkpoint snapshot, timestamped transactions, visibility

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies the all-or-nothing visibility guarantee for a timestamped transaction that commits concurrently with a checkpoint. Reads at various timestamps from the checkpoint cursor reveal consistent snapshots — no partial visibility of the concurrent commit.
- **Components:** `src/checkpoint/`, `src/txn/txn_ckpt.c`, `src/cursor/cur_btree.c`
- **Notes:** Like test_checkpoint10 but with timestamps. Uses `timing_stress_for_test=[checkpoint_slow]`. Two tables written with same `commit_timestamp`. After checkpoint, reads at `read_timestamp` below and above the commit timestamp confirm either full visibility or no visibility across both tables. Tests the checkpoint snapshot boundary at specific timestamps.
