# test_checkpoint28 — Prepared transaction with checkpoint_handle stress; two-table consistency

**File:** `test/suite/test_checkpoint28.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, prepared transactions, timing stress, cross-table consistency

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that two tables updated in a single prepared transaction have consistent visibility in a checkpoint taken concurrently with the prepare/commit, under `timing_stress_for_test=[checkpoint_handle]` stress injection.
- **Components:** `src/checkpoint/`, `src/txn/txn_prepare.c`, `src/cursor/cur_btree.c`
- **Notes:** Uses `timing_stress_for_test=[checkpoint_handle]` to widen checkpoint-handle acquisition windows. A prepared transaction updates both tables; a checkpoint runs concurrently. Reading from the checkpoint must show either both tables updated or neither — no partial visibility across tables. Tests atomicity of prepared commits relative to checkpoint snapshots.
