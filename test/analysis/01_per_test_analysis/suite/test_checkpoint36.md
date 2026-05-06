# test_checkpoint36 — Precise checkpoint requires stable_timestamp; use_timestamp=false errors

**File:** `test/suite/test_checkpoint36.py`
**Storage mode:** General
**Components under test:** precise checkpoint, API validation, timestamps

## Test Cases

### `test_checkpoint36.test_checkpoint36`
- **What it tests:** Verifies that attempting a precise checkpoint without a `stable_timestamp` set, or with `use_timestamp=false`, raises an appropriate error (`EINVAL` or `WT_ERROR`), since precise checkpoints require a stable timestamp to define the snapshot boundary.
- **Components:** `src/checkpoint/checkpoint.c`, `src/txn/txn_timestamp.c`
- **Notes:** Tests three error conditions: (1) `checkpoint=(precise=true)` with no stable timestamp set; (2) `checkpoint=(precise=true)` combined with `use_timestamp=false` in the checkpoint call; (3) successful precise checkpoint after `stable_timestamp` is properly set. Validates the API contract for precise checkpoints.
