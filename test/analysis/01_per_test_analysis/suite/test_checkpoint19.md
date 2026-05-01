# test_checkpoint19 — Timestamped: checkpoint cursor secures its matching HS checkpoint

**File:** `test/suite/test_checkpoint19.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, history store, checkpoint pairing, timestamped transactions

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Timestamped version of test_checkpoint18. Verifies that a checkpoint cursor opened with a `read_timestamp` correctly reads the historical version from the HS checkpoint that was paired with the DS checkpoint at that point in time, even after subsequent checkpoints advance the HS.
- **Components:** `src/checkpoint/`, `src/history/hs_cursor.c`, `src/cursor/cur_btree.c`
- **Notes:** Writes two versions at timestamps 1 and 10, checkpoints at stable=10, opens checkpoint cursor at `read_timestamp=1` (reading from HS), then runs more checkpoints. Verifies cursor still returns ts=1 value. Tests the HS/DS checkpoint pairing mechanism under the timestamped transaction model.
