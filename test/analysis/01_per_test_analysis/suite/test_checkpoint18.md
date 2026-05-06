# test_checkpoint18 — Non-timestamped: checkpoint cursor secures its matching HS checkpoint

**File:** `test/suite/test_checkpoint18.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, history store, checkpoint pairing, non-timestamped

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a checkpoint cursor on the data store is paired with the correct corresponding HS checkpoint and that subsequent DB-level checkpoints do not cause the cursor to lose its HS data. Uses non-timestamped transactions.
- **Components:** `src/checkpoint/`, `src/history/hs_cursor.c`, `src/cursor/cur_btree.c`
- **Notes:** Writes two versions of each key (non-timestamped), checkpoints, opens a checkpoint cursor, then runs additional checkpoints that may overwrite the HS. The cursor must continue returning the version that was in the HS at the time the checkpoint was taken. Tests that HS checkpoint identity is preserved for the lifetime of a checkpoint cursor.
