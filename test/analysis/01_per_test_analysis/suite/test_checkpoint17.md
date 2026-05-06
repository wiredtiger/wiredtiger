# test_checkpoint17 — History store accessible from checkpoint when HS was clean at checkpoint time

**File:** `test/suite/test_checkpoint17.py`
**Storage mode:** General
**Components under test:** checkpoint cursor, history store, multi-version visibility

## Test Cases

### `test_checkpoint.test_checkpoint`
- **What it tests:** Verifies that a checkpoint cursor can read historical data from the history store even when the HS itself was clean (unmodified) at checkpoint time. This ensures the clean-table optimization applied to the HS does not prevent checkpoint cursors from reading old versions needed for MVCC reads.
- **Components:** `src/checkpoint/`, `src/history/hs_cursor.c`, `src/cursor/cur_btree.c`
- **Notes:** Inserts data at two timestamps (ts=1 and ts=10). The first checkpoint captures both, making ts=1 data go to the HS. The second checkpoint may skip the HS (clean). A cursor on the second checkpoint with `read_timestamp=1` must still return the ts=1 value from the HS. Tests the HS+checkpoint cursor interaction across clean-checkpoint cycles.
