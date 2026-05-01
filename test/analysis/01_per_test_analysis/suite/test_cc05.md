# test_cc05 — Open checkpoint cursor prevents GC from removing that checkpoint

**File:** `test/suite/test_cc05.py`
**Storage mode:** General
**Components under test:** checkpoint cleanup subsystem, checkpoint cursor locking, named checkpoints

## Test Cases

### `test_cc05.test_cc`
- **What it tests:** Verifies that an open checkpoint cursor pins the checkpoint so that garbage collection (CC) cannot remove it even when `oldest_timestamp` has been advanced past that checkpoint's stable timestamp. After CC runs and oldest moves to 70, data written at ts=30 (captured in checkpoint_one / WiredTigerCheckpoint at stable_ts=35) is still visible through the locked cursor, returning `value_y`.
- **Components:** `src/btree/bt_walk.c`, `src/checkpoint/`, `src/session/session_dhandle.c`
- **Notes:** Skip: `@wttest.skip_for_hook("disagg", "layered trees do not support named checkpoints")`. Four scenarios from cross-product of `format_values` (column, integer_row) × `named_values` (named=True, named=False). Uses 10 000 rows with six value strings (u–z). Named scenario: opens `checkpoint=checkpoint_one`; anonymous scenario: opens `checkpoint=WiredTigerCheckpoint`. After CC runs, verifies the locked cursor still reads `value_y` (ts=30). After closing, re-opening: named checkpoint still returns `value_y`; anonymous checkpoint now returns `value_w` (latest, ts=70).
