# test_layered93 — Cursor operations on stable-only keys on a follower

**File:** `test/suite/test_layered93.py`
**Storage mode:** Disagg/Layered
**Components under test:** Layered cursor operations (reserve, search, search_near, update, remove, modify) on stable-only keys, follower correctness

## Test Cases

### `test_layered93.test_follower_ops_on_stable_table`
- **What it tests:** Leader creates a layered table, inserts keys 1–10 (each at their own commit_timestamp), checkpoints at stable=10. Follower opens and advances to the latest checkpoint. Opens a cursor on the follower and runs the parametrized operation (`do_op`) on key=5 (which exists only in the stable btree). Verifies the return value is 0 for all operations.
- **Components:** `src/cursor/cur_layered.c`, all cursor operations on stable-only keys
- **Notes:** Parametrized by operation: `reserve`, `search`, `search_near`, `update`, `remove`, `modify`. The `modify` operation replaces the first character of "value5" with "X". Each operation is executed inside a transaction that is rolled back afterward. All 6 operations must return 0 (success) when the key exists in stable. Disagg-only.
