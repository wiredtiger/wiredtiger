# test_layered92 — cursor.reserve() on layered cursors for all key states

**File:** `test/suite/test_layered92.py`
**Storage mode:** Disagg/Layered
**Components under test:** `cursor.reserve()` on layered table, leader and follower, stable-only, ingest-only, both, missing key

## Test Cases

### `test_layered92.test_leader_key_exists`
- **What it tests:** Leader creates table, writes key=1 (ts=1). Calls `reserve()` before checkpoint: returns 0. Checkpoints, calls `reserve()` again: returns 0. Tests that reserve succeeds both pre- and post-checkpoint on a leader.
- **Components:** `src/cursor/cur_layered.c`, reserve path on leader

### `test_layered92.test_leader_key_missing`
- **What it tests:** Leader creates table, checkpoints at ts=1 (no keys). Calls `reserve(key=99)`: must raise `WiredTigerError` (key not found).
- **Components:** Reserve on missing key — must propagate `WT_NOTFOUND` as an error

### `test_layered92.test_follower_key_in_stable_only`
- **What it tests:** Leader writes key=1 (ts=1), checkpoints. Follower advances checkpoint. Calls `reserve(key=1)` on the follower: returns 0 (key found in stable btree).
- **Components:** Reserve on stable-only key on follower

### `test_layered92.test_follower_key_in_ingest_only`
- **What it tests:** Leader checkpoints empty table at ts=1. Follower advances. Follower writes key=1 (ts=2). Calls `reserve(key=1)` on follower: returns 0 (key found in ingest btree).
- **Components:** Reserve on ingest-only key on follower

### `test_layered92.test_follower_key_in_both`
- **What it tests:** Leader writes key=1 (ts=1), checkpoints. Follower advances, then writes key=1 again (ts=2, overrides stable). Calls `reserve(key=1)` on follower: returns 0.
- **Components:** Reserve on key present in both stable and ingest

### `test_layered92.test_follower_key_missing`
- **What it tests:** Leader checkpoints empty table. Follower advances. Calls `reserve(key=99)` on follower: must raise `WiredTigerError`.
- **Components:** Reserve on missing key on follower — correct `WT_NOTFOUND` error propagation
