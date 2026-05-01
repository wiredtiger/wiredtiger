# test_layered53 — Checkpoint created solely to capture stable timestamp update

**File:** `test/suite/test_layered53.py`
**Storage mode:** Disagg/Layered
**Components under test:** checkpoint, page log, follower checkpoint pick-up, timestamp propagation, follower read-only enforcement

## Test Cases

### `test_layered53.test_layered53`
- **What it tests:** Verifies that a checkpoint can be created on the leader just to advance the stable timestamp (without any dirty data), and that this checkpoint is correctly propagated to a follower. Also verifies that: (a) the follower's `last_checkpoint` query timestamp is updated after each pick-up, (b) a follower that advances its own stable timestamp and checkpoints does not produce a new complete checkpoint (the leader's last LSN is unchanged), and (c) re-advancing the same checkpoint on the follower is idempotent and logs "Picking up the same checkpoint again" rather than applying it twice.
- **Components:** checkpoint (stable-timestamp-only checkpoint), page log (`disagg_get_complete_checkpoint_ext`), follower checkpoint pick-up (`disagg_advance_checkpoint`), `conn.query_timestamp('get=last_checkpoint')`, follower read-only enforcement, page log idempotence
- **Notes:** 10 items. The follower also attempts to checkpoint with modified data — verifying that follower-side checkpoints do not produce new complete checkpoints. The idempotence check confirms `meta_lsn` is unchanged after a duplicate pick-up. Disagg-only.
