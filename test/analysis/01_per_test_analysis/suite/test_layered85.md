# test_layered85 — Mid-scan checkpoint advance on follower cursor

**File:** `test/suite/test_layered85.py`
**Storage mode:** Disagg/Layered
**Components under test:** Mid-scan stable checkpoint advance, `layered_curs_advance_stable` stat, monotonic ordering, cursor bounds, tombstone visibility, multiple advances

## Test Cases

### `test_layered85.test_checkpoint_advance_during_scan_positioned_on_follower`
- **What it tests:** Forward and backward scans with a read timestamp, each with a mid-scan checkpoint advance. Setup: even keys 0–998 in stable, odd keys 1–999 in ingest. Begin transaction at the current read timestamp. Forward: scans 100 keys, then leader adds keys 1000–1099 and checkpoints (not visible at read timestamp). Continue forward scan. Verifies `layered_curs_advance_stable` incremented and all keys are in strictly ascending order. Backward: resets cursor, scans 100 keys backward, another checkpoint advance, continues backward. Verifies strictly descending order.
- **Components:** `src/cursor/cur_layered.c`, mid-scan checkpoint switch logic, `layered_curs_advance_stable` stat
- **Notes:** The checkpoint switch requires: read timestamp on active transaction, cursor positioned and iterating, new checkpoint available. New keys are beyond the read timestamp so they remain invisible, but the advance still triggers the stable cursor swap.

### `test_layered85.test_checkpoint_advance_during_bounded_scan_positioned_on_follower`
- **What it tests:** Same as `test_checkpoint_advance_during_scan_positioned_on_follower` but with bounds [200, 800]. Forward: 50 keys, mid-scan advance, continue. Verifies `layered_curs_advance_stable` incremented, ascending order, and all keys within [200, 800]. Backward: re-applies bounds after reset, 50 keys, advance, continue. Verifies descending order and bounds.
- **Components:** Bounded mid-scan checkpoint advance, bounds enforcement after stable cursor swap

### `test_layered85.test_checkpoint_advance_during_scan_with_tombstones_on_follower`
- **What it tests:** Leader checkpoints even keys 0–998. Follower adds odd keys 1–999 and removes even keys 400–600. Begin read transaction. Scans forward 250 keys, then leader checkpoints 1000–1099 (mid-scan advance). Continues scan. Verifies ascending order and that tombstoned even keys 400–600 never appear in the scan.
- **Components:** Tombstone visibility preserved across mid-scan checkpoint advance

### `test_layered85.test_multiple_checkpoint_advances_during_scan_on_follower`
- **What it tests:** Three-phase checkpoint advance during a single scan. Checkpoint 1: keys 0–299. Follower adds odd keys 301–999. Begin read transaction. Scans 150 keys. Checkpoint 2: leader adds 300–599, follower advances. Scans 200 more keys. Checkpoint 3: leader adds 600–999, follower advances. Completes scan. Verifies `layered_curs_advance_stable` > baseline and all keys in strictly ascending order throughout all three segments.
- **Components:** Multiple mid-scan stable cursor swaps, monotonic ordering invariant

### `test_layered85.test_scan_completeness_after_checkpoint_removes_key_mid_scan`
- **What it tests:** Leader checkpoints even keys 0–998. Follower adds odd keys 1–999. Begin read transaction (captures current read_ts). Forward scan collects 502 keys (past key 500). Commits transaction. Leader removes the next even key after the last seen, checkpoints. Starts a new read transaction at the post-remove timestamp. Continues scan with the repositioned cursor. Verifies: all keys in ascending order across both halves, the removed key does not appear, and even keys beyond the removed key (e.g., 702–998) all appear.
- **Components:** Scan completeness after stable key removal mid-scan, cursor repositioning with new read timestamp
- **Notes:** The test changes the read timestamp between the two halves by committing and starting a new transaction, so the delete is visible in the second half. Even keys after the removed key must still be present (completeness check).
