# test_layered09 — Leaf page delta encoding: write, modify, delete, and insert via delta, read on follower

**File:** `test/suite/test_layered09.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** page delta reconciliation, leaf page deltas, block_disagg, checkpoint, follower point-reads, encryption, compression

## Test Cases

### `test_layered09.test_layered_read_write`
- **What it tests:** Inserts 100 records at timestamp 5, checkpoints (full page), then updates every 10th record at timestamp 10 and checkpoints again. Asserts `rec_page_delta_leaf` stat > 0 (i.e., at least one delta was written). Reopens as follower and verifies point-reads at both timestamps return the correct value.
- **Components:** page delta writer (`block_disagg`, reconciliation), checkpoint, page log (palite), follower timestamped reads
- **Notes:** `delta_pct=100` forces delta writing aggressively. Parametrized across 2 encryption x 2 compression x disagg_storage = at minimum 4 scenarios. Skipped on macOS.

### `test_layered09.test_layered_read_modify`
- **What it tests:** Inserts 100 records at timestamp 5, checkpoints. Applies in-place `cursor.modify()` to every 10th record at timestamp 10, checkpoints. Asserts delta stat > 0. Reopens as follower and verifies: at ts=5 all records have original value; at ts=10, every 10th has the modified value.
- **Components:** modify path (`cur_layered.c`), page delta, checkpoint, follower timestamped reads
- **Notes:** Tests that `wiredtiger.Modify` operations are correctly stored in a delta and reconstructed on the follower.

### `test_layered09.test_layered_read_delete`
- **What it tests:** Inserts 100 records at timestamp 5, checkpoints. Deletes every 10th at timestamp 10, checkpoints. Asserts delta stat > 0. Reopens as follower and verifies: at ts=5 all records present; at ts=10, every 10th returns `WT_NOTFOUND`, others still have original value.
- **Components:** delete path, page delta with tombstones, follower timestamped reads

### `test_layered09.test_layered_read_insert`
- **What it tests:** Inserts 100 records at timestamp 5, checkpoints. Inserts 5 new records (keys 100-104) at timestamp 10, checkpoints. Asserts delta stat > 0. Reopens as follower and verifies at ts=5 only keys 0-99 exist; at ts=10 all 105 records exist.
- **Components:** page delta with new inserts appended to an existing page, follower reads

### `test_layered09.test_layered_read_multiple_delta`
- **What it tests:** Three rounds of inserts/updates at timestamps 5, 10, and 15, each followed by a checkpoint. Asserts delta stat > 0. Reopens as follower and verifies three-tier historical reads: at ts=5 all original values; at ts=10 updates at multiples of 10; at ts=15 further updates at multiples of 20.
- **Components:** chained delta pages (multiple sequential deltas on the same page), follower historical reads
- **Notes:** Tests that a chain of deltas on the same page is correctly applied in timestamp order on the follower.

### `test_layered09.test_layered_read_delete_insert`
- **What it tests:** Inserts 100 records at ts=5, checkpoints. Deletes every 10th at ts=10, checkpoints. Re-inserts every 20th with new value at ts=15, checkpoints. Asserts delta stat > 0. Reopens as follower and verifies three-tier reads: ts=5 all present, ts=10 every-10th deleted, ts=15 every-20th re-inserted (others every-10th still absent).
- **Components:** delete+insert sequence in page delta chain, follower historical reads
- **Notes:** The most complex delta scenario in this file — verifies delete-then-reinsert correctly appears in the delta chain at the right timestamps.
