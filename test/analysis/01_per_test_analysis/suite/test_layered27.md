# test_layered27 — Drain ingest table: insert/update/remove sequences during follower-to-leader promotion

**File:** `test/suite/test_layered27.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** ingest table drain process, follower-to-leader promotion, checkpoint after drain, Oplog helper, conn_layered_ingest.c, conn_layered.c

## Test Cases

### `test_layered27.test_drain_insert_update`
- **What it tests:** Leader inserts 100*multiplier records and checkpoints (checkpoint 1). Adds 100*multiplier more inserts and 200*multiplier updates to the oplog (not yet applied to leader). Follower applies all 400*multiplier entries (including post-checkpoint ones) and picks up checkpoint 1. Leader is closed (skipping checkpoint). Follower becomes leader, sets stable timestamp to latest, and checkpoints — this drains its ingest table. Reopens the new leader as follower and verifies all 400*multiplier entries are correct via `oplog.check()`.
- **Components:** drain process (follower-to-leader transition triggers ingest drain on checkpoint), ingest btree write, stable btree, checkpoint, Oplog check (point-reads + cursor scan)
- **Notes:** Parametrized across 2 sizes (multiplier=1 small, multiplier=100 large) and disagg_storage. The FIXME-WT-15763 comment marks out leader-side post-checkpoint application as disabled. Tests that when a follower (with ingest-only data beyond the last checkpoint) is promoted to leader and checkpoints, the ingest data is correctly merged into the stable btree.

### `test_layered27.test_drain_remove`
- **What it tests:** Same structure as `test_drain_insert_update` but the post-checkpoint follower operations are removes (tombstones). After drain and reopen as follower, verifies that the removed keys are gone and only the non-removed keys remain.
- **Components:** tombstone drain, ingest btree remove, drain process during promotion
- **Notes:** Verifies that tombstones in the ingest table are correctly propagated into the stable btree during the drain checkpoint.

### `test_layered27.test_drain_insert_remove_within_same_transaction`
- **What it tests:** A regression test for WT-15721/WT-16085 (consecutive tombstones in update chain during drain). Performs a specific sequence on a follower: insert at T1, delete at T2, insert+delete in the same transaction at T3, insert at T4, insert at T5. Steps up to leader, sets stable to T5, checkpoints. The test is expected to complete without error or crash — the key concern is that no consecutive tombstones appear in the update chain during the drain.
- **Components:** drain process, update chain validation, conn_layered_ingest.c, reconciliation
- **Notes:** Direct regression for two specific bugs (WT-15721, WT-16085). Does not verify data correctness after drain; the test passes if the checkpoint completes without error.

### `test_layered27.test_drain_remove_insert`
- **What it tests:** Same structure as `test_drain_remove` but the post-checkpoint operations are removes followed by fresh inserts of the same keys with new values. After drain and reopen as follower, verifies that the re-inserted values are visible (not the original or the removes).
- **Components:** remove+reinsert sequence in drain, tombstone-then-insert update chain resolution
- **Notes:** Verifies that the drain correctly handles a key that was deleted and then re-inserted after the last checkpoint, producing the new value in the stable btree.
