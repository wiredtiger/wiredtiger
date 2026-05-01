# test_layered47 — Ingest table prune-timestamp correctness during checkpoint pick-ups

**File:** `test/suite/test_layered47.py`
**Storage mode:** Disagg/Layered
**Components under test:** ingest btree, prune timestamp, checkpoint pick-up, cur_layered.c, conn_layered_ingest.c

## Test Cases

### `test_layered47.test_prune_timestamp_initialization`
- **What it tests:** Regression test for WT-15158. Before the fix, `prune_timestamp` was set to the last checkpoint timestamp during ingest table initialization, which could be newer than the checkpoint still in use. This test simulates the conflict: holds a cursor open on `uris[0]` pinning checkpoint 1, then picks up checkpoint 2, then opens a cursor on `uris[1]` (which initializes its `prune_timestamp` from checkpoint 2). A third checkpoint pick-up then tries to update the prune timestamp — previously this caused a conflict by setting it to an older checkpoint (1). The test just verifies no crash/error occurs.
- **Components:** ingest btree (prune_timestamp initialization logic), checkpoint pick-up, cur_layered.c (cursor open causing prune-timestamp init)
- **Notes:** Two URIs (`layered:test_layered47.a`, `layered:test_layered47.b`). Cursor on `uris[0]` is kept open across checkpoint advances. `uris[1]` cursor opens after checkpoint 2 to trigger the initialization at the wrong LSN.

### `test_layered47.test_checkpoint_order_mismatch`
- **What it tests:** Regression test for WT-15192. The prune-timestamp selection was based on per-table metadata checkpoint order, which is table-local. Different tables can have different orders for the same checkpoint, breaking the logic. This test creates 10 additional checkpoints for one URI only (making its local order 11), then adds a new checkpoint for the other URI (local order 2). The mismatch between orders 11 and 2 previously caused a bug; the test verifies the pick-up succeeds without error.
- **Components:** ingest btree (prune-timestamp selection by checkpoint order), checkpoint pick-up, multi-table metadata checkpoint ordering
- **Notes:** 10 extra leader checkpoints for `uris[0]`, 1 extra for `uris[1]`, then both are picked up in sequence.

### `test_layered47.test_first_gc_with_cursor_on_previous_checkpoint`
- **What it tests:** Verifies that when GC (ingest pruning) is attempted for the first time and there is a cursor pointing to a previous checkpoint, the prune timestamp is set correctly rather than causing an assertion or incorrect GC. Creates 3 checkpoints without opening follower cursors (so ingest GC doesn't run for those), then opens a cursor on checkpoint 4, adds checkpoint 5, and advances. Verifies no error.
- **Components:** ingest btree (first-GC prune-timestamp handling), cursor pinning, checkpoint pick-up
- **Notes:** Single URI. The cursor opens on the most recent (4th) checkpoint and is left open while a 5th checkpoint is picked up.
