# test_layered38 — Garbage collection of redundant content in the ingest table

**File:** `test/suite/test_layered38.py`
**Storage mode:** Disagg/Layered
**Components under test:** ingest btree, eviction, checkpoint pick-up, cursor pinning, GC (prune timestamp), Oplog helper

## Test Cases

### `test_layered38.test_gc_ingest_table`
- **What it tests:** Verifies that after all ingest-table data is subsumed into the stable btree (via checkpoint pick-up), eviction can remove all ingest table rows — but only after the cursor that was pinning them is closed and another checkpoint is picked up. Insert 1000 records into the ingest table on a follower, take a checkpoint, advance it to the follower, then trigger eviction. Verifies that items remain while a cursor is open, and are removed (count becomes 0,0) once the cursor is closed and a new checkpoint is picked up.
- **Components:** ingest btree (GC, prune timestamp), eviction (`debug=(release_evict)`), checkpoint pick-up, stable btree, cur_layered.c, Oplog (test_layered23 import)
- **Notes:** 1000 items, value_size=500. Uses `count_ingest` helper to count data vs tombstone entries directly in `file:test_layered38.wt_ingest`.

### `test_layered38.test_gc_ingest_table_with_remove`
- **What it tests:** Verifies GC behaviour when ingest table contains both inserts (at an older timestamp) and removes (tombstones at a newer timestamp). Inserts and immediately removes the same 1000 records. After the first checkpoint pick-up with a cursor pinning old content: the inserts are redundant but the removes must remain (they are not in the stable table yet). After the stable timestamp is advanced and a new checkpoint subsumes the removes, all tombstones can also be GC'd once the cursor is closed.
- **Components:** ingest btree (tombstone GC), prune timestamp, checkpoint pick-up, stable btree, eviction
- **Notes:** Complex multi-phase test. Verifies distinct item counts at two timestamps (insert-timestamp and remove-timestamp) using `count_ingest(session, ts)` and `count_ingest(session)`. Includes `session_follow.breakpoint()` call for debugging.

### `test_layered38.test_gc_ingest_with_cursor`
- **What it tests:** Verifies that GC of redundant ingest data is blocked while a cursor is open on the first picked-up checkpoint, and succeeds once the cursor is closed and a subsequent checkpoint is picked up.
- **Components:** ingest btree (GC gate), cursor pinning, checkpoint pick-up
- **Notes:** Single URI, 1000 items, straightforward insert-then-GC scenario with cursor pin. The cursor must be closed before GC can proceed.

### `test_layered38.test_gc_ingest_with_no_open_cursor`
- **What it tests:** Verifies that when no cursor is open during checkpoint pick-up, the ingest table is garbage-collected immediately (no cursor pin blocking).
- **Components:** ingest btree (GC, prune timestamp), checkpoint pick-up
- **Notes:** Simplest GC test. After a single checkpoint pick-up with no open cursors, eviction should reduce the ingest count to (0,0).
