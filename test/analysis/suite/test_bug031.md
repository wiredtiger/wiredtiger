# test_bug031 — WT-10717: missing update when constructing update list with WT_UPDATE_RESTORED_FROM_DS

**File:** `test/suite/test_bug031.py`
**Storage mode:** General
**Components under test:** reconciliation, history store, update restore evict, RTS

## Test Cases

### `test_bug031.test_bug031`
- **What it tests:** Reproduces WT-10717 (discovered with WT-10522 reverted) where an update at ts=10 could be lost from the history store when evicting a page that has an aborted update with `WT_UPDATE_RESTORED_FROM_DS` above it. Sequence: (1) insert at ts=10, set stable=ts=10; (2) remove at ts=20; (3) force-evict (DS has insert@10 with stop@20); (4) re-insert at ts=30, checkpoint; (5) reopen (RTS rolls back ts=20 and ts=30, leaving only insert@10 in DS); (6) begin uncommitted insert, force-evict (update-restore evict writes only insert@10 to DS, leaving aborted updates in memory); (7) commit at ts=40; (8) force-evict again — without WT-10522, the early exit in `__rec_append_orig_value` (triggered by the `RESTORED_FROM_DS` flag on the aborted ts=20 update) skips restoring insert@10 to HS. Validates that a read at ts=10 succeeds and returns 0.
- **Components:** `src/reconcile/rec_write.c`, `src/history/hs_cursor.c`
- **Notes:** Parametrized across `column` (`key_format=r`) and `row_integer` (`key_format=i`). Uses `debug=(release_evict)` cursor.
