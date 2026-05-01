# test_prepare23 — Eviction split failure during prepare rollback with rollback_to_stable

**File:** `test/suite/test_prepare23.py`
**Storage mode:** General
**Components under test:** prepared transactions, eviction split, rollback, rollback_to_stable, timing stress

## Test Cases

### `test_prepare23.test_prepare23`
- **What it tests:** Runs 1,000 iterations of: commit two values at timestamps 1 and 2, prepare a third update, attempt to evict (split fails due to `failpoint_eviction_split`), rollback the prepare, run rollback_to_stable, verify values; optionally uses a delete (tombstone) instead of an update for the prepared operation
- **Components:** `txn/txn_prepare.c`, `txn/txn_rollback.c`, `evict/evict_page.c`, `btree/bt_rec.c`, `rts/rts.c`
- **Notes:** Scenarios: column/integer-row × delete/non-delete; uses `failpoint_eviction_split` timing stress to force eviction to fail mid-split; the 1,000-iteration loop exercises the memory reclamation and state cleanup after a failed eviction+rollback cycle; after RTS, only data at or before stable_ts should be visible; guards against memory leaks or corruption from repeated failed evictions
