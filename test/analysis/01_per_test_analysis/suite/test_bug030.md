# test_bug030 — WT-10522: aborted tombstone with WT_UPDATE_RESTORED_FROM_DS flag causes early return

**File:** `test/suite/test_bug030.py`
**Storage mode:** General
**Components under test:** reconciliation, update chain, aborted tombstone, history store

## Test Cases

### `test_bug030.test_bug030`
- **What it tests:** Reproduces WT-10522 where reconciliation returned early when it encountered an aborted tombstone that still had the `WT_UPDATE_RESTORED_FROM_DS` flag set. The scenario: (1) inserts 10 rows at ts=10, sets oldest/stable to ts=10/20; (2) deletes all rows at ts=30 (unstable, past stable); (3) force-evicts all pages; (4) inserts value_b at ts=50 (uncommitted at stable); (5) checkpoints and reopens (triggering RTS which rolls back the ts=30 delete and ts=50 insert); (6) deletes all rows again at ts=60 (now committed, so visible after RTS); (7) force-evicts again — exercises the path where the update chain has an aborted-tombstone entry with `RESTORED_FROM_DS`. The test uses `debug_mode=(update_restore_evict=true)` and passes if no crash or incorrect reconciliation occurs.
- **Components:** `src/btree/bt_rec.c`, `src/reconcile/rec_write.c`
- **Notes:** Parametrized across `column` (`key_format=r`) and `row_integer` (`key_format=i`). Uses `debug=(release_evict)` cursor.
