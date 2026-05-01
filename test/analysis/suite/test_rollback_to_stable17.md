# test_rollback_to_stable17 — RTS handles updates in both history store and data store

**File:** `test/suite/test_rollback_to_stable17.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, data store, crash recovery, in-memory RTS

## Test Cases

### `test_rollback_to_stable17.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly handles the case where updates span both the history store (HS) and the data store simultaneously. Updates 200 rows at ts=2 (aaaa), 5 (bbbb), 7 (cccc), 9 (dddd). Sets stable=5. For on-disk: checkpoint then crash-restart; for in-memory: calls RTS directly. Post-RTS: rows at ts=2 show "aaaa", rows at ts=5 show "bbbb"; rows at ts=7 and ts=9 remain "bbbb" (HS-restored). Stats: `upd_aborted + hs_removed >= (nrows*2) - 2`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, worker threads (0/4/8). Uses RTS verifier (`verify_rts_logs`) as teardown. `cache_size=200MB`. All updates go to the same 200 rows (not distinct row ranges as in test_rollback_to_stable16).
