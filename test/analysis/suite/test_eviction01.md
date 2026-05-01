# test_eviction01 — Eviction of pages with only aborted updates in the update chain

**File:** `test/suite/test_eviction01.py`
**Storage mode:** General
**Components under test:** eviction, btree (update chains), reconciliation

## Test Cases

### `test_eviction01.test_eviction`
- **What it tests:** Populates a table with 100 rows, then in 500 iterations writes large updates (5 KB per key) for all rows within a transaction and rolls it back immediately. After all iterations, asserts that (a) some dirty eviction occurred (`cache_eviction_dirty > 0`) and (b) eviction never stalled due to lack of progress (`cache_eviction_blocked_no_progress == 0`). The goal is to confirm that eviction can successfully process pages whose update chains contain only aborted updates.
- **Components:** `src/evict/`, `src/btree/`, `src/reconcile/`
- **Notes:** `conn_config = 'cache_size=1GB'`. Uses `SimpleDataSet` with key format `S` and value format `u`. Correctness property: eviction makes forward progress and never blocks when all updates are aborted.

### Eviction trigger
- Pressure comes from 500 rounds of large (5 × 100 = 500 B per value) rolled-back updates on 100 rows. All on-page updates are in the aborted state after each rollback.
