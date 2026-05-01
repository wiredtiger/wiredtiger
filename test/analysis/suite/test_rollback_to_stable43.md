# test_rollback_to_stable43 — RTS with worker threads restores HS value across multiple tables

**File:** `test/suite/test_rollback_to_stable43.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, worker threads, history store, multiple tables, dryrun

## Test Cases

### `test_rollback_to_stable43.test_rollback_to_stable`
- **What it tests:** Verifies that RTS with 0-4 worker threads correctly restores HS values across 10 tables. Each table gets 1,000 rows with values at ts=10 (valuea), 20 (valueb), 30 (valuec), 40 (valued). Sets stable=20. Checkpoint (non-memory). Calls RTS with optional dryrun. Post-RTS non-dryrun: valueb visible at ts=40, valueb at ts=20, valuea at ts=10. Dryrun: valued still visible at ts=40. Stats: `calls=1`, `keys_removed=0`, `keys_restored=0`, `pages_visited>0`; non-dryrun: `upd_aborted + hs_removed >= nrows*2*ntables`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/checkpoint/`
- **Notes:** Parametrized on key_format (column/row_integer), in_memory, dryrun, worker threads (0/1/2/3/4). `cache_size=100MB`. `extraconfig` is empty string (overridable by subclasses). Unlike other tests, worker_thread_values includes 1/2/3 in addition to 0/4 for finer thread-count coverage.
