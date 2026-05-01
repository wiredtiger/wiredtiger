# test_rollback_to_stable46 — RTS does not skip clean reconciled pages with unstable on-disk updates

**File:** `test/suite/test_rollback_to_stable46.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, reconciliation, page eviction, clean pages

## Test Cases

### `test_rollback_to_stable46.test_rollback_to_stable`
- **What it tests:** Verifies that RTS does not skip a reconciled, on-disk clean page that has unstable updates. Inserts 5,000 rows at ts=20 (value_a), evicts to trigger reconciliation and write to disk. Then inserts 2,000 more rows at ts=30 (value_b) into subsequent pages. Checkpoints (non-memory). Sets stable=10 (before all updates). Calls RTS. Post-RTS: all rows should be invisible (0 rows at ts=20 and ts=30), as all updates are beyond stable=10.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/btree/`, `src/reconcile/`
- **Notes:** Parametrized on key_format (column with `key_format='i'`/row_integer, both use integer format oddly), in_memory, worker threads (0/4/8). `cache_size=50MB`. The critical case: the first 5,000 rows are in a clean reconciled page (evicted), so RTS must not skip it. Tests the fix for a bug where clean pages were incorrectly skipped.
