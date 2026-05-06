# test_rollback_to_stable36 — RTS with fast-truncated page where truncation is not stable

**File:** `test/suite/test_rollback_to_stable36.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, fast-delete, truncate, crash recovery, page instantiation

## Test Cases

### `test_rollback_to_stable36.test_rollback_to_stable36`
- **What it tests:** Verifies RTS correctly undoes a fast-truncate where the truncation is unstable but all other data is stable. Writes value_a@10 to 10,000 rows. Sets stable=10. Reopens connection (clears caches). Truncates rows 50 to nrows-50 at ts=20 (past stable). Checkpoints. Calls RTS (runtime or crash). After RTS: all nrows rows show value_a at ts=15 and ts=25. Checks `rec_page_delete_fast > 0` (confirmed fast-delete happened) and `cache_read_deleted > 0` (page instantiation occurred during RTS).
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/btree/`, `src/truncate/`
- **Notes:** Skipped for tiered. Parametrized on key_format (column/row_integer), crash/runtime, worker threads (0/4/8). Reference `trunc_with_remove=True` variant commented out. RTS verifier as teardown. The truncation is rolled back by instantiating fast-deleted pages and undoing the updates.
