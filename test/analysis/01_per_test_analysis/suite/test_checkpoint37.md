# test_checkpoint37 — Reconciliation removes obsolete updates with skip_update_obsolete_check

**File:** `test/suite/test_checkpoint37.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, reconciliation, obsolete update removal, statistics

## Test Cases

### `test_checkpoint37.test_checkpoint37`
- **What it tests:** Verifies that with `skip_update_obsolete_check=true` disabled (i.e., check enabled), reconciliation during checkpoint removes obsolete update chains from pages and increments `cache_obsolete_updates_removed` to a value greater than zero.
- **Components:** `src/reconcile/rec_write.c`, `src/reconcile/rec_visibility.c`, `src/checkpoint/`
- **Notes:** Populates a table with multiple update rounds at increasing timestamps. Advances `oldest_timestamp` to make early updates obsolete. Runs a checkpoint and reads `stat.conn.cache_obsolete_updates_removed`. Asserts the count is positive, confirming that reconciliation's obsolete-update removal path is exercised during the checkpoint. Tests the integration of `skip_update_obsolete_check` eviction configuration with checkpoint reconciliation.
