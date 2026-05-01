# test_drop03 — Drop under active transaction returns EBUSY; force=true overrides

**File:** `test/suite/test_drop03.py`
**Storage mode:** General
**Components under test:** schema drop, transaction isolation, EBUSY handling, force flag

## Test Cases

### `test_drop03.test_drop_during_txn`
- **What it tests:** Full scenario covering the interaction between an active (dirty) transaction and `session.drop()`:
  1. Inserts initial values and commits.
  2. Opens a new transaction, updates values.
  3. Calls `drop(uri, "force=false")` while the transaction is active — expects `EBUSY`.
  4. Verifies the table still exists (`confirm_nonempty`) and values reflect the in-progress transaction.
  5. Attempts to commit the now-conflicted transaction — expects rollback error.
  6. After rollback, verifies the original values are restored.
  7. Calls `drop(uri, "force=false")` again on the dirty-but-uncommitted table — expects `EBUSY` again.
  8. Calls `drop(uri, "force=true")` — expects success.
  9. Verifies the table no longer exists (`confirm_does_not_exist`).
  10. Confirms that `drop(force=false)` and `drop(force=true)` on a non-existent table behave as expected (error / success).
- **Components:** `src/schema/schema_drop.c`, `src/txn/`, `src/btree/`
- **Notes:** Reproducces the WT-N scenario described in the test comments. Uses `raisesBusy()` helper to distinguish EBUSY from other errors. The commit-transaction failure after a drop attempt leaves the transaction requiring rollback.
