# test_scrub_eviction_prepare — Page with prepared update not re-reconciled across multiple checkpoints

**File:** `test/suite/test_scrub_eviction_prepare.py`
**Storage mode:** General
**Components under test:** eviction, prepared transactions, checkpoint, reconciliation, scrub eviction

## Test Cases

### `test_scrub_eviction_prepare.test_scrub_eviction_prepare`
- **What it tests:** Verifies that a btree page containing a prepared (uncommitted) update is not re-reconciled during subsequent checkpoints (scrub eviction should skip it). Creates a table, inserts data, and prepares a transaction touching a specific page. Runs multiple checkpoints. Verifies that `btree_checkpoint_pages_reconciled` stat does not increase for the page with the prepared update across subsequent checkpoints.
- **Components:** `src/evict/evict_scrub.c`, `src/txn/txn_prepare.c`, `src/checkpoint/`, `src/reconcile/`
- **Notes:** Tests the scrub eviction path specifically for prepared transactions. `btree_checkpoint_pages_reconciled` is the key stat. The test confirms that prepared-update pages are correctly excluded from scrub eviction/re-reconciliation.
