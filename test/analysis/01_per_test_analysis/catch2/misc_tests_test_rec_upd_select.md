# test_rec_upd_select — Reconciliation update selection tests

**File:** `test/catch2/misc_tests/test_rec_upd_select.cpp`
**Storage mode:** General
**Components under test:** `__wti_rec_upd_select`, update visibility, reconciliation
**Test type:** Unit

## TEST_CASE_METHOD: "Reconciliation: basic visible update selection" [rec_upd_select]
- **What it tests:** When two updates exist — an older in-memory one and a newer on-disk one — `__wti_rec_upd_select` picks the oldest visible update (not yet on disk) as the update to reconcile.
- **Components:** `__wti_rec_upd_select`, update chain, `WT_RECONCILE`
- **Notes:** The selected update must be visible to the current oldest transaction and not already reflected on disk.

## TEST_CASE_METHOD: "Reconciliation: select non-pruned update (skip below prune timestamp)" [rec_upd_select]
- **What it tests:** Updates with timestamps below the prune timestamp are skipped; `__wti_rec_upd_select` selects the first update at or above the prune threshold.
- **Components:** `__wti_rec_upd_select`, prune timestamp, update chain
- **Notes:** Verifies that pruning works correctly in reconciliation — stale updates are not selected.

## TEST_CASE_METHOD: "Reconciliation: skip aborted and prepared updates" [rec_upd_select]
- **What it tests:** Aborted updates (txnid == WT_TXN_ABORTED) and prepared updates are skipped during reconciliation; the function selects the next eligible update.
- **Components:** `__wti_rec_upd_select`, aborted update filtering, prepared update handling
- **Notes:** Prepared updates are not stable and must not be written to disk during normal reconciliation.
