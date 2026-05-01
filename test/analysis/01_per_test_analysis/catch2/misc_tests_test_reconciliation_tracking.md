# test_reconciliation_tracking — Overflow tracking during reconciliation tests

**File:** `test/catch2/misc_tests/test_reconciliation_tracking.cpp`
**Storage mode:** General
**Components under test:** `__ut_ovfl_track_init`, `__ut_ovfl_discard_verbose`, `__ut_ovfl_discard_wrapup`
**Test type:** Unit

## TEST_CASE: "Overflow tracking: init" [reconciliation_tracking]
- **What it tests:** `__ut_ovfl_track_init` initializes the overflow tracking state to empty with zero entries.
- **Components:** `__ut_ovfl_track_init`, `WT_RECONCILE` overflow fields
- **Notes:** Verifies that the initial state has null pointers and zero counts for `ovfl_track`.

## TEST_CASE: "Overflow tracking: discard verbose" [reconciliation_tracking]
- **What it tests:** `__ut_ovfl_discard_verbose` logs the overflow page addresses that would be discarded during reconciliation.
- **Components:** `__ut_ovfl_discard_verbose`
- **Notes:** Diagnostic function; verifies it does not crash or corrupt state when overflow list is non-empty.

## TEST_CASE: "Overflow tracking: discard wrapup" [reconciliation_tracking]
- **What it tests:** `__ut_ovfl_discard_wrapup` processes the discard list and frees the tracked overflow pages, leaving the tracking state clean.
- **Components:** `__ut_ovfl_discard_wrapup`
- **Notes:** After wrapup, the overflow tracking list is empty and no pages remain to be discarded.
