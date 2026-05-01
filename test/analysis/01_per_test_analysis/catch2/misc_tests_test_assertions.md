# test_assertions — WiredTiger diagnostic assertion system tests

**File:** `test/catch2/misc_tests/test_assertions.cpp`
**Storage mode:** General
**Components under test:** `WT_ASSERT`, `WT_ASSERT_ALWAYS`, `WT_RET_ASSERT`, `WT_ERR_ASSERT`, `WT_RET_PANIC_ASSERT`, `WT_ASSERT_OPTIONAL`, `extra_diagnostics` connection config
**Test type:** Unit

## TEST_CASE: "Assertions off by default" [assertions]
- **What it tests:** With no `extra_diagnostics` config, `WT_ASSERT_OPTIONAL` does not fire even when the condition is false.
- **Components:** `WT_ASSERT_OPTIONAL`, connection flags
- **Notes:** Default behavior — diagnostic assertions are off.

## TEST_CASE: "Assertions on with WT_DIAGNOSTIC_ALL" [assertions]
- **What it tests:** `extra_diagnostics=[all]` enables all assertion categories; `WT_ASSERT_OPTIONAL` fires for any category.
- **Components:** `WT_DIAGNOSTIC_ALL`, `extra_diagnostics`
- **Notes:** Macro-level flag check before calling the assertion.

## TEST_CASE: "Single assertion category enabled" [assertions]
- **What it tests:** Enabling exactly one category (e.g., `checkpoint_validate`) causes assertions for that category to fire and others to be skipped.
- **Components:** Individual diagnostic categories, `WT_ASSERT_OPTIONAL`
- **Notes:** Tests all 10 categories individually.

## TEST_CASE: "Multiple assertion categories enabled" [assertions]
- **What it tests:** Enabling multiple specific categories enables assertions for each of those categories.
- **Components:** `extra_diagnostics`, multiple categories
- **Notes:** Combinations of 2–3 categories verified.

## TEST_CASE: "Disabled assertion category" [assertions]
- **What it tests:** A category not listed in `extra_diagnostics` does not cause `WT_ASSERT_OPTIONAL` to fire.
- **Components:** `WT_ASSERT_OPTIONAL`, category filtering
- **Notes:** Negative test — ensures disabled categories remain silent.

## TEST_CASE: "Reconfigure with empty extra_diagnostics" [assertions]
- **What it tests:** Reconfiguring with an empty `extra_diagnostics=[]` disables all previously-enabled categories.
- **Components:** `conn->reconfigure`, `extra_diagnostics`
- **Notes:** Dynamic reconfiguration path.

## TEST_CASE: "Reconfigure with invalid category" [assertions]
- **What it tests:** Reconfiguring with an unrecognized category name returns an error.
- **Components:** `conn->reconfigure`
- **Notes:** Config validation for unknown category names.

## TEST_CASE: "Reconfigure with valid category" [assertions]
- **What it tests:** Reconfiguring to a specific valid category enables assertions only for that category.
- **Components:** `conn->reconfigure`, `extra_diagnostics`
- **Notes:** Verifies that dynamic enable/disable works correctly.

## TEST_CASE: "Reconfigure transitions (off → on → off)" [assertions]
- **What it tests:** Toggling diagnostic assertions off and on via reconfigure behaves correctly.
- **Components:** `conn->reconfigure`
- **Notes:** Tests all transitions: disabled → enabled → disabled.

## TEST_CASE: "Assertion categories" [assertions]
- **What it tests:** All 10 supported diagnostic assertion category names are recognized and independently controllable: `checkpoint_validate`, `cursor_check`, `disk_validate`, `eviction_check`, `hs_validate`, `key_out_of_order`, `log_validate`, `prepared`, `slow_operation`, `txn_visibility`.
- **Components:** All diagnostic categories
- **Notes:** Exhaustive category coverage test.

## TEST_CASE: "WT_ASSERT_ALWAYS fires regardless of config" [assertions]
- **What it tests:** `WT_ASSERT_ALWAYS` triggers unconditionally (i.e., not gated by `extra_diagnostics`).
- **Components:** `WT_ASSERT_ALWAYS`
- **Notes:** Used in the test/signal-isolation context; not run in normal mode.
