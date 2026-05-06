# wt3338_partial_update — Partial (modify) update construction smoke test

**Path:** `test/csuite/wt3338_partial_update/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-3338
**Components under test:** `cursor->modify`, `wiredtiger_calc_modify`, partial update chain reconstruction, row-store and column-store

## What This Test Does
This test smoke-tests the partial update (modify) API by repeatedly applying random modify operations to a 1 KB value buffer and verifying that WiredTiger's modify chain reconstruction matches a locally maintained reference copy. It generates up to 37 modify entries per operation, applies them via the cursor, then reads back the value and compares it byte-by-byte with the expected result. Runs are repeated many times with random seeds to cover a wide range of modification patterns (offsets, sizes, replacement bytes).

## Test Scenarios / Cases

### Scenario: Row-store repeated random modify operations
- **What it tests:** That `cursor->modify` correctly applies partial updates and that the reconstructed full value matches the expected result computed by applying the same modify entries to a local buffer.
- **Components:** `cursor->modify`, `wiredtiger_calc_modify`, row-store B-tree, modify chain.
- **Notes:** Uses `MAX_MODIFY_ENTRIES=37` entries per operation. Replacement bytes cycle through 'Z'-based alphabet. Comparison is byte-by-byte.

### Scenario: Column-store repeated random modify operations
- **What it tests:** Same correctness check using a column-store table (`key_format=r`).
- **Components:** Column-store, `cursor->modify`.

## LazyFS Variant
None.
