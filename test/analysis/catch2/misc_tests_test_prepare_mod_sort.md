# test_prepare_mod_sort — Prepared transaction modification sort tests

**File:** `test/catch2/misc_tests/test_prepare_mod_sort.cpp`
**Storage mode:** General
**Components under test:** `__ut_txn_mod_compare` (via `__wt_qsort`), `WT_TXN_OP`
**Test type:** Unit

## TEST_CASE: "Basic cols and non key'd op" [mod_compare]
- **What it tests:** Sorting two operations — one non-keyed (`WT_TXN_OP_NONE`) on a row-store b-tree and one column op on a col-var b-tree — produces a valid sorted order.
- **Components:** `__ut_txn_mod_compare`, `__wt_qsort`
- **Notes:** Non-keyed operations can appear anywhere in the sorted sequence.

## TEST_CASE: "Basic rows and non key'd op" [mod_compare]
- **What it tests:** Three row-store operations with different keys plus one non-keyed op sort correctly (row ops by key ascending, non-keyed op can appear at any position within the same b-tree).
- **Components:** `__ut_txn_mod_compare`, `WT_TXN_OP_BASIC_ROW`, `WT_TXN_OP_NONE`
- **Notes:** Uses scratch buffer keys allocated with `__wt_scr_alloc`.

## TEST_CASE: "Row, column, and non key'd operations" [mod_compare]
- **What it tests:** A mixed set of 10 operations (column ops, row ops with random keys, non-keyed ops, `WT_TXN_OP_REF_DELETE`) sort to a valid order.
- **Components:** `__ut_txn_mod_compare`, mixed b-tree types
- **Notes:** B-tree IDs must appear in ascending order; within the same b-tree, keys must be in ascending order.

## TEST_CASE: "B-tree ID sort test" [mod_compare]
- **What it tests:** Six operations on six different b-trees (random IDs) all with the same key sort by b-tree ID ascending.
- **Components:** `__ut_txn_mod_compare`, b-tree ID ordering
- **Notes:** Same key across all ops; only b-tree ID varies.

## TEST_CASE: "Keyedness sort test" [mod_compare]
- **What it tests:** Twelve operations across 12 b-trees with mixed keyed/non-keyed types sort such that keyed ops appear before non-keyed ops within the same b-tree.
- **Components:** `__ut_txn_mod_compare`, keyed vs. non-keyed op ordering
- **Notes:** Row ops, column ops, and non-keyed ops are all present.

## TEST_CASE: "Many different row-store keys" [mod_compare]
- **What it tests:** Twelve row-store operations on two b-trees with randomly generated 3-character keys sort correctly by (b-tree ID, key).
- **Components:** `__ut_txn_mod_compare`, row-store key comparison
- **Notes:** Tests collation ordering with random key material.

## TEST_CASE: "Different column store keys test" [mod_compare]
- **What it tests:** Eight column-store operations on six b-trees with random recnos sort by (b-tree ID, recno).
- **Components:** `__ut_txn_mod_compare`, `WT_TXN_OP_BASIC_COL`, recno ordering
- **Notes:** Does not require a session (no keys to allocate).
