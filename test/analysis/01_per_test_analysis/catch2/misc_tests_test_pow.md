# test_pow — Power/log2 math utility function tests

**File:** `test/catch2/misc_tests/test_pow.cpp`
**Storage mode:** General
**Components under test:** `__wt_log2_int`, `__wt_ispo2`, `__wt_rduppo2`
**Test type:** Unit

## TEST_CASE: "__wt_log2_int" [pow]
- **What it tests:** Computes the integer base-2 logarithm (floor) of a given unsigned integer.
- **Components:** `__wt_log2_int`
- **Notes:** Verifies values for 1, 2, 4, 8, 1024, UINT32_MAX, and the edge case 0 (which returns 0 by convention).

## TEST_CASE: "__wt_ispo2" [pow]
- **What it tests:** Returns true if and only if the input is an exact power of two.
- **Components:** `__wt_ispo2`
- **Notes:** Verifies powers of 2 (1, 2, 4, 8, 1024), non-powers (3, 5, 6, 7), and the edge case 0 (returns true by convention — treated as 2^0 = 1 in some implementations).

## TEST_CASE: "__wt_rduppo2" [pow]
- **What it tests:** Rounds a value up to the nearest multiple of a given power of two.
- **Components:** `__wt_rduppo2`
- **Notes:** Verifies already-aligned values (no change), values needing rounding up, and zero. The second argument must itself be a power of two.
