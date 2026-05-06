# random — RNG determinism and output validation test

**Path:** `test/csuite/random/`
**Language:** C
**Storage mode:** N/A (no WiredTiger database opened)
**Jira ticket:** N/A
**Components under test:** `__wt_random`, `__wt_random_init_default`

## What This Test Does
This test verifies that WiredTiger's internal pseudo-random number generator (`__wt_random`) produces a fixed, deterministic sequence when initialized with the default seed. It checks 35 known-good values at power-of-two call counts (1, 2, 4, ..., 2^34) against hard-coded expected values embedded in the source. The test is explicitly skipped under Antithesis (which may perturb memory or execution order).

## Test Scenarios / Cases

### Scenario: Deterministic output validation
- **What it tests:** That `__wt_random_init_default` plus repeated calls to `__wt_random` produce identical uint32 values at each of 35 checkpoints compared to embedded expected values.
- **Components:** `__wt_random_init_default`, `__wt_random`.
- **Notes:** Any change to the RNG algorithm or initialization logic will break this test, making it a regression guard for the RNG contract.

## LazyFS Variant
None.
