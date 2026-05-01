# wt2719_reconfig — Connection reconfiguration fuzz test

**Path:** `test/csuite/wt2719_reconfig/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-2719
**Components under test:** `conn->reconfigure`, cache, eviction, checkpointing, logging, statistics, verbose, shared cache, file manager

## What This Test Does
This test fuzzes the WiredTiger connection reconfiguration API by applying a large set of individual configuration strings sequentially and then in random combinations. It verifies that every reconfiguration completes without hanging or returning an error. A 60-second alarm is set before each call to catch hangs — reconfiguration starts and stops internal server threads, making deadlocks a plausible failure mode.

## Test Scenarios / Cases

### Scenario: Linear pass through all configuration options
- **What it tests:** That each individual reconfiguration option (cache size, cache overhead, eviction threads/targets/triggers, checkpoint, compatibility, error prefix, file manager, log, shared cache, statistics, verbose) can be applied successfully one at a time.
- **Components:** `conn->reconfigure` with each option from a ~80-element list.
- **Notes:** Each call is guarded by a 60-second alarm.

### Scenario: Random compound reconfiguration
- **What it tests:** That combinations of multiple configuration options applied in a single `reconfigure` call do not hang or fail. Shared-cache and direct-cache options are kept mutually exclusive.
- **Components:** `conn->reconfigure` with randomly concatenated option strings, `__wt_random` for selection.
- **Notes:** The number of additional options per base option is randomly chosen.

## LazyFS Variant
None.
