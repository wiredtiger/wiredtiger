# wt3363_checkpoint_op_races — Checkpoint operation race detector

**Path:** `test/csuite/wt3363_checkpoint_op_races/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-3363
**Components under test:** Checkpoint, schema operations (bulk load, create, drop, cursor), handle locking, timing stress

## What This Test Does
This test verifies that a set of schema and cursor operations do not block against a concurrently running checkpoint, even when a large artificial delay (`checkpoint_slow` timing stress, ~10 seconds) is injected into each checkpoint. Ten worker threads continuously execute random operations (bulk load, create, cursor ops, drop, unique variants) while a dedicated checkpoint thread runs forced checkpoints every second. A monitor thread aborts with a core dump if any worker's operation counter fails to advance within 5 seconds (half the checkpoint delay), indicating a blocked operation.

## Test Scenarios / Cases

### Scenario: Schema operations vs. delayed checkpoint (row-store)
- **What it tests:** That `op_bulk`, `op_create`, `op_cursor`, `op_drop`, `op_bulk_unique`, and `op_create_unique` all complete promptly and do not deadlock or block against a checkpoint protected by `timing_stress_for_test=[checkpoint_slow]`.
- **Components:** `session->checkpoint(force)`, `op_bulk`, `op_create`, `op_drop`, `op_cursor`, schema handle locking, `checkpoint_slow` timing stress.
- **Notes:** N_THREADS=10, MAX_EXECUTION_TIME=10s delay, RUNTIME=900s (15 minutes). Only runs when `TESTUTIL_ENABLE_TIMING_TESTS` is set. Monitor aborts if any thread is stuck for more than MAX_EXECUTION_TIME/2 seconds.

## LazyFS Variant
None.
