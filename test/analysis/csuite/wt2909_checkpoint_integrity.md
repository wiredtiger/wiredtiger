# wt2909_checkpoint_integrity — Checkpoint integrity under injected write failures

**Path:** `test/csuite/wt2909_checkpoint_integrity/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-2909
**Components under test:** Checkpointing, fail_fs (custom failure file system), checkpoint recovery, two-table consistency, index consistency

## What This Test Does
This test injects write failures during checkpoint operations using the `fail_fs` extension (a custom file system that can be configured to fail after N writes). The "populate" code runs in a subprocess so crashes during injection do not affect the parent. After each sub-run, the parent reopens the database with the normal file system (no failures) and verifies that the two main tables remain mutually consistent and that indices are also consistent. The test performs a binary search to find the optimal N (number of writes before failure) that produces interesting partial-checkpoint states, then runs clusters of tests around that value.

## Test Scenarios / Cases

### Scenario: Binary search calibration for failure point
- **What it tests:** Automatically finds the range of N where a checkpoint can be partially written — small N gives an empty checkpoint, large N gives a complete checkpoint, the target is the boundary where partial checkpoints occur.
- **Components:** `fail_fs` extension, subprocess execution, binary search over write-count parameter.
- **Notes:** Runs TESTS_PER_CALIBRATION (2) tests per calibration step, up to TESTS_WITH_RECALIBRATION (5) rounds.

### Scenario: Row-store checkpoint integrity (default)
- **What it tests:** That after a failed checkpoint and recovery, the two main tables contain the same keys and both point to the same values, and that all index entries are consistent with the primary table.
- **Components:** Row-store tables, indices, `fail_fs`, log recovery.
- **Notes:** Keys are integers; values are multi-field structs. Tables are updated together in one transaction.

### Scenario: Column-store checkpoint integrity
- **What it tests:** Same as row-store but using column-store tables (`key_format=r`).
- **Components:** Column-store, `fail_fs`, log recovery.

## LazyFS Variant
None. This test implements its own failure injection via the `fail_fs` extension.
