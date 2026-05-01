# wt7989_compact_checkpoint — Compact and checkpoint concurrent interaction

**Path:** `test/csuite/wt7989_compact_checkpoint/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-7989
**Components under test:** `session->compact`, `session->checkpoint`, concurrent compact+checkpoint threads, `checkpoint_slow` timing stress, row-store, column-store

## What This Test Does
This test verifies that compaction and checkpointing can run concurrently without deadlock or data loss, and that compaction actually makes measurable progress (pages reviewed > 0, pages rewritten > 0, resulting file size smaller). It runs four scenarios combining two synchronization modes (timing-stress vs. barrier-synchronized) with two storage formats (row and column store). In the timing-stress mode a 10-second sleep is injected before each checkpoint. In the synchronized mode both threads spin until both are ready before starting. After both threads finish, the test asserts compact progress statistics and verifies more than 10% of the file space is reclaimed.

## Test Scenarios / Cases

### Scenario: Row-store — timing stress (checkpoint_slow)
- **What it tests:** That compaction completes successfully while repeated checkpoints are artificially delayed by 10 seconds, verifying no deadlock and that compaction makes progress.
- **Components:** `session->compact`, `session->checkpoint` (CHECKPOINT_NUM=3), `set_timing_stress_checkpoint`, `WT_STAT_DSRC_BTREE_COMPACT_PAGES_REVIEWED`, `WT_STAT_DSRC_BTREE_COMPACT_PAGES_REWRITTEN`.
- **Notes:** NUM_RECORDS=1,000,000. 1/3 of middle records removed before compaction. File size checked for >10% available space.

### Scenario: Row-store — synchronized thread start
- **What it tests:** Same compaction+checkpoint correctness when both threads use a shared `ready_counter` barrier to start simultaneously, increasing the chance of a true concurrent execution window.
- **Components:** `thread_wait()` barrier (spin on `ready_counter`), `thread_func_compact`, `thread_func_checkpoint`.

### Scenario: Column-store — timing stress (checkpoint_slow)
- **What it tests:** Same as row-store timing stress scenario using column-store (`key_format=r, value_format=QQQS`).
- **Components:** Column-store, compact, checkpoint, timing stress.

### Scenario: Column-store — synchronized thread start
- **What it tests:** Same as row-store synchronized scenario using column-store.
- **Components:** Column-store, barrier-synchronized compact+checkpoint threads.

## LazyFS Variant
None.
