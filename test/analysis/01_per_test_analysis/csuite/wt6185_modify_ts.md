# wt6185_modify_ts — Modify operations with timestamps and eviction correctness

**Path:** `test/csuite/wt6185_modify_ts/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-6185
**Components under test:** `cursor->modify`, timestamp transactions, modify chain reconstruction after eviction and checkpoint, row-store and column-store

## What This Test Does
This test verifies that `cursor->modify` operations applied under snapshot-isolation transactions with commit timestamps produce correct results when re-read at those timestamps, even after page eviction and checkpointing. Each run consists of up to 25 rounds of modify operations (up to 4 modifies per transaction, 90% commit rate) followed by re-reading all previously committed states and optionally evicting the page or taking a checkpoint. Detailed operation traces are maintained so that on failure the exact sequence of modifies, timestamps, and expected/actual values can be printed. The test runs 250 such outer iterations.

## Test Scenarios / Cases

### Scenario: Row-store repeated modify with timestamps, eviction, and checkpoint
- **What it tests:** That after a series of randomly generated modify operations committed at monotonically increasing timestamps, re-reading each committed state at its commit timestamp returns the correct value, even after page eviction (`WT_CURSTD_DEBUG_RESET_EVICT`) or checkpoint.
- **Components:** `cursor->modify`, `session->timestamp_transaction`, `session->commit_transaction`, `session->checkpoint`, forced eviction via debug cursor flag.
- **Notes:** KEYNO=50, MAX_MODIFY_ENTRIES=5, MAX_OPS=25, RUNS=250, VALUE_SIZE=80. 20% chance of eviction per inner iteration, 80% chance of checkpoint (configurable with -c/-e flags). `custom_die` hook dumps operation trace on failure.

### Scenario: Column-store repeated modify with timestamps
- **What it tests:** Same correctness check using a variable-length column-store table (`key_format=r`), activated with the `-C` flag.
- **Components:** Column-store, `cursor->modify` with timestamps, eviction, checkpoint.

## LazyFS Variant
None.
