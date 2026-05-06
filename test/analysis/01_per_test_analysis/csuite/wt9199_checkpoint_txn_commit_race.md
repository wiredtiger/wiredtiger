# wt9199_checkpoint_txn_commit_race — Checkpoint vs. transaction commit timestamp race

**Path:** `test/csuite/wt9199_checkpoint_txn_commit_race/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-9199
**Components under test:** Checkpoint, transaction commit with timestamps, `commit_transaction_slow` timing stress, `prepare_checkpoint_delay` timing stress, stable timestamp ordering

## What This Test Does
This test reproduces a race where a checkpoint could miss a transaction's updates even though the commit timestamp was before the checkpoint's stable timestamp. The window occurs when: (1) a transaction checks timestamp validity and then sleeps (via `commit_transaction_slow` stress), (2) another thread advances the stable timestamp past the commit timestamp, and (3) a checkpoint runs at the new stable timestamp, selecting a stable that precedes the eventual commit. The test inserts 1,000 records in a transaction with commit_timestamp=70 (= initial stable 50 + 20), while the checkpoint thread sets stable=70 (+20) and then calls checkpoint. The expected outcome is that `commit_transaction` returns EINVAL because the commit timestamp would violate the stable timestamp ordering — verifying the race condition is caught rather than silently producing incorrect data.

## Test Scenarios / Cases

### Scenario: Commit timestamp race with stable timestamp advancement under timing stress
- **What it tests:** That committing a transaction with a commit timestamp equal to or less than the stable timestamp correctly returns `EINVAL`, even when `commit_transaction_slow` and `prepare_checkpoint_delay` timing stresses introduce delays that widen the race window.
- **Components:** `session->commit_transaction(commit_timestamp)`, `conn->set_timestamp(stable_timestamp)`, `session->checkpoint`, `timing_stress_for_test=[commit_transaction_slow, prepare_checkpoint_delay]`.
- **Notes:** NUM_RECORDS=1000. insert thread sets stable=50, signals checkpoint thread, then tries to commit at ts=70. Checkpoint thread also sets stable to 70, waits 2 seconds, then checkpoints. The commit at ts=70 is expected to fail with EINVAL since stable was advanced to 70 first.

## LazyFS Variant
None.
