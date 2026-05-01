# wt2535_insert_race — Concurrent read-modify-write lost update detection

**Path:** `test/csuite/wt2535_insert_race/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-2535
**Components under test:** Snapshot isolation, concurrent update, optimistic locking retry (`WT_ROLLBACK`), row-store B-tree

## What This Test Does
This test looks for lost updates on a single record shared by many threads. Twenty threads each perform 100,000 read-modify-write operations (read the current integer value, add 1, write back) on a single row-store key under snapshot isolation. On `WT_ROLLBACK`, the operation is retried. At the end, the final value must exactly equal `nthreads * nrecords` (2,000,000). A mismatch indicates a lost update — a correctness bug.

## Test Scenarios / Cases

### Scenario: 20-thread concurrent increment with snapshot isolation
- **What it tests:** That snapshot isolation + `WT_ROLLBACK` retry guarantees that no updates are lost: the final counter value equals the product of thread count and operations per thread.
- **Components:** `session->begin_transaction("isolation=snapshot")`, `cursor->search`, `cursor->update`, `session->commit_transaction`, `WT_ROLLBACK` retry loop.
- **Notes:** All threads synchronize before starting (via `ready_counter`) to maximize contention. Configurable via `-T threads` and `-n records`.

## LazyFS Variant
None.
