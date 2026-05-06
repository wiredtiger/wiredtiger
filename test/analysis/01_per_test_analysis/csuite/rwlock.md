# rwlock — Read-write lock correctness and stability under load

**Path:** `test/csuite/rwlock/`
**Language:** C
**Storage mode:** General (WiredTiger connection used for sessions)
**Jira ticket:** HELP-4355
**Components under test:** `__wt_rwlock` (WiredTiger internal read-write lock), optional POSIX `pthread_rwlock_t`

## What This Test Does
This test stress-tests WiredTiger's internal read-write lock (`WT_RWLOCK`) under high concurrency. Up to 1,000 threads each perform one million lock operations, acquiring a write lock every 10,000 reads. A correctness check verifies that a shared counter is updated only under the write lock and never observed in a torn state by readers. A separate dump thread periodically prints lock internals. Timing is reported. This test is also the binary exercised by `time_shift_test.sh` to validate monotonic-clock usage.

## Test Scenarios / Cases

### Scenario: High-concurrency read-mostly locking
- **What it tests:** That the rwlock correctly mediates concurrent readers and infrequent writers, and that readers never observe an inconsistent `shared_counter` value (i.e., the counter is not torn between write increments).
- **Components:** `__wt_readlock`, `__wt_writelock`, `__wt_readunlock`, `__wt_writeunlock`, `__wt_atomic_add_uint64`.
- **Notes:** 100 threads by default; configurable up to 1,000. Each thread does 1 million ops. Correctness check (`CHECK_CORRECTNESS` macro) is compiled in by default.

### Scenario: POSIX fallback (`USE_POSIX` compile flag)
- **What it tests:** The same concurrency model using the standard POSIX `pthread_rwlock_t` instead of the WiredTiger rwlock, serving as a baseline comparison.
- **Components:** `pthread_rwlock_rdlock`, `pthread_rwlock_wrlock`, `pthread_rwlock_unlock`.
- **Notes:** Enabled only when `USE_POSIX` is defined at compile time.

## LazyFS Variant
None. However, `time_shift_test.sh` uses this binary with `libfaketime` to verify that WiredTiger's rwlock implementation uses a monotonic clock (not `CLOCK_REALTIME`), so it does not hang when the system clock shifts backwards.
