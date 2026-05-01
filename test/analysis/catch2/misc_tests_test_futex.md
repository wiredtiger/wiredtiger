# test_futex — Futex wait/wake primitive tests

**File:** `test/catch2/misc_tests/test_futex.cpp`
**Storage mode:** General
**Components under test:** `__wt_futex_wait`, `__wt_futex_wake`, `WT_FUTEX_WAKE_ONE`, `WT_FUTEX_WAKE_ALL`
**Test type:** Unit

## TEST_CASE: "Futex: wake one waiter" [futex]
- **What it tests:** A single thread waiting on a futex is woken by `__wt_futex_wake(WT_FUTEX_WAKE_ONE)`.
- **Components:** `__wt_futex_wait`, `__wt_futex_wake`, `WT_FUTEX_WAKE_ONE`
- **Notes:** Uses a waiter struct and an outcome enum. Waiter thread is spawned, blocks on `__wt_futex_wait`, and is released by the main thread's wake call.

## TEST_CASE: "Futex: timeout one waiter" [futex]
- **What it tests:** A thread waiting on a futex times out if no wake arrives within the specified duration.
- **Components:** `__wt_futex_wait` with timeout
- **Notes:** Waiter outcome is `TIMED_OUT` after the timeout elapses.

## TEST_CASE: "Futex: wake one of two waiters" [futex]
- **What it tests:** `WT_FUTEX_WAKE_ONE` wakes exactly one of two waiting threads; the other remains blocked.
- **Components:** `__wt_futex_wake`, `WT_FUTEX_WAKE_ONE`
- **Notes:** After waking one, a second wake call is needed to release the remaining waiter.

## TEST_CASE: "Futex: wake two of two waiters (WAKE_ALL)" [futex]
- **What it tests:** `WT_FUTEX_WAKE_ALL` wakes both waiting threads simultaneously.
- **Components:** `__wt_futex_wake`, `WT_FUTEX_WAKE_ALL`
- **Notes:** Both waiter outcomes become `WOKEN` after a single wake-all call.

## TEST_CASE: "Futex: wake three waiters separately" [futex]
- **What it tests:** Three waiting threads are each woken individually by successive `WT_FUTEX_WAKE_ONE` calls.
- **Components:** `__wt_futex_wait`, `__wt_futex_wake`, `WT_FUTEX_WAKE_ONE`
- **Notes:** Verifies correct sequential wake behavior when the waiter queue has multiple entries.
