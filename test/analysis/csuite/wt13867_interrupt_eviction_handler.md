# wt13867_interrupt_eviction_handler — Eviction interrupt via WT_EVENT_EVICTION handler

**Path:** `test/csuite/wt13867_interrupt_eviction_handler/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-13867
**Components under test:** `WT_EVENT_EVICTION` general event handler, application eviction interruption, `WT_STAT_CONN_APPLICATION_CACHE_OPS`, `WT_STAT_CONN_APPLICATION_CACHE_INTERRUPTIBLE_OPS`, `WT_STAT_CONN_APPLICATION_CACHE_UNINTERRUPTIBLE_OPS`

## What This Test Does
This test verifies that eviction can be interrupted by a user-supplied event handler returning -1 from the `WT_EVENT_EVICTION` callback, and that the correct statistics counters reflect the interruption. It runs two phases: first without interruption (to establish that the event handler is called and that both interruptible and uninterruptible eviction ops occur), then with interruption enabled (to confirm that interruptible ops are blocked while uninterruptible ops still proceed). The test also validates that the eviction event is only triggered for the application session (not internal sessions).

## Test Scenarios / Cases

### Scenario: Eviction without interruption (baseline)
- **What it tests:** That the `handle_general` callback is called with `WT_EVENT_EVICTION` during application-driven cache pressure, and that both `APPLICATION_CACHE_INTERRUPTIBLE_OPS` and `APPLICATION_CACHE_UNINTERRUPTIBLE_OPS` counters increase.
- **Components:** `WT_EVENT_EVICTION`, eviction subsystem, cache pressure from large writes.
- **Notes:** Writes are repeated (doubling `WRITE_CYCLES`) until at least `MIN_CACHE_OPS` (100) total cache operations occur.

### Scenario: Eviction interrupted by event handler
- **What it tests:** That when the event handler returns -1, `APPLICATION_CACHE_INTERRUPTIBLE_OPS` does not increase (eviction is blocked for interruptible paths) but `APPLICATION_CACHE_UNINTERRUPTIBLE_OPS` continues to increase (uninterruptible paths cannot be blocked).
- **Components:** `WT_EVENT_EVICTION` with -1 return, `APPLICATION_CACHE_UNINTERRUPTIBLE_OPS`, `APPLICATION_CACHE_INTERRUPTIBLE_OPS`.
- **Notes:** Also verifies that `my_session` (the only application session) is the sole session receiving eviction events.

## LazyFS Variant
None.
