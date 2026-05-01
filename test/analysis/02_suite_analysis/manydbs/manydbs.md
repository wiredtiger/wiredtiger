# manydbs — Multiple concurrent WiredTiger connections with condition-variable idle check

**Path:** `test/manydbs/`
**Language:** C
**Storage mode:** General
**Components under test:** multiple concurrent `WT_CONNECTION` handles, condition-variable scheduling, WAL logging (`transaction_sync`), statistics cursor, idle connection behavior

## Overview

This test opens up to 10 independent WiredTiger databases (each in its own subdirectory) simultaneously in the same process, optionally writes a small random workload to a random subset of them, waits 30 seconds, and then checks the `WT_STAT_CONN_COND_AUTO_WAIT_RESET` statistic against `WT_STAT_CONN_COND_AUTO_WAIT` for each connection. The key assertion is that condition-variable spurious wakeups (resets) are rare: zero when completely idle, and no more than 5% of total waits under a light workload.

## Test Scenarios / Cases

### Scenario: Idle connections — no spurious condition-variable wakeups
- **What it tests:** With `-I` (idle mode), all connections are opened and held open for 30 seconds with zero user activity. The test asserts that `cond_reset` did not increase by more than `CV_RESET_THRESHOLD_IDLE` (0 on Linux/most platforms, 20 on macOS/Windows/NetBSD) above its value at startup.
- **Components:** Internal thread scheduling, condition variables, connection idle state
- **Notes:** This directly tests WT-internal fix for extraneous wakeups (WT-2336 era). Platform thresholds account for known OS-level spurious wakeup bugs.

### Scenario: Light write workload — bounded condition-variable resets
- **What it tests:** Without `-I`, the test writes 100 key/value pairs to a random 25% of the open databases on each 5-second interval (6 intervals total). After 30 seconds the test asserts that `cond_reset / cond_wait <= 1/20` (5%) for each connection.
- **Components:** Condition variables under light write workload, WAL write-behind (`transaction_sync`), cursor insert
- **Notes:** The three connection configurations cycled across databases are: sync disabled, sync=none, sync=fsync. This exercises the WAL coalescing logic under all three sync modes.

### Scenario: Rotating transaction-sync configurations
- **What it tests:** Each database is opened with one of `WT_CONFIG0` (sync disabled), `WT_CONFIG1` (sync=none), or `WT_CONFIG2` (sync=fsync), assigned round-robin. All three configurations coexist in the same process.
- **Components:** WAL transaction_sync modes, per-connection logging configuration
- **Notes:** Verifies that multiple connections with different sync policies do not interfere with each other's scheduling behaviour.

### Scenario: Configurable database count
- **What it tests:** With `-D N`, opens N databases (default 10, max `MAX_DBS`=10 in the source, but the flag allows any count). Tests that all N connections can coexist and that the idle/reset assertions hold across all of them.
- **Components:** Connection multiplexing, per-connection directory management
- **Notes:** Each database gets its own `WT_TEST/WT_TEST.N` subdirectory.

## Coverage Notes

The manydbs test uniquely validates WiredTiger's internal condition-variable scheduling quality: it is the only test that directly measures spurious-wakeup rate as a correctness criterion. It also stresses multi-connection coexistence in a single process under different logging configurations. Gaps: no checkpoint during the run; no crash or recovery testing; no read workload (only writes); the data written is never verified for correctness (the test is purely about scheduling behaviour); the 10-database limit is hardcoded and relatively small.
