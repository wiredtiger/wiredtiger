# readonly — Read-only connection access control test (multi-process)

**Path:** `test/readonly/`
**Language:** C
**Storage mode:** General
**Components under test:** `readonly=true` / `readonly=false` connection configuration, WiredTiger.lock file enforcement, filesystem permission enforcement, multi-process connection sharing, data read-back after clean shutdown

## Overview

This test validates that WiredTiger correctly enforces read-only access semantics across multiple processes and under different filesystem permission configurations. The parent process creates a database, populates it with 10 000 records, shuts down cleanly, then creates four variants of the database directory with different lock-file and permission states. It then opens all four with read-only handles and spawns child processes (via `system()`) that try to open the same databases with both read-only and read-write configurations, checking that the expected success or failure occurs in each case.

## Test Scenarios / Cases

### Scenario: Setup — populate and create directory variants
- **What it tests:** Creates a table with 10 000 uint64/blob records, shuts down, then produces four directory copies:
  - `home` — writable, with lock file (original)
  - `home_wr` — writable, lock file removed
  - `home_rd` — read-only filesystem permissions (chmod 0555 dir, 0444 files), lock file present
  - `home_rd2` — read-only filesystem permissions, lock file removed
- **Components:** `wiredtiger_open`, `session->create`, cursor insert, `conn->close`, filesystem copy, `chmod`
- **Notes:** The lock-file presence/absence and filesystem permissions are the two independent control variables.

### Scenario 1: Parent read-only, child also read-only
- **What it tests:** Parent holds read-only connections to all four directories. Child process opens all four with `readonly=true`. Expected: child fails to open `home` and `home_wr` (lock file prevents a second reader), succeeds on `home_rd` and `home_rd2` (no write lock needed for read-only, and permissions deny write).
- **Components:** `readonly=true`, lock file EWOULDBLOCK, filesystem permissions, multi-process connection
- **Notes:** Child success on read-only directories confirms that multiple concurrent read-only processes are allowed when no lock-file write is needed.

### Scenario 2: Parent read-only, child attempts read-write
- **What it tests:** Same parent state. Child uses `readonly=false`. Expected: child fails on all four directories (parent holds read-only connections with lock files on `home`/`home_wr`; filesystem permissions block write on `home_rd`/`home_rd2`).
- **Components:** Lock file exclusion, filesystem permission error on write open
- **Notes:** Validates that a writable open is blocked when the directory is either locked or read-only.

### Scenario 3: Parent reopens writeable dirs, child read-only
- **What it tests:** Parent closes and reopens `home` and `home_wr` with read-only config (no change to `home_rd`/`home_rd2`). Child attempts read-only opens. Expected: same as Scenario 1.
- **Components:** Connection reopen, read-only re-entry
- **Notes:** Confirms that reopening a writable dir as read-only and then having a child open it read-only behaves the same as the original read-only scenario.

### Scenario 4: Parent reopened, child attempts read-write
- **What it tests:** Same parent state as Scenario 3. Child uses `readonly=false`. Expected: all four fail for the same reasons as Scenario 2.
- **Components:** Lock file, filesystem permissions
- **Notes:** Completes the 2×2 matrix of parent-state × child-intent.

### Scenario: Child data verification
- **What it tests:** When a child process successfully opens a read-only database, it iterates the full cursor and counts records, asserting exactly 10 000 are visible.
- **Components:** Cursor walk, `cursor->next`, read-only data visibility
- **Notes:** This confirms that a read-only connection provides a complete, consistent view of the data written before the clean shutdown.

## Coverage Notes

The readonly test is the only test that directly verifies WiredTiger's multi-process lock-file enforcement semantics and `readonly=true` connection behaviour under real OS filesystem permissions. It uniquely covers the two-process parent+child interaction for all four combinations of parent/child read-only vs. read-write intent, against directories with and without lock files and with and without filesystem write permissions. Gaps: no concurrent writes from the parent while the child is reading; no crash recovery of a read-only database; no read-only testing with timestamps or prepared transactions; spawning a child via `system()` rather than `fork()` avoids shared memory state but means the test cannot easily inspect child internals.
