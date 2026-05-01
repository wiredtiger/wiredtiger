# thread — Concurrent reader/writer stress test for a single B-tree file

**Path:** `test/thread/`
**Language:** C
**Storage mode:** General
**Components under test:** cursor API (search, update, remove), session lifecycle, B-tree row/variable-length column store, statistics cursor, WAL logging

## Overview

This test loads an initial key set into one or more WiredTiger files and then launches configurable numbers of reader and writer threads that concurrently perform random searches, updates, and removes for a fixed operation count. At completion the files are verified for structural integrity and per-thread operation statistics are printed. It is a foundational concurrency smoke test with no timestamp or transaction logic.

## Test Scenarios / Cases

### Scenario: Shared-file concurrent reads and writes
- **What it tests:** All reader and writer threads operate on the same single file. Readers perform random `cursor->search` operations; writers perform random `cursor->update` (80% of the time) or `cursor->remove` (20%) on the same key space. Neither operation is wrapped in an explicit transaction.
- **Components:** B-tree read/write concurrency, auto-commit transactions, cursor search/update/remove
- **Notes:** Default configuration: 10 readers, 10 writers, 1 000 keys, 10 000 ops each. Row-store (`-t r`) or variable-length column-store (`-t v`) selectable.

### Scenario: Multiple-file mode (file-per-writer)
- **What it tests:** When `-F` is given, each writer thread loads and owns a separate file; reader threads round-robin across the writer files. This exercises concurrent file-handle management across many open tables.
- **Components:** Multi-file session management, per-session cursor ownership, B-tree file open/close
- **Notes:** Combined with `-v` (vary ops), each file gets an operation count that decreases by an order of magnitude per index, stressing the scheduler on tables with very different workload intensities.

### Scenario: Session-per-operation mode
- **What it tests:** When `-S` is given, every single read or write opens a fresh `WT_SESSION` and cursor, performs one operation, then closes the session. This exercises the session/cursor open and close paths under high concurrency.
- **Components:** Session open/close, cursor open/close, connection handle sharing
- **Notes:** Significantly slower than the default shared-session mode; primarily a lifecycle-correctness test.

### Scenario: WAL log printing
- **What it tests:** When `-L` is enabled, every read and write operation issues a `session->log_printf` call that records the thread ID and key. This confirms the log API works under concurrent mixed-mode access.
- **Components:** WAL (logging), `session->log_printf`
- **Notes:** Log is enabled unconditionally in the connection config (`log=(enabled)`).

### Scenario: Post-run statistics dump
- **What it tests:** After all threads finish, connection-level and file-level statistics cursors are iterated and written to a flat file. The test asserts the cursor walk completes without error.
- **Components:** Statistics cursor (`statistics:`, `statistics:<uri>`)
- **Notes:** Verifies that statistics are internally consistent after a concurrent workload.

## Coverage Notes

The thread test provides a lightweight but broad concurrent-access smoke test for the cursor and session APIs without timestamp complexity. Its unique value is the session-per-operation mode (`-S`), which stresses session open/close under contention — a path not well covered by heavier tests. Gaps: no transaction isolation or timestamp testing; no checkpoint during the workload (checkpoint only runs at shutdown); no insert-only or bulk-load phase after initial load; file verification after each run catches B-tree structural issues but does not check data values.
