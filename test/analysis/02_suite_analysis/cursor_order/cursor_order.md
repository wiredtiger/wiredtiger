# cursor_order — Concurrent append-insert and reverse-scan ordering test

**Path:** `test/cursor_order/`
**Language:** C
**Storage mode:** General
**Components under test:** cursor append insert, cursor reverse scan (`cursor->prev`), B-tree key ordering invariant, snapshot isolation

## Overview

This test validates that reverse-scanning a B-tree with `cursor->prev` always returns keys in strictly descending order even while concurrent threads are appending new keys at the end of the file. Append-inserter threads atomically increment a shared key counter and insert new records; reverse-scanner threads walk backwards from the current end of the file for a fixed number of steps, asserting at each step that the returned key is strictly less than the previously seen key and that the first key seen is at or above the initial key range. The test terminates once any thread completes its operation count.

## Test Scenarios / Cases

### Scenario: Reverse scan correctness under concurrent appends (row-store)
- **What it tests:** One or more reverse-scanner threads repeatedly reset a cursor to the end of the file, then call `cursor->prev` N times, verifying strict descending order of string keys at each step. Meanwhile, append-inserter threads continuously insert new records with auto-incremented keys.
- **Components:** Row-store B-tree, `cursor->prev`, cursor reset, snapshot isolation session
- **Notes:** Default: 5 reverse scanners, 1 append inserter, 1 000 initial keys, scan depth 10 steps per scan, 1 000 000 ops per thread. Both threads use `isolation=snapshot`.

### Scenario: Reverse scan correctness under concurrent appends (variable-length column-store)
- **What it tests:** Same as the row-store scenario but with `key_format=r` (record-number column store). Key ordering checks compare raw record numbers.
- **Components:** Variable-length column-store B-tree, `cursor->prev`, record-number keys
- **Notes:** Enabled with `-t v`.

### Scenario: Multiple-file mode
- **What it tests:** When `-F` is given, each append-inserter thread owns a separate file; reverse-scanner threads are distributed across writer files. This exercises concurrent cursor ordering across multiple open B-tree files.
- **Components:** Multi-file B-tree concurrency, per-file cursor lifecycle
- **Notes:** Combined with `-v` (vary ops), op counts decrease by an order of magnitude per file index.

### Scenario: First-key range invariant
- **What it tests:** The first key returned by `cursor->prev` (i.e., the largest key currently in the file) must be at or above `initial_key_range - append_inserters`, the key range at the start of the workload phase. This catches stale-snapshot scenarios where a scanner sees an unexpectedly old view of the file.
- **Components:** Snapshot isolation visibility, B-tree page split/merge under concurrent inserts
- **Notes:** The check is `this_key < initial_key_range` on the first `prev` call; any failure calls `testutil_die`.

### Scenario: Post-run structural verification
- **What it tests:** After all threads finish, `session->verify` is called on each file to confirm that the B-tree structure is internally consistent after the concurrent append+scan workload.
- **Components:** B-tree verify
- **Notes:** Runs once per file; structural errors would indicate B-tree corruption from concurrent operations.

## Coverage Notes

The cursor_order test uniquely targets the specific invariant that `cursor->prev` must return keys in strictly descending order even as the tree is concurrently growing at its right-most edge (append inserts). This exercises B-tree page splits, the cursor repositioning logic after a split, and snapshot isolation's interaction with newly visible pages. It does not test timestamps, updates, removes, or random-key inserts. The test terminates when the first thread finishes, so with asymmetric op counts (vary ops) some threads see a much shorter run.
