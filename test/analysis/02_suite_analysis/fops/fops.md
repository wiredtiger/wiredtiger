# fops — Concurrent schema-operation stress test (create, drop, checkpoint, verify, bulk, cursor)

**Path:** `test/fops/`
**Language:** C
**Storage mode:** General
**Components under test:** schema operations (create, drop), checkpoint, verify, bulk cursor, open_cursor, file and table URIs, transaction wrapping of schema ops

## Overview

Multiple threads concurrently and randomly issue schema-level operations — `session.create`, `session.drop`, `session.checkpoint` (forced), `session.verify`, and `session.open_cursor` (including bulk cursors) — against a shared URI. The test iterates over both `file:` and `table:` URI types and is designed to expose races between concurrent schema operations, eviction, and the metadata layer. No data is actually read or written through cursors; the test focuses exclusively on metadata/schema correctness under concurrency.

## Test Scenarios / Cases

### Scenario: Concurrent schema operations on a shared URI (file: and table:)
- **What it tests:** N threads simultaneously pick one of eight operations at random: `obj_bulk`, `obj_create`, `obj_cursor`, `obj_drop`, `obj_checkpoint`, `obj_verify`, `obj_bulk_unique`, `obj_create_unique`. Expected errors (ENOENT, EBUSY, EEXIST) are explicitly tolerated; any other error fails the test. Both `file:wt` and `table:wt` URIs are exercised in successive sub-runs.
- **Components:** Schema layer, metadata lock, B-tree create/drop, checkpoint engine, verify, bulk cursor
- **Notes:** Default: 10 threads, 1 000 ops each, 1 run. The `cache_size=5MB` is intentionally small to provoke eviction races. `operation_tracking` is disabled to keep output readable.

### Scenario: Bulk cursor on shared URI
- **What it tests:** `obj_bulk` tries to create the URI (if it does not exist) and then immediately open a bulk cursor. Because other threads may be concurrently dropping or creating the file, this exercises the race between bulk-cursor open and concurrent schema changes.
- **Components:** Bulk cursor, concurrent create/drop, metadata lock
- **Notes:** `ENOENT` and `EBUSY` are silently ignored. An error from trying to bulk-load a non-empty file (`bulk-load is only supported on newly created`) is also suppressed via the event handler.

### Scenario: Unique-URI create+drop cycle
- **What it tests:** `obj_bulk_unique` and `obj_create_unique` each generate a fresh unique URI (using an atomically incremented counter), create it, optionally open a bulk cursor, then drop it (retrying on `EBUSY`). This exercises rapid create/drop of distinct URIs concurrently with operations on the shared URI.
- **Components:** Schema create, schema drop (with and without `force`), metadata atomicity
- **Notes:** `force` drop is randomly selected. A bulk cursor opened on the unique URI may encounter `EINVAL` if a forced checkpoint raced and created a checkpoint of the empty file.

### Scenario: Forced checkpoint under schema contention
- **What it tests:** `obj_checkpoint` issues `session.checkpoint("force")` while other threads are simultaneously creating, dropping, opening cursors, and verifying. `EBUSY` and `ENOENT` are tolerated.
- **Components:** Checkpoint (forced), metadata lock contention, schema operation ordering
- **Notes:** The event handler suppresses "forced or named checkpoint" messages to reduce noise.

### Scenario: Transactional schema operations
- **What it tests:** When `-x` is passed, every schema operation is wrapped in `begin_transaction` / `commit_transaction` (or `rollback_transaction` on error). This validates that schema operations can be made transactional and that rollback is correctly handled when a schema operation returns `ENOENT` or `EBUSY`.
- **Components:** Transaction-wrapped schema operations, metadata transactionality
- **Notes:** `EINVAL` on commit is tolerated (can occur if the transaction was already invalidated by a conflicting schema change).

## Coverage Notes

The fops test uniquely covers the concurrent metadata/schema layer under mixed create, drop, checkpoint, verify, and cursor-open operations — a combination rarely exercised by data-path tests. It is particularly effective at finding locking bugs or use-after-free errors in the schema and metadata subsystems. Gaps: no data is inserted or read, so data-path correctness is not verified; there is no timestamp or isolation testing; only simple unkeyed `file:` and `table:` URIs are used (no column-store, LSM, or tiered URIs).
