# schema_abort — Timestamp-based crash recovery across logged/unlogged/oplog tables with schema ops

**Path:** `test/csuite/schema_abort/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** Timestamp-based recovery, stable timestamp, rollback-to-stable (RTS), logged table, oplog-style table, collection-style table, schema operations (create/drop), LazyFS

## What This Test Does
This test forks a child process that runs multiple worker threads writing the same data to three distinct table types (logged+non-timestamped, logged+timestamped "oplog", not-logged+timestamped "collection") as well as performing schema operations (table create/drop). A checkpoint thread periodically commits checkpoints and advances the stable timestamp. The parent process kills the child after at least one checkpoint is confirmed, then reopens the database, runs recovery, and verifies that each table contains exactly the data expected up to the last stable timestamp. The test validates the combination of stable timestamp, RTS, and log recovery.

## Test Scenarios / Cases

### Scenario: Row-store tables (default)
- **What it tests:** Recovery correctness for all three table types under a row-store schema. Each worker thread's written records are verified against its record file up to the stable timestamp boundary.
- **Components:** Log recovery, rollback-to-stable, stable timestamp, row-store B-tree.
- **Notes:** Uses PREPARE_FREQ to include prepared transactions (~every 5th record).

### Scenario: Column-store tables (`-c` flag)
- **What it tests:** Same as row-store but using column-store (`key_format=r`) tables.
- **Components:** Column-store B-tree, RTS.
- **Notes:** Both row and column modes exercise the same timestamp logic.

### Scenario: Schema operations (table create/drop) integrated
- **What it tests:** That table creation and drop events that occur between checkpoints are properly rolled back or retained after an unclean shutdown, without leaving orphan metadata entries.
- **Components:** Schema create, schema drop, metadata consistency, RTS.
- **Notes:** Multiple threads alternate between inserting data and creating/dropping short-lived tables.

### Scenario: Aggressive sweep (`-A` flag)
- **What it tests:** That enabling aggressive sweep (handle sweeping) alongside the crash workload does not corrupt recovery.
- **Components:** Handle sweep, B-tree eviction, recovery.
- **Notes:** Exercises the interaction between sweep and the stable timestamp.

### Scenario: LazyFS power-failure variant
- **What it tests:** Recovery after a power-failure-style event where the filesystem cache is cleared before reopening. Only data that was `fsync`-ed is visible after recovery.
- **Components:** LazyFS, `method=fsync` transaction sync, RTS.
- **Notes:** Driven by `smoke_lazyfs.sh`.

## LazyFS Variant
Yes — `smoke_lazyfs.sh` drives the LazyFS power-failure scenario.
