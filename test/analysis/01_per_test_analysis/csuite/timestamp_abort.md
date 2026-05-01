# timestamp_abort — Timestamp-based crash recovery with optional backup verification

**Path:** `test/csuite/timestamp_abort/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** Timestamp-based recovery, rollback-to-stable, stable/oldest timestamp, logged/unlogged tables, incremental backup, LazyFS

## What This Test Does
This is the most comprehensive crash-recovery test in the csuite. It forks a child process that runs up to 200 worker threads writing to four tables (logged+non-timestamped "local", logged+timestamped "oplog", not-logged+timestamped "collection", not-logged shadow), while a checkpoint thread advances the stable timestamp and commits periodic checkpoints. Optionally, a backup thread creates full and incremental backups. The parent kills the child after at least one checkpoint is verified, then reopens the database, runs recovery, and verifies that each table contains exactly the data expected given the recovered stable timestamp. It also supports a model-based verification mode.

## Test Scenarios / Cases

### Scenario: Row-store crash recovery (default)
- **What it tests:** Full crash-recovery correctness across all four table types: the logged local table must contain all written records up to SIGKILL; the oplog and collection tables must contain records up to the stable timestamp; the shadow table must match the collection table.
- **Components:** Log recovery, RTS, stable timestamp, four table types.
- **Notes:** Uses timing stress (`checkpoint_slow`) and prepare transactions.

### Scenario: Column-store tables (`-c` flag)
- **What it tests:** Same as row-store but with `key_format=r` tables.
- **Components:** Column-store, RTS, timestamp recovery.

### Scenario: Incremental backup integration (`-B` flag)
- **What it tests:** That full and incremental backups taken during the crash workload are valid and can be used for recovery after the crash.
- **Components:** `testutil_backup_create_full`, `testutil_backup_create_incremental`, backup verification.
- **Notes:** Each backup directory is independent; granularity and full-backup interval are configurable.

### Scenario: LazyFS power-failure simulation
- **What it tests:** Recovery after clearing the filesystem page cache, simulating a power failure. Only `fsync`-committed data should be visible.
- **Components:** LazyFS, `method=fsync`, RTS.
- **Notes:** Driven by `smoke_lazyfs.sh`.

### Scenario: Model-based verification (`-M` flag)
- **What it tests:** That the recovered database contents agree with a WiredTiger model's prediction of what should be visible at the recovered stable timestamp.
- **Components:** WiredTiger model, RTS.

## LazyFS Variant
Yes — `smoke_lazyfs.sh` drives the LazyFS variant.
