# incr_backup — Randomized incremental backup correctness test

**Path:** `test/csuite/incr_backup/`
**Language:** C
**Storage mode:** General
**Jira ticket:** N/A
**Components under test:** Backup (full and incremental), checkpointing, schema operations (create/drop), log management

## What This Test Does
This test exercises the incremental backup API in a randomized, seed-reproducible manner. It creates a database with up to 100 tables, applies a predictable cycle of insert/update/modify/remove operations, periodically takes full or incremental backups with varying granularity and consolidation settings, and verifies that each backup contains exactly the expected records. The seed can be replayed to reproduce failures.

## Test Scenarios / Cases

### Scenario: Fixed-seed run (0x9b1bde3f111fe316)
- **What it tests:** Exercises the full incremental-backup lifecycle with a known-good seed that does not expose the WT-10551 bitmap bug.
- **Components:** Backup bitmap generation, incremental cursor, table create/drop during backup cycles.
- **Notes:** Run first to ensure correctness under a stable configuration.

### Scenario: Fixed-seed run (123456789)
- **What it tests:** Exercises the same lifecycle with a seed that was historically known to reproduce the WT-10551 incremental bitmap bug if the fix is absent.
- **Components:** Backup bitmap, incremental backup ranges, file rename stress (`backup_rename` timing stress).
- **Notes:** This seed is documented as a regression guard for WT-10551.

### Scenario: Random-seed run
- **What it tests:** Discovers unknown bugs via randomized table counts (up to 100), allocation sizes (512B–16M), log file sizes (100K–20M), checkpoint intervals, and granularity values (1–23 KB/MB).
- **Components:** Full backup, incremental backup, connection close/reopen during backup sequences, table drop-during-backup, consolidate flag variation.
- **Notes:** Seed is printed so failures can be reproduced with `-S <seed>`.

### Scenario: Value-cycle correctness verification
- **What it tests:** After each backup, opens the backup directory and walks every key to confirm that values match what the deterministic insert→update→modify→remove cycle dictates.
- **Components:** `check_table()`, `check_backup()`, cursor iteration, value reconstruction from change count.
- **Notes:** Verifies both key count and byte-level value content.

## LazyFS Variant
None. This test does not include a `smoke_lazyfs.sh`.
