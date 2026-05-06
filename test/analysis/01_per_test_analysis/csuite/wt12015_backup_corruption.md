# wt12015_backup_corruption — Backup ID integrity after crash before turtle file update

**Path:** `test/csuite/wt12015_backup_corruption/`
**Language:** C
**Storage mode:** General
**Jira ticket:** WT-12015
**Components under test:** Incremental backup, backup IDs, checkpoint crash recovery, turtle file update, `backup:query_id` cursor

## What This Test Does
This test verifies that WiredTiger correctly handles backup IDs when it crashes during a checkpoint, specifically right before the turtle file (WiredTiger.turtle) is renamed/updated. Two scenarios are exercised: crashing during the checkpoint after an incremental backup, and crashing during `backup_force_stop` after an incremental backup. In both cases, the parent process reopens the database, performs additional work, creates another backup from the available (possibly minimum or maximum) backup ID, and verifies the backup is self-consistent.

## Test Scenarios / Cases

### Scenario 1: Crash during checkpoint after incremental backup (`SCENARIO_TEST_BACKUP`)
- **What it tests:** That after a crash during a checkpoint (injected via `checkpoint_fail_before_turtle_update` debug flag) following a series of full+incremental backups, reopening the database correctly exposes valid backup IDs via `backup:query_id`, and a new incremental backup built from the minimum available ID is self-consistent.
- **Components:** `testutil_backup_create_full`, `testutil_backup_create_incremental`, `backup:query_id`, checkpoint fail-point, recovery, `verify_backup`.
- **Notes:** 3 incremental backups are created before the injected crash. Uses `fork` + `SIGCHLD` to detect the expected crash.

### Scenario 2: Crash during force-stop after incremental backup (`SCENARIO_TEST_FORCE_STOP`)
- **What it tests:** Same crash injection point, but the crash happens during `backup_force_stop` rather than during a regular checkpoint. After recovery the test tries from the maximum available backup ID.
- **Components:** `testutil_backup_force_stop`, backup ID recovery, incremental backup.
- **Notes:** If no backup IDs are found after recovery (EINVAL from query_id cursor), falls back to a full backup.

## LazyFS Variant
None.
