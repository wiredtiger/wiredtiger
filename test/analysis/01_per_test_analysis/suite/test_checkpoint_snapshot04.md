# test_checkpoint_snapshot04 — Full vs target backup with open session; compare_backups

**File:** `test/suite/test_checkpoint_snapshot04.py`
**Storage mode:** General
**Components under test:** checkpoint snapshot, backup, full backup vs target backup

## Test Cases

### `test_checkpoint_snapshot04.test_checkpoint_snapshot04`
- **What it tests:** Verifies that a full backup and a target (partial) backup taken with an open session holding an active snapshot produce consistent data, confirmed by `compare_backups`. Tests that the checkpoint snapshot mechanism works correctly when backups are taken with concurrent active sessions.
- **Components:** `src/checkpoint/`, `src/backup/`, `src/txn/txn_ckpt.c`
- **Notes:** Opens a session with a transaction snapshot, takes both a full backup and a target backup of specific tables. After the session/snapshot is closed, compares backup contents to confirm consistency. Uses `take_full_backup` and `compare_backups` helpers from `wtbackup`.
