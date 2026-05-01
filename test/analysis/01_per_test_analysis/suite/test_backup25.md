# test_backup25 — Commit-level durability with later checkpoints while backup cursor is open

**File:** `test/suite/test_backup25.py`
**Storage mode:** General
**Components under test:** backup cursor, logging, commit durability, crash recovery

## Test Cases

### `test_backup25.test_backup25`
- **What it tests:** Populates a logged table until log file 2 exists, checkpoints, writes an uncommitted key, opens a backup cursor, then writes and checkpoints data twice more (bkupkey1, bkupkey2), adds a final uncheckpointed but logged entry (bkupkey3), flushes logs, and copies the live directory (simulating a crash with `WiredTiger.backup` present). Opens the copy and verifies all three bkupkeys are visible, confirming that commit-level durability (log replay) applies even when the backup cursor caused the restore to use the backup-cursor-era checkpoint as the base.
- **Components:** `src/cursor/cur_backup.c`, `src/log/log.c`, `src/conn/conn_open.c`
- **Notes:** Non-parametrized. Expects `'Both WiredTiger.turtle and WiredTiger.backup exist.*'` stdout message when opening the copy.
