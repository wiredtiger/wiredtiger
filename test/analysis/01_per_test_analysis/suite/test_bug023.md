# test_bug023 — WT-5930: backup + compatibility error leaves database in bad state

**File:** `test/suite/test_bug023.py`
**Storage mode:** General (logging enabled, `config_base=false`)
**Components under test:** backup cursor, compatibility, connection open, data recovery

## Test Cases

### `test_bug023.test_bug023`
- **What it tests:** Reproduces WT-5930 where opening a backup database with a compatibility error left the database in an incorrect state, causing the next successful open to lose data. Creates a table, inserts 10 records (checkpoint), inserts 10 more (no checkpoint, to be recovered via WAL), takes a full backup to `backup.dir/`, closes the primary connection. Then attempts to open the backup directory with `require_min=3.3.0` — asserts this raises `WiredTigerError` with `Version incompatibility detected`. Finally, opens the backup directory with `require_min=3.2.0` (the correct setting) and verifies that all 20 records are present.
- **Components:** `src/conn/conn_open.c`, `src/backup/backup.c`, `src/log/log_recover.c`
- **Notes:** Non-parametrized. Connection configured with `compatibility=(release=3.2.0)`. Uses `backup_base` helper for `take_full_backup`.
