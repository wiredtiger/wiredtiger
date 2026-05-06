# test_backup05 — fsyncLock-style manual backup with live copy and metadata consistency

**File:** `test/suite/test_backup05.py`
**Storage mode:** General
**Components under test:** backup cursor, metadata flushing, schema (drop/create), filesystem copy

## Test Cases

### `test_backup05.test_backup`
- **What it tests:** Simulates a MongoDB-style `fsyncLock` backup pattern: checkpoints the database, opens a backup cursor (preventing log removal and schema operations), copies the live directory (aligned or unaligned dd), then closes the backup cursor (`fsyncUnlock`). Verifies that: (1) `session.drop()` fails while the backup cursor is open; (2) schema operations succeed after the cursor is closed; (3) the copied directory verifies cleanly when opened. Repeated 100 times with varying alignment every `freq=5` iterations. Captures the expected stdout pattern `recreating metadata`.
- **Components:** `src/cursor/cur_backup.c`, `src/meta/meta_table.c`, `src/schema/schema_drop.c`, `src/os_posix/os_fs.c`
- **Notes:** Uses `copy_wiredtiger_home` to simulate a crash-copy. Parametrized across tiered storage sources.
