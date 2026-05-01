# test_bulk02 — Bulk-load with checkpoint, backup, and transaction constraints

**File:** `test/suite/test_bulk02.py`
**Storage mode:** General
**Components under test:** bulk cursor, checkpoint, backup, transaction isolation

## Test Cases

### `test_bulkload_checkpoint.test_bulkload_checkpoint`
- **What it tests:** Verifies that checkpoints skip tables with an open bulk cursor. Opens a bulk cursor, inserts 9 records, then checkpoints 4 times (named or unnamed). Closes the bulk cursor. For named checkpoints, asserts that opening a cursor on the checkpoint (`checkpoint=myckpt`) raises `WiredTigerError` — the table was not included in any checkpoint taken while the bulk cursor was open.
- **Components:** `src/cursor/cur_bulk.c`, `src/checkpoint/checkpoint.c`
- **Notes:** Parametrized across `types` (file, table) × `configs` (var recno/S, row S/S) × `ckpt_type` (named, unnamed) = 8 combinations.

### `test_bulkload_backup.test_bulk_backup`
- **What it tests:** Verifies that a full backup taken while a bulk cursor is open does not include the bulk-load data. Opens a bulk cursor, inserts 9 records, optionally checkpoints (named, unnamed, or none), then runs a full backup using the `wt` backup command. Opens the backup directory and asserts that the table is empty (`cursor.next()` returns `WT_NOTFOUND`).
- **Components:** `src/cursor/cur_bulk.c`, `src/backup/backup.c`
- **Notes:** Parametrized across `types` (file, table) × `configs` (var, row) × `ckpt_type` (named, none, unnamed) × `session_type` (same session, different session) = 24 combinations.

### `test_bulk_checkpoint_in_txn.test_bulk_checkpoint_in_txn`
- **What it tests:** Skeleton test for the previously observed crash when a checkpoint is taken while both a bulk cursor and a transaction are open. The active test body is commented out; currently only creates a table and begins a transaction without any assertions.
- **Components:** `src/cursor/cur_bulk.c`
- **Notes:** Non-parametrized. The body is commented out as documentation of a historical crash scenario.

### `test_bulk_checkpoint_in_txn.test_bulk_cursor_in_txn`
- **What it tests:** Verifies that opening a bulk cursor inside an active transaction raises `WiredTigerError` with `Bulk cursors can't be opened inside a transaction`.
- **Components:** `src/cursor/cur_bulk.c`, `src/txn/txn_api.c`
- **Notes:** Non-parametrized.
