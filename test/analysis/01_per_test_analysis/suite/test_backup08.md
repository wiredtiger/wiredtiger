# test_backup08 — Backup preserves stable timestamp; recovery timestamp matches checkpoint

**File:** `test/suite/test_backup08.py`
**Storage mode:** General
**Components under test:** backup cursor, timestamp checkpointing, recovery timestamp

## Test Cases

### `test_backup08.test_timestamp_backup`
- **What it tests:** Creates 3 checkpoint-durable collection tables and one oplog table (logged). Writes data to each table with increasing timestamps and takes checkpoints at a stable timestamp slightly behind the data. Uses `use_timestamp=false/true/default` to vary which timestamp is saved per checkpoint. Performs a live backup by copying all files from the backup cursor. Opens the backup directory and queries `get=recovery` timestamp, asserting it matches the expected checkpoint timestamp.
- **Components:** `src/cursor/cur_backup.c`, `src/txn/txn_timestamp.c`, `src/checkpoint/checkpoint.c`
- **Notes:** Parametrized across 3 checkpoint timestamp modes: `use_stable=false` (expected recovery ts = 0), `use_stable=default` (same as true), `use_stable=true` (expected recovery ts = stable ts at checkpoint time). Each table uses `log=(enabled=false)` except oplog.
