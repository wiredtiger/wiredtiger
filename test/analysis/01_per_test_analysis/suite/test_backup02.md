# test_backup02 — Concurrent checkpoints, backup, and inserts under sustained load

**File:** `test/suite/test_backup02.py`
**Storage mode:** General
**Components under test:** backup cursor, checkpoint, concurrent ops, threading

## Test Cases

### `test_backup02.test_backup02`
- **What it tests:** Runs background checkpoint and backup threads concurrently with an insert/update worker thread for a configurable duration (10 seconds normal, 60 seconds long test). Verifies the system remains stable under sustained parallel operation against 3 tables. The test passes if no exception is raised.
- **Components:** `src/cursor/cur_backup.c`, `src/checkpoint/checkpoint.c`, `src/session/session_api.c`
- **Notes:** Uses `wtthread.backup_thread`, `wtthread.checkpoint_thread`, and `wtthread.op_thread`. Work queue feeds 200 insert operations (`gi`), then continuously feeds 200 update operations (`gu`) every 0.1 seconds until the time limit. Only 1 op thread; 3 URIs (`table:test_backup021/2/3`).
