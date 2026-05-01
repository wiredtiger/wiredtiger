# test_checkpoint05 — Checkpoint count does not grow unboundedly with open backup cursor

**File:** `test/suite/test_checkpoint05.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, backup cursor, checkpoint retention

## Test Cases

### `test_checkpoint05.test_checkpoint05`
- **What it tests:** Verifies that the number of retained internal WiredTiger checkpoints does not grow without bound when a backup cursor is open. While a backup cursor is held, checkpoints needed for backup cannot be deleted; once closed, old checkpoints are cleaned up.
- **Components:** `src/checkpoint/`, `src/backup/`
- **Notes:** Opens a backup cursor, performs many checkpoints, closes the backup cursor, performs additional checkpoints, then counts checkpoints in metadata to verify the count is bounded. Guards against unbounded growth of checkpoint metadata.
