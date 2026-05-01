# test_checkpoint02 — Concurrent background checkpoints with multi-threaded inserts

**File:** `test/suite/test_checkpoint02.py`
**Storage mode:** General
**Components under test:** checkpoint subsystem, concurrent access, threading

## Test Cases

### `test_checkpoint02.test_checkpoint02`
- **What it tests:** Verifies that named checkpoints can be created concurrently while multiple threads are actively inserting data, without data corruption or deadlock.
- **Components:** `src/checkpoint/`, `src/btree/bt_split.c`, `src/txn/txn.c`
- **Notes:** Runs a configurable number of writer threads alongside a background checkpoint thread for several seconds. After completion, opens a checkpoint cursor and reads all rows to verify data integrity. Tests that WiredTiger's checkpoint locking is safe under concurrent write pressure.
