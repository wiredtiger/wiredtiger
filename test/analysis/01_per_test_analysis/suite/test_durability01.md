# test_durability01 — Metadata durability after exclusive operations close files

**File:** `test/suite/test_durability01.py`
**Storage mode:** General
**Components under test:** metadata, checkpoint, crash recovery, verify, durability

## Test Cases

### `test_durability01.test_durability`
- **What it tests:** Verifies that the WiredTiger metadata file is checkpointed consistently with data files, so that a crash-copy of the database at any point during repeated update-checkpoint/verify cycles can be reopened and verified successfully. Strategy:
  1. Inserts one record per iteration (100 iterations).
  2. Every 5th iteration: runs a checkpoint; other iterations: runs `verifyUntilSuccess` on the table (which causes an exclusive open that closes and reopens data files).
  3. After each operation: copies the live database directory to `RESTART` (simulating a crash).
  4. Opens `RESTART` in a new connection and runs verify, confirming the metadata is consistent with the copied data files.
- **Components:** `src/meta/`, `src/checkpoint/`, `src/btree/bt_vrfy.c`, `src/conn/conn_open.c`
- **Notes:** The bug scenario is: metadata checkpoint lags behind data file checkpoint, so the crash-copy has newer data files than the metadata knows about, causing verify to fail. Uses `copy_wiredtiger_home` to simulate a crash at any point.
