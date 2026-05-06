# test_timestamp19 — Oldest timestamp persisted in metadata on restart

**File:** `test/suite/test_timestamp19.py`
**Storage mode:** General
**Components under test:** `oldest_timestamp` persistence across restart, metadata

## Test Cases

### `test_timestamp19.test_timestamp`
- **What it tests:** Sets oldest=stable=10; inserts three waves of updates (timestamps 20, 30, 40); checkpoints at stable=10; advances oldest=stable=40; inserts three more waves (timestamps 50, 60, 70); checkpoints at stable=40; closes and reopens the connection; verifies that trying to set oldest to 10 raises an error (current oldest from metadata is 40); then advances oldest=stable=70.
- **Components:** `txn_timestamp.c`, `meta.c`, `recovery.c`
- **Notes:** Parameterized over integer-row and column formats. Tests that `oldest_timestamp` saved in metadata (from the last checkpoint) becomes the floor for oldest_timestamp on restart.
