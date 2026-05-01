# test_truncate09 — Fast truncate rollback-to-stable: stable vs. unstable truncated ranges

**File:** `test/suite/test_truncate09.py`
**Storage mode:** General (skipped for disagg — disagg does not support RTS)
**Components under test:** fast delete, rollback-to-stable, crash recovery, `simulate_crash_restart`

## Test Cases

### `test_truncate09.test_truncate09`
- **What it tests:** Inserts 80,000 rows; sets stable=100; truncates keys 20,000-40,000 at ts=150 and advances stable to 200; checkpoints (making first truncation stable); truncates keys 50,000-70,000 at ts=250 and removes key 75,000; checkpoints again; simulates crash restart (forcing RTS to ts=200); verifies that key 30,000 (in stable truncation) is not found, key 60,000 (in unstable truncation) is found, and key 75,000 (unstable remove) is found.
- **Components:** `btree.c`, `txn_timestamp.c`, `rts.c`, `recovery.c`
- **Notes:** Parameterized over column and integer-row formats. Uses `simulate_crash_restart` to test that RTS properly rolls back unstable fast-truncate deletions while preserving stable ones.
