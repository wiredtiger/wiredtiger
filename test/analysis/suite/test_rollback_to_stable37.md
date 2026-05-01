# test_rollback_to_stable37 — RTS restores stable update when no-timestamp update rewrites HS data

**File:** `test/suite/test_rollback_to_stable37.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, non-timestamp updates, dryrun, eviction

## Test Cases

### `test_rollback_to_stable37.test_rollback_to_stable`
- **What it tests:** Verifies RTS correctly restores the stable value (value_c, a no-timestamp update) after a subsequent timestamped update (value_d@3000) is written past stable. Creates 300 updates at ts=20..319 (value_a+str(i)). Opens a long-running reader at ts=10. Writes value_b@2000, evicts. Writes value_c without timestamp (ts=0), evicts. Writes value_d@3000. Checkpoints. Sets stable=2000 and checkpoints again. Calls RTS (dryrun or real). After RTS: value_c visible at ts=1000 and ts=2000; in non-dryrun value_c also at ts=3000; in dryrun value_d still at ts=3000. `keys_removed == 0`.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/evict/`
- **Notes:** Parametrized on key_format (column/row_integer), dryrun (true/false), worker threads (0/4/8). `cache_size=1GB`, `log=(enabled=false)`. The key scenario: no-timestamp update (value_c) acts as the "stable" anchor; RTS should treat it as the current stable value and remove subsequent timestamped updates.
