# test_timestamp04 — rollback_to_stable visibility rules

**File:** `test/suite/test_timestamp04.py`
**Storage mode:** General
**Components under test:** `rollback_to_stable`, `stable_timestamp`, logged vs. non-logged tables, eviction

## Test Cases

### `test_timestamp04.test_rollback_to_stable`
- **What it tests:** Creates four tables (logged+timestamps, not-logged+timestamps, logged+no-timestamps, not-logged+no-timestamps); inserts 10,000 keys with value=1 at timestamp=key; sets stable_timestamp=key_range/2; checkpoints; calls `rollback_to_stable`; verifies: non-timestamped tables retain all data, non-logged timestamp table rolls back to stable, logged timestamp table behavior depends on whether connection log is enabled (log=on keeps all; log=off rolls back). Then updates all keys to value=2 at timestamp=key+key_range, advances stable to 1.25×range, rolls back again, and verifies partial rollback. Checks `txn_rts`, `txn_rts_upd_aborted`, `txn_rts_hs_removed`, `txn_rts_keys_removed` statistics.
- **Components:** `txn_rollback_to_stable.c`, `txn_timestamp.c`, `log.c`
- **Notes:** Manages its own connection with cache size 20MB or 2MB and optional log config. Parameterized over 3 connection configs (no-log, log V1, log V2) × 3 table types (row, row-smallcache, VLCS).
