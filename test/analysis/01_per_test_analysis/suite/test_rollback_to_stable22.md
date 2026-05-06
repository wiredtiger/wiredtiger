# test_rollback_to_stable22 — RTS concurrent with history store eviction under cache pressure

**File:** `test/suite/test_rollback_to_stable22.py`
**Storage mode:** General
**Components under test:** rollback_to_stable, history store, eviction, concurrency

## Test Cases

### `test_rollback_to_stable22.test_rollback_to_stable`
- **What it tests:** Stress test verifying that history store eviction operations do not conflict with or corrupt rollback_to_stable. Creates 10 tables. Runs 1,000 update iterations (100 bytes * 1,000 rows = 100MB total) with periodic RTS calls every 100 iterations. Each RTS sets stable_timestamp slightly below current to roll back the most recent batch. This reliably triggers concurrent HS eviction while RTS is running.
- **Components:** `src/txn/txn_rollback_to_stable.c`, `src/history/`, `src/evict/`
- **Notes:** Parametrized on worker threads (0/4/8) only. `cache_size=100MB` (tight enough to force eviction). `prepare=False`. Row store only (comment explains HS is always row store, VLCS not needed). No explicit assertions beyond not crashing.
