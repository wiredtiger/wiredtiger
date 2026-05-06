# test_truncate15 — Readonly database reading fast-truncated pages does not cause cache stuck

**File:** `test/suite/test_truncate15.py`
**Storage mode:** General (skipped for disagg — readonly connections not yet supported)
**Components under test:** fast delete, readonly connection, cache eviction, `WT_ROLLBACK` from cache pressure

## Test Cases

### `test_truncate15.test_truncate15`
- **What it tests:** Writes 100,000 rows per key at ts=10; reopens; fast-truncates the middle half at prepare_ts=20, commit_ts=25, durable_ts=30; advances stable to 30; checkpoints; reopens in readonly mode with extremely tight cache (1MB, 90% dirty target/100% trigger); reads the table at ts=10, ts=20, ts=25, and ts=30 and verifies correct visibility. Checks that `WT_ROLLBACK` is NOT returned from cache overflow.
- **Components:** `btree.c`, `evict.c`, `txn_timestamp.c`
- **Notes:** Parameterized over column and integer-row formats. Requires 100,000 rows (50,000 is insufficient to trigger the problem). Tests the regression where a readonly database with a very small cache caused a cache-stuck condition when reading address-deleted pages.
