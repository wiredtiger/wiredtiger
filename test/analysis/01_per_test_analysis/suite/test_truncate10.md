# test_truncate10 — Fast truncate with durable > commit timestamp handling

**File:** `test/suite/test_truncate10.py`
**Storage mode:** General
**Components under test:** fast delete, prepared transactions, durable timestamp > commit timestamp, stats

## Test Cases

### `test_truncate10.test_truncate10`
- **What it tests:** Writes 10,000 rows at ts=10; reopens the connection to flush memory; truncates the middle half at prepare_ts=20, commit_ts=25, durable_ts=30; optionally advances stable (to 10, 20, 25, or 30) and optionally checkpoints; verifies visibility at ts=10 (all rows), ts=20 (all rows), ts=25 (half rows), ts=30 (half rows). Asserts that at least one fast-delete page was recorded.
- **Components:** `btree.c`, `txn.c`, `txn_timestamp.c`, `checkpoint.c`
- **Notes:** Parameterized over column/row × stable_timestamp (10/20/25/30) × checkpoint. Tests that a truncate prepared+committed with durable > commit behaves correctly at each visibility point. `log=(enabled=false)` to allow fast delete.
