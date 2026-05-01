# test_txn13 — Very large log records: 1GB and 2GB succeed, 4GB fails with EFBIG

**File:** `test/suite/test_txn13.py`
**Storage mode:** General
**Components under test:** log record size limits, `EFBIG` error, large-value transactions

## Test Cases

### `test_txn13.test_large_values`
- **What it tests:** Writes 8 records of `valuesize` bytes each in a single transaction (so total log record is ~8×valuesize); tests three value sizes: 128MB per key (1GB total — expects success), 256MB per key (2GB total — expects success), 512MB per key (4GB total — expects `WiredTigerError` with "exceeds the maximum"). Tagged `@wttest.longtest`.
- **Components:** `log.c`, `txn.c`
- **Notes:** Parameterized over integer-row and column formats × 1gb/2gb/4gb. Requires 20GB cache and `eviction_dirty_trigger=100`. Tests the log record size limit and the specific `EFBIG`-style error for transactions exceeding the maximum log record size.
