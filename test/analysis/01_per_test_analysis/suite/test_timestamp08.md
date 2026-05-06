# test_timestamp08 — Timestamp API: uint variant, all_durable tracking

**File:** `test/suite/test_timestamp08.py`
**Storage mode:** General
**Components under test:** `timestamp_transaction_uint`, `query_timestamp`, `all_durable`, prepared transactions

## Test Cases

### `test_timestamp08.test_timestamp_api`
- **What it tests:** Tests `timestamp_transaction_uint` API: zero timestamp rejected; commit timestamp older than first commit in same txn rejected; commit timestamp < oldest rejected; commit timestamp <= stable rejected; out-of-order commit timestamps across transactions are allowed; read timestamp < oldest rejected; `force` allows moving oldest backward; `oldest_reader` query reflects active read timestamps.
- **Components:** `txn_timestamp.c`, `txn.c`
- **Notes:** Tests both the `_uint` variant and differences from the string-based API.

### `test_timestamp08.test_all_durable`
- **What it tests:** Verifies `query_timestamp('get=all_durable')`: returns 0 before any timestamped commit; reflects last commit after non-prepared commit; drops to (lowest-in-flight - 1) when a lower-timestamp transaction is active; reflects durable timestamp for prepared transactions; returns checkpoint timestamp after checkpoint+reopen.
- **Components:** `txn_timestamp.c`, `txn.c`, `checkpoint.c`
- **Notes:** Tests the full lifecycle of `all_durable` including prepared transactions with separate durable timestamps, multiple commit timestamps within one transaction, and post-checkpoint behavior.
