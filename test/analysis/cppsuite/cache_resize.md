# cache_resize — Tests transaction rejection and commit behaviour under dynamic cache size changes

**File:** `test/cppsuite/tests/cache_resize.cpp`
**Storage mode:** General
**Components under test:** `conn->reconfigure()` cache resizing, transaction management under cache pressure, custom operation tracker, MVCC

## Overview

This test validates the interaction between live cache resizing and large transactions. A custom-operation thread continuously toggles the connection cache size between 1 MB and 500 MB. Simultaneously, insert threads attempt to write transactions whose data footprint is larger than 1 MB but smaller than 500 MB. The expectation is that transactions attempted when the cache is small (1 MB) are rejected, while those attempted when the cache is large (500 MB) succeed. A custom operation tracker records (timestamp, txn_id) → (op_type, cache_size) to allow post-run validation that all committed transactions occurred when the cache was sufficiently large. Validation is partially implemented but some assertions are temporarily disabled pending WT-12931.

## Configuration

**Config file:** `test/cppsuite/configs/cache_resize_default.txt`

| Parameter | Value | Notes |
|---|---|---|
| `duration_seconds` | 15 | |
| `cache_size_mb` | 500 | Initial cache size |
| `cache_max_wait_ms` | 1 | Short wait before giving up on cache-full operations |
| `timestamp_manager.enabled` | false | Not needed for this test |
| `custom_config.thread_count` | 1 | Cache resize thread |
| `custom_config.op_rate` | 10s | Resize interval |
| `insert_config.thread_count` | 5 | |
| `insert_config.key_size` | 1,000,000 | 1 MB keys to create large transactions |
| `insert_config.op_rate` | 3s | |
| `insert_config.ops_per_transaction` | min=2, max=2 | Fixed 2 ops per txn (~2 MB per txn) |
| `collection_count` | 1 | Single empty collection |
| `operation_tracker.tracking_key_format` | QQ | (timestamp, txn_id) |
| `operation_tracker.tracking_value_format` | iQ | (op_type, cache_size) |

## Test Scenarios

### Scenario: Custom operation — live cache resize
- **What it tests:** Alternately reconfigures the WiredTiger connection with `cache_size=1MB` and `cache_size=500MB`. Records each resize event to the custom tracking table (op_type=CUSTOM, cache_size=new_size).
- **Components:** `WT_CONNECTION::reconfigure`, custom operation tracker.
- **Notes:** Cache resize events are themselves tracked but are identified by `tracking_operation::CUSTOM` in the tracking table and skipped during validation of insert transactions. The tracking of the resize itself may fail with `WT_ROLLBACK` under cache pressure; this is logged as a warning but does not fail the test.

### Scenario: Insert operation — large transaction under fluctuating cache
- **What it tests:** Each insert thread continuously attempts 2-operation transactions with 1 MB keys. When the cache is 1 MB, the transaction is expected to be rejected (rolled back). When the cache is 500 MB, it is expected to commit. The current cache size at insert time is stored as the value so it can be correlated in validation.
- **Components:** B-tree insert, cache pressure, MVCC transaction rollback under cache exhaustion.
- **Notes:** `cache_max_wait_ms=1` ensures insert threads fail fast rather than blocking when the cache is too small.

### Scenario: Validate — cache size at commit time
- **What it tests:** Reads the operation tracking table and groups records by transaction ID. For each committed transaction, checks that the recorded cache size at the last operation exceeds `cache_size_500mb` (500,000,000 bytes).
- **Components:** Operation tracking table, custom key/value format parsing.
- **Notes:** The core cache-size assertion (`testutil_assert(cache_size > cache_size_500mb)`) is currently disabled (FIXME-WT-12931) because some transactions are observed committing when the cache is very low. The validation still asserts that the tracking table is non-empty and that all records have a valid op type.

## Key Observations

- The test exercises the interaction between `conn->reconfigure()` (cache resize) and ongoing transactions — a scenario that can expose races in the eviction/transaction accounting code.
- The custom `operation_tracker_cache_resize` subclass overrides `set_tracking_cursor` to record the cache size as part of the transaction's value, enabling post-run correlation.
- A key limitation: the core assertion about cache size at commit time is disabled (WT-12931), so the test currently only validates that transactions complete and that the tracking table is non-empty.
- The `cache_max_wait_ms=1` setting is crucial — without it, insert threads would block for a long time waiting for cache space, making the test extremely slow when the cache is 1 MB.
- No stress configuration exists; the test runs only in the default 15-second configuration.
