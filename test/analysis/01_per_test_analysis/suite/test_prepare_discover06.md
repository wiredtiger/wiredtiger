# test_prepare_discover06 — Disaggregated storage: prepared_discover cursor for inserts via follower

**File:** `test/suite/test_prepare_discover06.py`
**Storage mode:** Disaggregated/layered only (`disagg_only`)
**Components under test:** prepared transactions, prepared_discover cursor, layered storage, leader/follower, claim_prepared_id

## Test Cases

### `test_prepare_discover06.test_prepare_discover_layered`
- **What it tests:** Tests the `prepared_discover:` cursor in a disaggregated/layered setup: leader prepares insert transactions with `prepared_id` and checkpoints; follower opens the checkpoint, iterates `prepared_discover:` cursor to find the prepared transactions, claims them with `claim_prepared_id`, and either commits or rolls back; step-up follower to leader and verify data reflects the resolution
- **Components:** `txn/txn_prepare.c`, `cursor/cur_prepare_discover.c`, `conn/conn_layered_ingest.c`, `cursor/cur_layered.c`, `checkpoint/checkpoint.c`
- **Notes:** Scenarios: commit/rollback × disagg_storages; uses layered: URI; the follower role simulates the MongoDB secondary recovery path where prepared transactions from a primary checkpoint must be replayed or rolled back; verifies that after step-up, data is correct for both the commit (keys visible) and rollback (keys not visible / original values visible) cases
