# test_prepare_discover07 — Disaggregated storage: prepared_discover cursor for tombstones via follower

**File:** `test/suite/test_prepare_discover07.py`
**Storage mode:** Disaggregated/layered only (`disagg_only`)
**Components under test:** prepared transactions, prepared_discover cursor, tombstones, layered storage, leader/follower, claim_prepared_id

## Test Cases

### `test_prepare_discover07.test_prepare_discover_layered`
- **What it tests:** Same three-phase pattern as test_prepare_discover06 but the leader prepares delete (tombstone) operations (removes keys 4–6) instead of inserts; follower discovers and claims the prepared tombstones, commits or rolls back; after step-up, verifies that committed deletes result in WT_NOTFOUND for the removed keys, and rolled-back deletes restore the original values
- **Components:** `txn/txn_prepare.c`, `btree/bt_delete.c`, `cursor/cur_prepare_discover.c`, `conn/conn_layered_ingest.c`, `cursor/cur_layered.c`, `checkpoint/checkpoint.c`
- **Notes:** Scenarios: commit/rollback × disagg_storages; uses layered: URI; companion to test_prepare_discover06 (which tests inserts); together they cover the full set of DML operations (insert, update, delete) through the disaggregated prepared_discover workflow
