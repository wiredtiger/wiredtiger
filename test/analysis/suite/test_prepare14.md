# test_prepare14 — Prepared update+remove on same key evicted and read with ignore_prepare

**File:** `test/suite/test_prepare14.py`
**Storage mode:** General and in-memory
**Components under test:** prepared transactions, eviction, ignore_prepare, tombstones, update chain

## Test Cases

### `test_prepare14.test_prepare14`
- **What it tests:** A single prepared transaction performs both an update and a remove (tombstone) on the same key (so both start and stop timestamps in the time window come from the same prepared transaction); the page is evicted via a debug cursor; a concurrent read with `ignore_prepare=true` verifies the key is not found (tombstone is the visible update at the read timestamp); tests both in-memory and on-disk configurations
- **Components:** `txn/txn_prepare.c`, `evict/evict_page.c`, `btree/bt_delete.c`, `cursor/cur_std.c`
- **Notes:** Scenarios: no_inmem/inmem × column/integer-row; the key feature is that both the update and the tombstone are from the same prepared transaction, meaning the time window cell on disk has both a prepared start_ts and a prepared stop_ts; `ignore_prepare=true` at a read_timestamp between the two should see no value (the tombstone wins)
