# test_hs25 — History store: correct update structure when evicting page with prepared updates

**File:** `test/suite/test_hs25.py`
**Storage mode:** General
**Components under test:** history store, prepared transactions, eviction

## Test Cases

### `test_hs25.test_insert_updates_hs`
- **What it tests:** Updates key 1 (ts=2, valuea). Updates key 2 (ts=2, valuea then ts=3, valueb). Prepares a transaction that updates key 1 twice within the same transaction (valueb then valuec) at prepare_ts=4. Runs an eviction cursor with `ignore_prepare=true`. Asserts `evict_cursor[1] == valuea` and `evict_cursor[2] == valueb` (the most recent committed values). Rolls back the prepared transaction. Verifies that the update structure (update chain ordering) is handled correctly when a page is evicted that has in-memory prepared updates above committed versions.
- **Components:** `src/history/`, `src/txn/`, `src/evict/`
- **Notes:** Scenarios: key_format ∈ {`r`, `i`}; cache_size=50MB. The test exercises the specific HS path where a prepared update is present in the update list when the eviction cursor processes the page, ensuring the committed values below the prepared update are correctly seen.
