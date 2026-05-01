# test_prepare_hs01 — Prepared updates in multiple sessions with history store eviction

**File:** `test/suite/test_prepare_hs01.py`
**Storage mode:** General
**Components under test:** prepared transactions, history store, eviction, multi-session, timestamps

## Test Cases

### `test_prepare_hs01.test_prepare_hs`
- **What it tests:** Loads 10,000 rows to prime the cache, then opens 3 sessions each preparing updates on ~4,000 keys (12,000 total prepared updates); the heavy load triggers eviction and pushes prior values into the history store; verifies that the committed value at ts=2 is readable from the history store while the prepared sessions are still open (not committed); closes prepared sessions (implicitly rolling back); verifies the original value is still visible
- **Components:** `txn/txn_prepare.c`, `history/hs_cursor.c`, `evict/evict_lru.c`, `evict/evict_page.c`
- **Notes:** Scenarios: column/integer-row; `conn_config = 'cache_size=50MB,eviction_updates_trigger=95,eviction_updates_target=80'`; value_format=u (binary); uses multiple sessions to avoid cache stall from a single large uncommitted prepare; the key check is that HS-evicted prior values remain readable during an open prepared transaction
