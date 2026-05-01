# test_shared_cache01 — Shared cache pool basics across multiple connections

**File:** `test/suite/test_shared_cache01.py`
**Storage mode:** General
**Components under test:** shared cache, multiple connections, eviction, cache rebalancing

## Test Cases

### `test_shared_cache01.test_shared_cache_shared01`
- **What it tests:** Basic shared cache test with 2 connections sharing a pool. Creates tables in each connection, inserts data, and verifies data is accessible. Confirms no errors during normal operation with a shared cache.
- **Components:** `src/conn/conn_cache_pool.c`, `src/evict/`
- **Notes:** Uses `shared_cache=(name=pool,size=200M,chunk=10M,reserve=30M)`.

### `test_shared_cache01.test_shared_cache_shared02`
- **What it tests:** Tests with more connections (3+) sharing the same cache pool. Verifies pool can accommodate multiple connections and data remains consistent.
- **Components:** `src/conn/conn_cache_pool.c`
- **Notes:** Exercises pool with more than 2 participants.

### `test_shared_cache01.test_shared_cache_shared03`
- **What it tests:** Tests full cache allocation across connections. Fills the cache to near its limit and verifies eviction occurs properly across connections sharing the pool.
- **Components:** `src/conn/conn_cache_pool.c`, `src/evict/`
- **Notes:** Cache pressure test.

### `test_shared_cache01.test_shared_cache_shared04`
- **What it tests:** Tests cache rebalancing when one connection's workload changes. Verifies that the pool correctly redistributes cache among connections.
- **Components:** `src/conn/conn_cache_pool.c`
- **Notes:** Dynamic rebalancing test.

### `test_shared_cache01.test_shared_cache_shared05`
- **What it tests:** Tests a connection joining the shared cache pool late (after others are already using it). Verifies the late-joiner gets appropriate cache allocation.
- **Components:** `src/conn/conn_cache_pool.c`
- **Notes:** Late-join scenario.

### `test_shared_cache01.test_shared_cache_shared06`
- **What it tests:** Tests a connection leaving the shared cache pool (closing connection). Verifies other connections can continue using the pool and their allocations are adjusted.
- **Components:** `src/conn/conn_cache_pool.c`
- **Notes:** Connection leave/close scenario.

### `test_shared_cache01.test_shared_cache_shared07`
- **What it tests:** Verifies that configuring absolute values for eviction trigger/target fails in shared cache mode (must use percentages).
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Error validation for absolute eviction config.

### `test_shared_cache01.test_shared_cache_shared08`
- **What it tests:** Tests shared cache with verbose logging enabled. Verifies no errors in verbose output.
- **Components:** `src/conn/conn_cache_pool.c`
- **Notes:** Verbose mode test.

### `test_shared_cache01.test_shared_cache_shared09`
- **What it tests:** Tests a mixed workload across shared cache connections. Some connections read, some write, verifying the pool handles concurrent activity correctly.
- **Components:** `src/conn/conn_cache_pool.c`, `src/evict/`
- **Notes:** Concurrency test.

### `test_shared_cache01.test_shared_cache_shared10`
- **What it tests:** Tests default shared cache configuration values when minimal options are specified. Verifies defaults are applied correctly.
- **Components:** `src/conn/conn_cache_pool.c`
- **Notes:** Default configuration test.

### `test_shared_cache01.test_shared_cache_shared11`
- **What it tests:** Tests that the pool size can be increased/decreased via reconfigure and that connections adapt accordingly.
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Dynamic pool resize test.
