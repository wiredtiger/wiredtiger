# test_shared_cache02 — Shared cache reconfigure API tests

**File:** `test/suite/test_shared_cache02.py`
**Storage mode:** General
**Components under test:** shared cache, reconfigure API, quota enforcement, eviction configuration

## Test Cases

### `test_shared_cache02.test_shared_cache_reconfig01`
- **What it tests:** Verifies that `connection.reconfigure()` can successfully change the shared cache pool size (grows from 200M to 300M). Verifies no error is returned and the pool continues to function.
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Two connections sharing a 200M pool; grows to 300M via reconfigure on one connection. Basic success case.

### `test_shared_cache02.test_shared_cache_reconfig02`
- **What it tests:** Verifies that reconfiguring the reserve size to over-subscribe the pool fails with `/Shared cache unable to accommodate this configuration/`. Two connections each have a 20M reserve; attempting to increase one to 40M (which would total 60M, exceeding 50M pool) should fail.
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Quota enforcement test. Verifies the reserve size is not updated on failure.

### `test_shared_cache02.test_shared_cache_reconfig03`
- **What it tests:** Verifies that when the previous reserve is correctly accounted for, a reconfigure that would appear to over-subscribe (if old reserve wasn't subtracted) succeeds. Grows reserve on one connection from 20M to 30M in a 50M pool (total would be 50M = valid).
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Tests that old reserve is correctly released when computing new reservation.

### `test_shared_cache02.test_shared_cache_reconfig04`
- **What it tests:** Verifies reconfiguring a connection to switch from no shared cache to using a shared cache pool. Both connections initially have no shared cache, then each is reconfigured to join the pool with a 20M reserve.
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Late-join via reconfigure scenario.

### `test_shared_cache02.test_shared_cache_reconfig05`
- **What it tests:** Verifies that configuring absolute values for `eviction_trigger` and `eviction_target` fails with appropriate error messages (`/Shared cache configuration requires a percentage value for eviction trigger/` and `/...eviction target/`), while using percentage values (integers without units) succeeds.
- **Components:** `src/conn/conn_cache_pool.c`, `src/conn/conn_api.c`
- **Notes:** Error validation for eviction config in shared cache mode. Tests both trigger and target failure paths, plus the success path with percentage value.
