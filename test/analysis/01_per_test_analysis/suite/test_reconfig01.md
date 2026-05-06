# test_reconfig01 — Connection reconfiguration smoke tests

**File:** `test/suite/test_reconfig01.py`
**Storage mode:** General
**Components under test:** connection reconfiguration API, eviction, statistics, I/O capacity, checkpoint, logging, file manager

## Test Cases

### `test_reconfig01.test_reconfig_shared_cache`
- **What it tests:** Verifies that `conn.reconfigure("shared_cache=(name=pool,size=300M)")` succeeds without error
- **Components:** `conn/conn_cache.c`, `conn/conn_shared_cache.c`

### `test_reconfig01.test_reconfig_eviction`
- **What it tests:** Verifies that eviction thread count (threads_max, threads_min) and eviction targets/triggers (absolute values in MB for eviction_target, eviction_trigger, eviction_dirty_target, eviction_dirty_trigger, eviction_checkpoint_target) can all be reconfigured at runtime
- **Components:** `evict/evict_lru.c`, `conn/conn_cache.c`
- **Notes:** Tests increasing and decreasing thread counts, setting min==max, and using absolute size values (e.g., `eviction_target=50M`)

### `test_reconfig01.test_reconfig_statistics`
- **What it tests:** Verifies toggling statistics collection between `all`, `fast`, and `none` at runtime via `conn.reconfigure()`
- **Components:** `conn/conn_stat.c`

### `test_reconfig01.test_reconfig_capacity`
- **What it tests:** Verifies `io_capacity=(total=80M/100M)` can be reconfigured; verifies that a value below minimum raises an error with message `/below minimum/`
- **Components:** `conn/conn_capacity.c`

### `test_reconfig01.test_reconfig_checkpoints`
- **What it tests:** Verifies checkpoint wait interval (`wait=0/5`) and log_size trigger (`log_size=0/1M`) can be reconfigured at runtime
- **Components:** `checkpoint/checkpoint.c`

### `test_reconfig01.test_reconfig_statistics_log_ok`
- **What it tests:** Verifies that all valid statistics_log sub-options (wait, json, on_close, timestamp) can be reconfigured; verifies disabling (wait=0) also works
- **Components:** `conn/conn_stat.c`

### `test_reconfig01.test_reconfig_statistics_log_fail`
- **What it tests:** Verifies that an unknown reconfiguration key (`log=(path=foo)`) raises an error with message `/unknown configuration key/`
- **Components:** `config/config.c`

### `test_reconfig01.test_file_manager`
- **What it tests:** Verifies that `file_manager` sub-options (`close_scan_interval`, `close_idle_time`) can be reconfigured independently and together
- **Components:** `conn/conn_sweep.c`
