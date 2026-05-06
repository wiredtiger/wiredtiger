# test_reconfig03 — Connection reconfiguration patterns mirroring MongoDB's reconfigwt.js

**File:** `test/suite/test_reconfig03.py`
**Storage mode:** General (logging enabled, auto-checkpoint every 1 second)
**Components under test:** connection reconfiguration, eviction, cache, shared cache, checkpoint log-size trigger

## Test Cases

### `test_reconfig03.test_reconfig03_mdb`
- **What it tests:** Simulates the reconfiguration sequence used in MongoDB's reconfigwt.js test: populates increasingly large datasets, sleeps 1 second between each to allow the auto-checkpoint to run, and reconfigures `eviction_target`, `cache_size`, `eviction_dirty_target`, and `shared_cache` at each step; verifies no errors occur during the mixed workload+reconfigure sequence
- **Components:** `conn/conn_cache.c`, `evict/evict_lru.c`, `conn/conn_shared_cache.c`
- **Notes:** `conn_config = 'log=(enabled,file_max=100K,prealloc=false,remove=false,zero_fill=false),checkpoint=(wait=1),cache_size=1G'`; grows dataset from 10,000 to 40,000 entries; the shared_cache reconfigure uses chunk, name, reserve, and size sub-options; designed to reproduce real-world MongoDB reconfiguration patterns

### `test_reconfig03.test_reconfig03_log_size`
- **What it tests:** Verifies that the checkpoint log-size trigger can be reconfigured to small values (20 bytes), large values (1 MB), and disabled (0) at runtime
- **Components:** `checkpoint/checkpoint.c`, `log/log.c`
