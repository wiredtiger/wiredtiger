# test_reconfig02 — Logging reconfiguration: allowed and forbidden options, prealloc and remove behavior

**File:** `test/suite/test_reconfig02.py`
**Storage mode:** General (logging enabled)
**Components under test:** connection reconfiguration, write-ahead log, log preallocation, log file removal

## Test Cases

### `test_reconfig02.test_reconfig02_simple`
- **What it tests:** Verifies that `log=(remove=...)`, `log=(prealloc=...)`, and `log=(zero_fill=...)` can be toggled true/false at runtime via `conn.reconfigure()`
- **Components:** `log/log.c`, `conn/conn_log.c`
- **Notes:** `init_config = 'log=(enabled,file_max=100K,prealloc=false,remove=false,zero_fill=false)'`

### `test_reconfig02.test_reconfig02_disable`
- **What it tests:** Verifies that immutable log options (`enabled`, `compressor`, `file_max`, `path`, `recover`) cannot be changed via reconfigure and raise `/unknown configuration key/`
- **Components:** `config/config.c`, `log/log.c`

### `test_reconfig02.test_reconfig02_prealloc`
- **What it tests:** Starts with `prealloc=false`; verifies no pre-allocated log files (`*Prep*`) exist; reconfigures to `prealloc=true`; waits up to 100 seconds for the log worker thread to create pre-allocated files; asserts at least one `*Prep*` file appears in the directory
- **Components:** `log/log_slot.c`, `conn/conn_log.c`
- **Notes:** Uses `time.sleep(1)` in a loop (up to 100 iterations) to wait for the worker thread

### `test_reconfig02.test_reconfig02_remove`
- **What it tests:** Starts with `remove=false`; writes 1,000 entries and closes+reopens the connection (triggering a checkpoint and advancing to the next log file); confirms original log files (`*gerLog*`) are still present; reconfigures to `remove=true`; forces a checkpoint; sleeps 2 seconds for the log removal worker; confirms original log files are gone
- **Components:** `log/log.c`, `log/log_slot.c`, `conn/conn_log.c`
