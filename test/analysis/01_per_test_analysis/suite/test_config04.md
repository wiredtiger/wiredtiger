# test_config04 — Individual connection config option validation (cache, eviction, log, session)

**File:** `test/suite/test_config04.py`
**Storage mode:** General
**Components under test:** connection API, cache config, eviction config, log config, config parsing

## Test Cases

### `test_config04.test_bad_config`
- **What it tests:** Completely invalid config string; expects error.
- **Components:** `src/config/`

### `test_config04.test_cache_size_number`
- **What it tests:** Cache size specified as a plain number (bytes).
- **Components:** `src/conn/conn_open.c`, `src/cache/`

### `test_config04.test_cache_size_K` / `test_cache_size_M` / `test_cache_size_G` / `test_cache_size_T`
- **What it tests:** Cache size with K/M/G/T suffixes; verifies suffix parsing.
- **Components:** `src/config/`, `src/cache/`
- **Notes:** Covers all magnitude suffixes.

### `test_config04.test_cache_too_small`
- **What it tests:** Cache size below minimum threshold; expects error.
- **Components:** `src/cache/`, `src/config/`

### `test_config04.test_cache_too_large`
- **What it tests:** Cache size above system memory or maximum; expects error.
- **Components:** `src/cache/`, `src/config/`

### `test_config04.test_eviction`
- **What it tests:** Valid eviction_target and eviction_trigger values.
- **Components:** `src/evict/`

### `test_config04.test_eviction_bad` / `test_eviction_bad2`
- **What it tests:** eviction_target >= eviction_trigger; expects error.
- **Components:** `src/evict/`, `src/config/`

### `test_config04.test_eviction_absolute`
- **What it tests:** Eviction with absolute byte thresholds.
- **Components:** `src/evict/`

### `test_config04.test_eviction_abs_and_pct`
- **What it tests:** Eviction with both absolute and percentage thresholds combined.
- **Components:** `src/evict/`

### `test_config04.test_eviction_abs_less_than_one_pct`
- **What it tests:** Absolute eviction threshold less than 1% of cache.
- **Components:** `src/evict/`

### `test_config04.test_eviction_absolute_bad` / `test_eviction_abs_and_pct_bad` / `test_eviction_abs_and_pct_bad2`
- **What it tests:** Invalid absolute eviction configs; expects error.
- **Components:** `src/evict/`, `src/config/`

### `test_config04.test_eviction_tgt_abs_too_large` / `test_eviction_trigger_abs_too_large` / `test_eviction_dirty_tgt_abs_too_large` / `test_eviction_dirty_trigger_abs_too_large`
- **What it tests:** Absolute eviction thresholds exceeding cache size; expects error.
- **Components:** `src/evict/`, `src/config/`

### `test_config04.test_eviction_dirty_trigger_abs_equal_to_dirty_target` / `test_eviction_dirty_trigger_abs_too_low`
- **What it tests:** Dirty eviction trigger <= dirty target; expects error.
- **Components:** `src/evict/`, `src/config/`

### `test_config04.test_eviction_checkpoint_tgt_abs_too_large` / `test_eviction_updates_tgt_abs_too_large` / `test_eviction_updates_trigger_abs_equal_to_updates_target` / `test_eviction_updates_trigger_abs_too_low`
- **What it tests:** Checkpoint and updates eviction absolute threshold validation.
- **Components:** `src/evict/`, `src/config/`

### `test_config04.test_invalid_config`
- **What it tests:** Unknown config key; expects error.
- **Components:** `src/config/`

### `test_config04.test_valid_config_with_quotes`
- **What it tests:** Config string with quoted values; verifies correct parsing.
- **Components:** `src/config/`

### `test_config04.test_error_prefix`
- **What it tests:** `error_prefix` config option sets prefix on error messages.
- **Components:** `src/conn/conn_open.c`

### `test_config04.test_logging`
- **What it tests:** `log=(path=...)` config; verifies custom log directory.
- **Components:** `src/log/`

### `test_config04.test_multiprocess`
- **What it tests:** `multiprocess` config option.
- **Components:** `src/conn/conn_open.c`

### `test_config04.test_session_max`
- **What it tests:** `session_max` config limit.
- **Components:** `src/session/`

### `test_config04.test_transactional`
- **What it tests:** Transactional config option.
- **Components:** `src/txn/`

### `test_config04.test_removed_metadata_config`
- **What it tests:** LSM removed metadata config options raise error (no longer supported).
- **Components:** `src/config/`, `src/lsm/`
