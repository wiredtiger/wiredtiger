# test_config03 — Probabilistic connection config combinations (eviction target/trigger, cache, sessions)

**File:** `test/suite/test_config03.py`
**Storage mode:** General
**Components under test:** connection API, eviction config, config parsing

## Test Cases

### `test_config03` (inherits from `test_base03.test_base03`)
- **What it tests:** Probabilistic scenario matrix combining: `cache_size`, `create`, `error_prefix`, `eviction_target`, `eviction_trigger`, `multiprocess`, `session_max`, `transactional`, `verbose`. Prunes to 100–1000 scenarios. Verifies invalid combos (target >= trigger, create=false) produce errors.
- **Components:** `src/conn/conn_open.c`, `src/evict/`, `src/config/`
- **Notes:** Uses `wtscenario` pruning. Expects `WT_ERROR` when eviction target >= trigger. Exercises all combinations systematically.
