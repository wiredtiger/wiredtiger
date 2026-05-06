# test_config11 — debug=(release_evict_page) session reconfigure causing eviction on page release

**File:** `test/suite/test_config11.py`
**Storage mode:** General
**Components under test:** session reconfigure, eviction, debug mode

## Test Cases

### `test_config11.test_config11`
- **What it tests:** `debug=(release_evict_page=true)` session reconfigure causes pages to be evicted when the cursor releases them. Verifies cache usage decreases after reads with this mode enabled.
- **Components:** `src/session/session_api.c`, `src/evict/`, `src/cursor/`
- **Notes:** Scenarios: column-store (`key_format=r`) and integer row-store (`key_format=i`). Measures cache bytes in use before and after reads with the debug option to confirm eviction happened.
