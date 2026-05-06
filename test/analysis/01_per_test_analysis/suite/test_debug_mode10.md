# test_debug_mode10 — Tests debug_mode realloc_malloc always-malloc reallocation mode

**File:** `test/suite/test_debug_mode10.py`
**Storage mode:** General
**Components under test:** debug mode, memory allocation

## Test Cases

### `test_debug_mode10.test_realloc_exact`
- **What it tests:** Smoke test that `debug_mode=(realloc_malloc=true)` does not break normal insert and checkpoint operations. When enabled, every `__wt_realloc` call performs a fresh `malloc`+`memcpy`+`free` instead of an in-place realloc, maximizing reallocation coverage for memory-error tools.
- **Components:** `src/os_posix/os_alloc.c`, `src/conn/conn_debug.c`
- **Notes:** Test inserts one key-value pair and runs a checkpoint. Method is named `test_realloc_exact` (inherited naming convention from mode07) but tests `realloc_malloc`.

### `test_debug_mode10.test_realloc_exact_off`
- **What it tests:** Verifies that reconfiguring `debug_mode=(realloc_malloc=false)` restores normal realloc behavior without errors.
- **Components:** `src/os_posix/os_alloc.c`, `src/conn/conn_debug.c`
- **Notes:** No behavioral assertions beyond absence of errors.
