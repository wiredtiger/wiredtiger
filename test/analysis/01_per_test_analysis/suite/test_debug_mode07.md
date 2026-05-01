# test_debug_mode07 — Tests debug_mode realloc_exact exact-size reallocation mode

**File:** `test/suite/test_debug_mode07.py`
**Storage mode:** General
**Components under test:** debug mode, memory allocation

## Test Cases

### `test_debug_mode07.test_realloc_exact`
- **What it tests:** Smoke test that `debug_mode=(realloc_exact=true)` does not break normal insert and checkpoint operations. When enabled, `__wt_realloc` allocates exactly the requested size (no slack), increasing the chance of detecting buffer overruns under ASAN/Valgrind.
- **Components:** `src/support/hazard.c`, `src/os_posix/os_alloc.c`, `src/conn/conn_debug.c`
- **Notes:** Test inserts one key-value pair and runs a checkpoint to exercise the many internal realloc calls that occur during reconciliation.

### `test_debug_mode07.test_realloc_exact_off`
- **What it tests:** Verifies that reconfiguring `debug_mode=(realloc_exact=false)` restores normal realloc behavior without errors.
- **Components:** `src/os_posix/os_alloc.c`, `src/conn/conn_debug.c`
- **Notes:** No behavioral assertions beyond absence of errors.
