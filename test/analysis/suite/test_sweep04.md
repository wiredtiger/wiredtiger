# test_sweep04 — Sweep keeps up with high table churn (long test)

**File:** `test/suite/test_sweep04.py`
**Storage mode:** General
**Components under test:** file manager sweep, dhandle lifecycle, linear regression analysis

## Test Cases

### `test_sweep04.test_big_run`
- **What it tests:** (Currently skipped via FIXME-WT-13706.) Creates 10 core tables and 100 transient tables; over 20,000 loop iterations, drops and recreates transient tables while accessing core tables; collects dhandle counts every 100 iterations; in the second half of the run stops accessing transient tables and monitors decay. At the end, uses least-squares regression to assert that the slope in Q2 (second quarter) is less than Q1 slope and less than 20.0 dhandles/100 iters, and the final slope is < 5.0 with an average < core + transient + 20.
- **Components:** `file_manager.c`, `dhandle.c`
- **Notes:** Marked `@longtest`. Uses a pool of 100 sessions. Verifies the sweep server can maintain bounded dhandle growth under steady-state churn.
