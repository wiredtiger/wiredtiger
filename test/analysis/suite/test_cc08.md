# test_cc08 — Checkpoint cleanup of logged tables in aggressive (reclaim_space) mode

**File:** `test/suite/test_cc08.py`
**Storage mode:** General (skips tiered)
**Components under test:** checkpoint cleanup subsystem, WAL-logged tables, statistics

## Test Cases

### `test_cc08.test_cc08`
- **What it tests:** Verifies that CC processes logged (WAL-enabled) tables only when configured with `checkpoint_cleanup=[method=reclaim_space]` (aggressive mode). With `method=none` CC should select zero pages from logged tables.
- **Components:** `src/btree/`, `src/log/`, `src/conn/conn_sweep.c`
- **Notes:** Skip: `@wttest.skip_for_hook("tiered", ...)`. Two scenarios: `cc_method_none` (cc_aggressive=False) and `cc_method_reclaim_space` (cc_aggressive=True). Table created with small page sizes (`allocation_size=512`, `internal_page_max=512`, `leaf_page_max=512`) to increase the number of internal pages targeted by CC. Connection is opened with `log=(enabled=true)`, then reopened with the chosen `checkpoint_cleanup` config. An explicit cursor is opened to ensure the dhandle is open for CC to process. Asserts `checkpoint_cleanup_pages_read_reclaim_space > 0` and `visited > 0` in aggressive mode; `selected_pages == 0` in none mode.
