# test_disagg01 — Low-level page log API smoke test (put, get, discard, checkpoint lifecycle)

**File:** `test/suite/test_disagg01.py`
**Storage mode:** Disagg (disagg_only — non-disagg scenario excluded)
**Components under test:** src/block_disagg, ext/page_log/palite, src/conn (page log handle management)

## Infrastructure notes

`test_disagg01` inherits from both `wttest.WiredTigerTestCase` and `DisaggConfigMixin`.
`DisaggConfigMixin` (defined in `helper_disagg.py`) provides:
- `conn_extensions` — loads the configured page log shared library (e.g. `palite`) and injects
  its extension config string (verbosity level, optional delay settings).
- `conn_config` / `disagg_conn_config` — builds the `disaggregated=(…)` connection string.
- `add_scenario_config` — populates `is_disagg`, `is_local_storage`, and `ds_name` from the
  active scenario when they are not already set.
- Various checkpoint helpers (`disagg_get_complete_checkpoint_ext`,
  `disagg_advance_checkpoint`, `restart_without_local_files`, etc.) used by other test files.

The class uses `gen_disagg_storages('test_disagg01', disagg_only=True)` to build scenarios,
which produces one scenario per configured page log backend (typically `palite`) and excludes
the `non_disagg` variant. Scenarios are expanded via `make_scenarios`.

## Test Cases

### `test_disagg01.test_disagg_basic`
- **What it tests:** Exercises every method in the public page log Python API in a single
  sequential flow: `pl_begin_checkpoint`, `pl_complete_checkpoint`, `pl_open_handle`,
  `plh_put` (both full-page and delta variants), `plh_get`, `plh_discard`, and
  `pl_terminate`. Verifies that `plh_get` returns the exact sequence of page images
  (full + deltas) for two different page numbers (20 and 21), and that `plh_discard`
  returns an LSN strictly greater than the discarded range.
- **Components:** `ext/page_log/palite` (palite storage engine), `src/block_disagg`
  (block layer that drives put/get/discard), `src/conn` (page log handle registration
  via `conn.get_page_log`)
- **Notes:**
  - Parametrized by `gen_disagg_storages` — one scenario per configured page log
    implementation. Currently only `palite` is exercised.
  - Covers the checkpoint lifecycle: an empty checkpoint (ID 1) is completed before
    data writes begin so that checkpoint 2 has a valid baseline LSN to reference.
  - Both `WT_PAGE_LOG_DELTA` and non-delta (full image) put flags are exercised.
  - The delta chain for page 20 is: full → delta1 → delta2; for page 21: full → delta1.
    `plh_get` is called at the tip LSN of each chain and the result list is compared
    element-by-element.
  - Failure here indicates a regression in the most fundamental page log contract:
    if put/get/discard are broken, all higher-level disagg tests will also fail.
