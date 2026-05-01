# test_disagg03 — Tiered storage disabled and tiered URI creation rejected in disaggregated mode

**File:** `test/suite/test_disagg03.py`
**Storage mode:** Disagg (disagg_only — non-disagg scenario excluded)
**Components under test:** src/tiered (tiered storage worker thread startup), src/conn/conn_tiered.c, src/schema (table creation dispatch)

## Infrastructure notes

`test_disagg03` is decorated with `@disagg_test_class`, which wraps the class with
`DisaggConfigMixin` behaviour and additionally:
- Creates `follower/` and `kv_home/` directories in `early_setup` (symlinks follower's
  `kv_home` to the leader's).
- Loads the page log extension via `conn_extensions` (calling `add_scenario_config` then
  `disagg_conn_extensions`).
- Appends `disaggregated=(page_log=<backend>)` to `conn_config` unless already present.
- Suppresses expected `WT_VERB_RTS` verbose output at shutdown.

`conn_config` is overridden in the class body to combine `conn_base_config` (verbose tiered
logging enabled) with the role and `lose_all_my_data=true` disagg flags.

Scenarios are the Cartesian product of three dimensions:
- **disagg_storages** — one per configured page log backend (e.g. `palite`).
- **role_scenarios** — `leader` or `follower`.
- **prefix_scenarios** — `tiered:`, `tier:`, or `object:` URI prefix.

This produces 6 scenarios per backend (2 roles × 3 prefixes).

## Test Cases

### `test_disagg03.test_disagg_tiered_disabled`
- **What it tests:** Verifies that the tiered storage worker thread is NOT started when the
  database runs in disaggregated storage mode. Checks that the WiredTiger stdout log contains
  the message `"Tiered storage not started: disaggregated storage."` Exercises both leader
  and follower roles.
- **Components:** `src/tiered` (tiered worker thread startup logic), `src/conn/conn_tiered.c`
  (startup guard that detects disagg mode), `src/conn` (role configuration)
- **Notes:**
  - Parametrized across role (leader/follower) and URI prefix (tiered/tier/object) — the
    prefix parameter is irrelevant to this test's assertion but is inherited from the shared
    scenario matrix.
  - Failure means the tiered worker thread started alongside the disagg page log, which
    could cause conflicting I/O paths or double-handling of pages.

### `test_disagg03.test_disagg_tiered_create_disabled`
- **What it tests:** Verifies that attempting to `session.create()` a table using any of the
  three tiered URI prefixes (`tiered:`, `tier:`, `object:`) raises `WiredTigerError` with
  `"Operation not supported"` when in disaggregated storage mode. Also asserts (before the
  create attempt) that the tiered worker is still absent via the same log message check.
- **Components:** `src/schema` (table creation dispatch, URI prefix routing), `src/tiered`
  (gating logic), `src/conn/conn_tiered.c`
- **Notes:**
  - Parametrized across role (leader/follower) and all three tiered URI prefixes
    (`tiered:`, `tier:`, `object:`), ensuring the rejection applies to every variant of
    tiered addressing, not just the canonical `tiered:` prefix.
  - Failure means a tiered table can be created inside a disaggregated database, which
    would expose undefined interactions between the two storage back-ends.
