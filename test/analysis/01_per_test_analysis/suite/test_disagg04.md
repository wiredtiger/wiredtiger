# test_disagg04 — Layered table `storage_tier` config validation and cold-tier I/O stat tracking

**File:** `test/suite/test_disagg04.py`
**Storage mode:** Disagg (disagg_only — non-disagg scenario excluded)
**Components under test:** src/block_disagg, src/conn/conn_layered*.c, src/cursor/cur_layered.c, src/schema (table config parsing), src/stat

## Infrastructure notes

`test_disagg04` inherits from both `wttest.WiredTigerTestCase` and `DisaggConfigMixin`.
`DisaggConfigMixin` supplies `conn_extensions` and `conn_config`/`disagg_conn_config`.
Scenarios come from `gen_disagg_storages('test_disagg04', disagg_only=True)` — one per
configured page log backend; the non-disagg variant is excluded.

The class uses `layered:` URIs exclusively. Before each test that writes data, the
connection is reconfigured to `role=leader` so checkpoint and I/O are permitted.

Helper methods defined in the class:
- `validate_config(uri, config_str, check_func)` — creates a table, reopens the connection,
  reads the metadata cursor for the URI, and optionally runs `check_func` against the raw
  metadata string.
- `get_stat(stat)` — opens the connection-level statistics cursor and returns the value for
  the given stat key.
- `add_data(uri, nitems)` — inserts `nitems` key/value pairs and calls `session.checkpoint()`.

## Test Cases

### `test_disagg04.test_disagg_storage_tier`
- **What it tests:** Validates the full set of valid and invalid values for the
  `disaggregated=(storage_tier=…)` table configuration option on layered tables:
  1. Empty value (`storage_tier=`) — expects `WiredTigerError` + stderr `"Invalid argument"`.
  2. No `storage_tier` specified — expects the option to be absent from persisted metadata
     (backward-compatibility requirement: existing databases must not be affected).
  3. Valid value `cold` — expects `storage_tier=cold` to appear verbatim in metadata after
     a connection reopen (persistence check).
  4. Typo `coldd` — expects `WiredTigerError` + stderr `"Invalid argument"`.
- **Components:** `src/schema` (config parsing and validation for `disaggregated` sub-config),
  `src/conn/conn_layered*.c` (metadata persistence of tier config), `src/block_disagg`
  (storage tier routing)
- **Notes:**
  - All four sub-cases share a single test method but use distinct URI suffixes (`%02d`
    formatted) to avoid collisions.
  - The metadata check after reopen confirms the config is durably stored, not just accepted
    transiently.
  - Failure in the "no storage_tier" case would break backward compatibility with databases
    that pre-date this config option.

### `test_disagg04.test_cold_write`
- **What it tests:** Verifies that writing data to a `storage_tier=cold` layered table
  causes the `disagg_block_put_cold` connection-level statistic to increase from zero.
  Inserts 1000 key/value pairs and checkpoints before reading the stat.
- **Components:** `src/block_disagg` (`disagg_block_put_cold` stat increment path),
  `src/stat`, `src/conn/conn_layered*.c` (cold-tier routing during reconciliation),
  `src/checkpoint`
- **Notes:**
  - Stat is checked before and after `add_data` to confirm it was exactly zero before
    any writes and strictly positive after.
  - Failure means cold-tier blocks are not being sent through the cold write path, or
    the stat is not being incremented, so cold-tier isolation cannot be relied upon.

### `test_disagg04.test_cold_read`
- **What it tests:** Verifies that reading pages from a `storage_tier=cold` layered table
  causes the `disagg_block_get_cold` connection-level statistic to increase. After writing
  and checkpointing 1000 rows, confirms the stat is still zero (pages resident in cache),
  then calls `verifyUntilSuccess` on the URI to force all pages to be read from the page
  log, and asserts the stat has become positive.
- **Components:** `src/block_disagg` (`disagg_block_get_cold` stat increment path),
  `src/stat`, `src/verify` (drives full-table page reads), `src/conn/conn_layered*.c`
  (cold-tier routing during page fetch)
- **Notes:**
  - Uses `verifyUntilSuccess` rather than a cursor scan because verify reads every page
    unconditionally, including those that might still be in cache after the checkpoint.
  - The stat being zero immediately after write+checkpoint (before verify) demonstrates
    that writes do not accidentally trigger cold reads.
  - Failure means the cold read path is not being taken (e.g. pages fall back to the
    normal hot path), breaking accounting and potentially cold-tier storage semantics.
