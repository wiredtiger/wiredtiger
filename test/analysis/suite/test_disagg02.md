# test_disagg02 — Unsupported operation rejection: compaction in disaggregated storage mode

**File:** `test/suite/test_disagg02.py`
**Storage mode:** Disagg (disagg_only — non-disagg scenario excluded)
**Components under test:** src/conn/conn_compact.c (or equivalent compaction dispatch), src/block_disagg

## Infrastructure notes

`test_disagg02` inherits from both `wttest.WiredTigerTestCase` and `DisaggConfigMixin`.
`DisaggConfigMixin` supplies `conn_extensions` (page log extension loading) and
`conn_config`/`disagg_conn_config` (connection string generation). Scenarios are produced
by `gen_disagg_storages('test_disagg02', disagg_only=True)` — one per configured page log
backend (e.g. `palite`); the non-disagg variant is excluded.

## Test Cases

### `test_disagg02.test_disagg_compact`
- **What it tests:** Confirms that calling `session.compact()` — in both its standard
  (table URI) and background (`background=true`) forms — raises `WiredTigerError` with the
  message `"Operation not supported"` when the connection is running in disaggregated storage
  mode. This verifies that the compaction code path actively rejects the request rather than
  silently doing nothing or corrupting state.
- **Components:** `src/session` (session compact dispatch), `src/block_disagg` or the
  disaggregated connection layer that intercepts unsupported operations, `src/conn`
  (disaggregated mode flag checked at operation entry)
- **Notes:**
  - Decorated with `@wttest.skip_for_hook("tiered", …)` — the test is skipped when the
    tiered hook is active because tiered tables already disable compaction for a different
    reason.
  - Two distinct call signatures are tested: `compact('table:test_disagg02')` (object-level)
    and `compact(None, 'background=true')` (background/global compaction).
  - Parametrized by page log backend via `gen_disagg_storages`.
  - Failure here means compaction is either silently accepted (risk of data corruption or
    undefined behaviour on disaggregated storage) or raises the wrong error type/message.
