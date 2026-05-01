# test_layered88 — Unsupported layered table operations return clear errors

**File:** `test/suite/test_layered88.py`
**Storage mode:** Disagg/Layered
**Components under test:** Read-only connection rejection, custom collator rejection for layered tables

## Test Cases

### `test_layered88.test_readonly`
- **What it tests:** Closes the default connection and attempts to open a new one with `readonly=true` and disagg config. Verifies it raises `WiredTigerError` with message "disaggregated storage is not supported with read-only connections". Reopens the connection to allow teardown to proceed.
- **Components:** `src/conn/conn_open.c`, disagg mode validation
- **Notes:** FIXME-WT-17177: read-only disagg connections are not yet supported. Disagg-only.

### `test_layered88.test_reverse_collator`
- **What it tests:** Creates a layered table with `collator=reverse`. The `create()` call succeeds (metadata is written). Then calls `session.open_cursor()` and verifies it raises `WiredTigerError` with "layered tables do not support custom collators". Drops the table so teardown verify does not fail on the unsupported configuration.
- **Components:** `src/cursor/cur_layered.c`, layered dhandle open collator check
- **Notes:** FIXME-WT-14738: custom collators are not yet supported on layered tables. Loads the `reverse` collator extension via `conn_extensions()`.
