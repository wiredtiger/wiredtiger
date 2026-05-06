# test_tiered21 — Incompatible tiered configuration options rejected at open and reconfigure

**File:** `test/suite/test_tiered21.py`
**Storage mode:** Tiered
**Components under test:** connection open configuration validation, `conn.reconfigure` validation, incompatibility between tiered storage and in-memory mode

## Test Cases

### `test_tiered21.test_options`
- **What it tests:** Verifies that opening a WiredTiger connection with both tiered storage and `in_memory=true` raises an error containing "not compatible with". Closes the test's default connection, then attempts to open a new connection with the standard tiered config plus `in_memory=true`. The `assertRaisesHavingMessage` check confirms the rejection.
- **Components:** `src/conn/conn_open.c` (configuration compatibility checks), tiered + in-memory mutual exclusion
- **Notes:** Parametrized across all tiered storage backends (tiered_only=True, no non_tiered scenario).

### `test_tiered21.test_reconfigure`
- **What it tests:** Verifies that calling `conn.reconfigure('in_memory=true')` on a live tiered connection raises an error containing "unknown configuration key". The `in_memory` option is not a valid reconfigure key (it can only be set at open time), so the error message confirms the option is rejected before any compatibility check is even reached.
- **Components:** `src/conn/conn_api.c` (`WT_CONNECTION::reconfigure` validation)
- **Notes:** Same parametrization as `test_options`. Uses the already-open tiered connection from test setup.
