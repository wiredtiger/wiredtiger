# test_layered51 — Logging is rejected for layered tables

**File:** `test/suite/test_layered51.py`
**Storage mode:** Disagg/Layered
**Components under test:** conn_layered_ingest.c (schema validation), WT logging

## Test Cases

### `test_layered51.test_create_logged`
- **What it tests:** Verifies that attempting to create a layered table with `log=(enabled=true)` raises a `WiredTigerError` with the message "Logging is not supported for layered". This is a schema validation guard ensuring that WAL logging cannot be accidentally enabled for layered tables, which is incompatible with the disaggregated storage model.
- **Components:** conn_layered_ingest.c or layered table schema validation, WT logging enforcement
- **Notes:** Single test method with no data operations. Uses `assertRaisesWithMessage`. Note: the class is named `test_layered52` in the source file (copy-paste error in the file's class name) but the file is `test_layered51.py`. Also uses the scenario key `test_layered50` (another copy-paste artefact). Disagg-only.
