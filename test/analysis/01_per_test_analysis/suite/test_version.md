# test_version — wiredtiger_version() API smoke test

**File:** `test/suite/test_version.py`
**Storage mode:** General
**Components under test:** `wiredtiger.wiredtiger_version()`, connection API version reporting

## Test Cases

### `test_version.test_version`
- **What it tests:** Calls `wiredtiger.wiredtiger_version()` and implicitly verifies it returns without error (no assertion or value check is performed beyond the call completing successfully).
- **Components:** `conn.c` (wiredtiger_version export)
- **Notes:** No parameterization. Minimal smoke test confirming the `wiredtiger_version()` Python binding is callable. Tagged `[connection_api]`.
