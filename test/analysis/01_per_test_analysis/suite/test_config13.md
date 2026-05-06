# test_config13 — FLCS table creation rejection (no longer supported)

**File:** `test/suite/test_config13.py`
**Storage mode:** General
**Components under test:** session create API, FLCS (fixed-length column-store)

## Test Cases

### `test_config13.test_create_flcs`
- **What it tests:** Attempting to create a fixed-length column-store (FLCS) table raises an error, verifying that FLCS is no longer a supported table type.
- **Components:** `src/schema/schema_create.c`
- **Notes:** FLCS uses `key_format=r` with a fixed-length `value_format` like `8t`. The test verifies `WT_ERROR` or `EINVAL` is returned.
