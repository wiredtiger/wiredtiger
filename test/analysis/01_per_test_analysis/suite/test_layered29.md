# test_layered29 — Create a large number of layered tables (scale/stress test)

**File:** `test/suite/test_layered29.py`
**Storage mode:** Disagg/Layered (disagg_only)
**Components under test:** layered table creation at scale, schema layer, conn_layered.c, page log (palite)

## Test Cases

### `test_layered29.test_create_tables`
- **What it tests:** Creates 10,000 layered tables (`layered:test_table0` through `layered:test_table9999`) in a loop. Asserts that each `session.create()` call returns 0 (success).
- **Components:** layered table schema handler (bulk table creation path), metadata store scalability, page log registration for each new table, conn_layered.c
- **Notes:** Marked with `@wttest.longtest('lots of tables')` — runs only in long-test mode. Parametrized by disagg_storage scenario. Tests that the system can handle a large number of simultaneously existing layered tables without running out of resources, hitting metadata limits, or corrupting any internal structures. Would break if: table ID allocation overflows, metadata table becomes excessively slow, or the page log extension fails to handle 10,000 table registrations.
