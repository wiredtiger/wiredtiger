# test_layered40 — Layered table metadata has logging disabled

**File:** `test/suite/test_layered40.py`
**Storage mode:** Disagg/Layered
**Components under test:** conn_layered_ingest.c (schema/metadata), WT logging, metadata cursor

## Test Cases

### `test_layered40.test_layered40`
- **What it tests:** Verifies that layered tables are created with `log=(enabled=false)` in their metadata, even when the connection has WAL logging globally enabled (`log=(enabled=true)`). Creates two tables (one via `table:` URI with `block_manager=disagg,type=layered`, one via `layered:` URI), then opens a `metadata:create` cursor and checks that both table entries contain the string `log=(enabled=false)`.
- **Components:** conn_layered_ingest.c or schema creation layer (ensures log=false for layered tables), WT logging, metadata cursor
- **Notes:** Single test method, no further parametrization beyond the disagg storage backend. Disagg-only. This guards against inadvertent logging being enabled for layered tables, which would break the disaggregated model.
