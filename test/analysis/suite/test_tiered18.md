# test_tiered18 — Tiered shared storage: table creation, colgroup metadata, and log configuration

**File:** `test/suite/test_tiered18.py`
**Storage mode:** Tiered with `shared=true` (tiered_shared connection config)
**Components under test:** shared tiered table creation (`tiered_storage=(shared=true)`), colgroup metadata for active and shared colgroups, log configuration propagation to file/tiered URIs, `conn_config` with `shared=true`

## Test Cases

### `test_tiered18.test_tiered_shared`
- **What it tests:** Partially active test (most of the body is commented out with FIXME-WT-14939). The active portion creates one shared tiered table (`uri_shared` with `tiered_storage=(shared=true)`) and verifies via the metadata cursor that:
  - The table metadata has `key_format=S`.
  - The active colgroup (`colgroup:test_tiered18_shared.active`) references the correct `file:` URI.
  - The shared colgroup (`colgroup:test_tiered18_shared.shared`) references the correct `tiered:` URI.
  - Both the `file:` and `tiered:` metadata contain `log=(enabled=true)`.
  The commented-out portions would have tested: shared-default table creation, non-tiered table in a shared connection, alter of log config on shared and non-tiered tables, drop, and reopen with `shared=false` to verify that creating a shared table then raises "Invalid argument".
- **Components:** `src/tiered/tiered_handle.c` (shared tiered table creation), `src/schema/schema_create.c`, colgroup metadata, connection shared mode config
- **Notes:**
  - Parametrized across all tiered storage backends (dir_store, s3_store, gcp_store, azure_store) with `tiered_shared=True` passed to `gen_tiered_storage_sources`.
  - Connection config uses `get_shared_conn_config` which appends `shared=true` to the `tiered_storage` section.
  - `local_retention=3`.
  - Two buckets are created (`bucket` and `bucket1`) for dir_store.
  - The `check_metadata` helper checks both exact string and "trailing comma" variants to handle metadata with additional fields.
