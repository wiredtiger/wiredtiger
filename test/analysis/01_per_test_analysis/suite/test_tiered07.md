# test_tiered07 — Schema operations (drop, create, rename) on tiered tables

**File:** `test/suite/test_tiered07.py`
**Storage mode:** Tiered
**Components under test:** `session.drop`, `session.create`, tiered object file lifecycle on drop, `remove_files` / `force` drop options, object-name collision detection after drop

## Test Cases

### `test_tiered07.test_tiered`
- **What it tests:** Schema operations against tiered tables: (1) creates four tiered tables (`abc`, `ab`, `abcd`) and one non-tiered table (`local`); (2) inserts data and calls `flush_tier` (parametrized whether a prior checkpoint was done); (3) drops the local and `abc` tables, verifying that `remove_files=true` (the default) removes local object files `abc-0000000001.wtobj` and `abc-0000000002.wtobj`; (4) `drop(..., force=true)` on an already-dropped table succeeds silently; (5) `drop(...)` on a non-existent table raises `WiredTigerError`; (6) if a checkpoint was done before flush_tier (so bucket objects exist), re-creating a table with the same name raises an error; if no prior checkpoint was done (no bucket objects), re-creation succeeds; (7) verifies similarly-named tables (`ab`, `abcd`) are unaffected; (8) tests `remove_files=false` — drop removes metadata but leaves the local `.wtobj` file in place.
- **Components:** `src/tiered/tiered_handle.c`, `src/schema/schema_drop.c`, `src/tiered/conn_tiered.c` (object removal work units), storage_source (dir_store only for file-system checks)
- **Notes:**
  - Restricted to dir_store only (tiered_storage_dirstore_source = storage_sources[:1]) because S3 directory listing appends a trailing `/` which the tiered code does not expect.
  - Parametrized on `first_ckpt`: `True` (checkpoint before flush_tier) vs `False` (flush_tier before first checkpoint). This controls whether bucket objects are actually created and therefore whether same-name re-creation is blocked.
  - Key format is `key_format=S,value_format=S`.
